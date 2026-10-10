package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real-adapter diagnostic with synthetic protobuf payloads; not a production latency gate. */
@Testcontainers
class DocumentPartialBenchmarkIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final RustFsBenchmarkStore S3 = new RustFsBenchmarkStore();

    @Test void measureSingleChunkUpdate() throws Exception {
        try (var database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
                var opened = BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", S3.getEndpoint().toString()),
                Map.entry("region", S3.getRegion()),
                Map.entry("access-key", S3.getAccessKey()),
                Map.entry("secret-key", S3.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")))) {
            opened.ensureNamespace("partial-benchmark");
            var tx = new Tx(database.entityManagerFactory());
            var documents = new DocumentLedger(tx);
            var drives = new DriveLedger(tx);
            var drive = new DriveRecord();
            drive.driveId = UUID.randomUUID(); drive.accountId = "benchmark"; drive.name = "benchmark";
            drive.driveType = "INTAKE"; drive.bucket = "partial-benchmark";
            drives.insert(drive);
            var profile = new ManagedBackendLedger.Profile(new BackendIdentity("s3", "s3/v1", Map.of(
                    "endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(), "path-style", "true")), "benchmark");
            new ManagedBackendLedger(tx).bind("benchmark", profile);
            var budget = new PayloadBudget(128L * 1024 * 1024);
            var puts = new LongAdder(); var gets = new LongAdder();
            var written = new LongAdder(); var read = new LongAdder(); var observedBudget = new AtomicLong();
            BlobStore measured = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                    new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                        observedBudget.accumulateAndGet(budget.reservedBytes(), Math::max);
                        if (method.getName().equals("put"))
                            assertThat(((BlobStore.PutSpec) args[0]).key()).doesNotContain("//");
                        try {
                            var result = method.invoke(opened.store(), args);
                            if (method.getName().equals("put")) { puts.increment(); written.add(((byte[]) args[1]).length); }
                            if (method.getName().equals("getBounded")) { gets.increment(); read.add(((BlobStore.GetResult) result).data().length); }
                            return result;
                        } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var borrowed = new OpenedBlobStore(measured, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
            var reader = new DocumentPartReader((g, p) -> measured, 4, 64L * 1024 * 1024, budget);
            var writer = new DocumentAttemptWriter(tx, drives, "benchmark", profile.identity(), borrowed, budget);
            var engine = new DocumentOperations(documents, drives, tx, measured, new PartStorage(),
                    new JdbcPurgeQueue(tx), null, "benchmark", reader, writer);
            var caller = new RepositoryCaller("benchmark", true);
            var csv = new StringBuilder("chunks,chunk_payload_bytes,path,sample,elapsed_nanos,puts,gets,put_bytes,get_bytes,observed_reserved_bytes\n");
            try {
                for (int size : new int[] {4096, 262144}) {
                    for (int sample = -2; sample < 12; sample++) {
                        // Alternate execution order. Each measured update has a fresh, equally seeded document.
                        for (int position = 0; position < 2; position++) {
                            boolean partial = Math.floorMod(sample + position, 2) == 0;
                            var search = SearchMetadata.newBuilder();
                            for (int chunk = 0; chunk < 32; chunk++) search.addSemanticResults(SemanticProcessingResult.newBuilder()
                                    .setResultId("chunk-" + chunk).setChunkerConfigId("x".repeat(size)));
                            var original = Document.newBuilder().setDocId(UUID.randomUUID().toString())
                                    .setOwnership(OwnershipContext.newBuilder().setAccountId("benchmark").setDatasourceId("source"))
                                    .setSearchMetadata(search).build();
                            var seed = SaveDocumentRequest.newBuilder().setDocument(original).setDrive(drive.name)
                                    .setUseDatasourceId(true).setGraphId("intake:benchmark").build();
                            var saved = engine.saveDocument(caller, seed);
                            var row = documents.findByNodeId(UUID.fromString(saved.getNodeId())).orElseThrow();
                            var changedChunk = search.getSemanticResults(0).toBuilder().setChunkerConfigId("y" + "x".repeat(size - 1)).build();
                            var expected = original.toBuilder().setSearchMetadata(search.setSemanticResults(0, changedChunk)).build();
                            var request = seed.toBuilder().setDocument(expected);
                            if (partial) request.setDocument(original.toBuilder().setSearchMetadata(
                                    SearchMetadata.newBuilder().addSemanticResults(changedChunk)))
                                    .addPartsWritten(DocumentPart.DOCUMENT_PART_CHUNKS).addChunkSetsWritten("chunk-0")
                                    .setCopyUnwrittenPartsFrom(row.readManifest().getAddress());
                            puts.reset(); gets.reset(); written.reset(); read.reset(); observedBudget.set(0);
                            long start = System.nanoTime();
                            engine.saveDocument(caller, request.build());
                            long elapsed = System.nanoTime() - start;
                            long putCount = puts.sum(), getCount = gets.sum(), putBytes = written.sum(), getBytes = read.sum(), peak = observedBudget.get();
                            assertThat(budget.reservedBytes()).isZero();
                            assertThat(putCount).isEqualTo(33);
                            assertThat(getCount).isEqualTo(partial ? 65 : 33);
                            var actual = engine.getDocument(caller, GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build()).getDocument();
                            assertThat(actual).isEqualTo(expected);
                            if (sample >= 0) csv.append("32,").append(size).append(',').append(partial ? "partial" : "full")
                                    .append(',').append(sample).append(',').append(elapsed).append(',').append(putCount)
                                    .append(',').append(getCount).append(',').append(putBytes).append(',').append(getBytes)
                                    .append(',').append(peak).append('\n');
                        }
                    }
                }
                Path output = Path.of("build/reports/document-partial-benchmark.csv");
                Files.createDirectories(output.getParent()); Files.writeString(output, csv);
                Files.writeString(output.resolveSibling("partial-benchmark-environment.txt"), S3.environment());
            } finally {
                writer.close(); reader.close();
                assertThat(writer.awaitIdle(Duration.ofSeconds(10))).isTrue();
                assertThat(reader.awaitIdle(Duration.ofSeconds(10))).isTrue();
                borrowed.close();
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}

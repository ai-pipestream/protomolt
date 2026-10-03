package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.v1.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Opt-in diagnostic, not a CI timing assertion or production throughput qualification. */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "PROTOMOLT_DOCUMENT_BENCHMARK", matches = "true")
class DocumentStagingBenchmarkIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final String NAMESPACE = "staging-benchmark";
    private static final int WARMUPS = 2;
    private static final int SAMPLES = 12;

    @Test void comparePartCountsAndSizes() throws Exception {
        try (var database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
                var opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                        "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false"))) {
            opened.ensureNamespace(NAMESPACE);
            var tx = new Tx(database.entityManagerFactory());
            var identity = S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true);
            new ManagedBackendLedger(tx).bind("benchmark", new ManagedBackendLedger.Profile(identity, "benchmark"));
            var counts = new Counts(opened.store());
            var borrowed = new OpenedBlobStore(counts.store, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
            var serial = new DocumentPartStager(tx, "benchmark", identity, borrowed, 256L * 1024 * 1024, 1);
            var parallel = new DocumentPartStager(tx, "benchmark", identity, borrowed, 256L * 1024 * 1024, 4);
            var csv = new StringBuilder("parts,bytes_per_part,path,sample,elapsed_nanos,puts,gets,cumulative_put_nanos,cumulative_get_nanos\n");
            try {
                for (int count : new int[] {1, 8, 32}) {
                    for (int size : new int[] {4096, 262144}) {
                        var parts = parts(count, size);
                        for (int sample = -WARMUPS; sample < SAMPLES; sample++) {
                            // Rotate first position across all three paths on the same warmed provider.
                            for (int position = 0; position < 3; position++) {
                                int variant = (sample + WARMUPS + position) % 3;
                                boolean managed = variant != 0;
                                counts.reset();
                                UUID node = UUID.randomUUID(), attempt = UUID.randomUUID();
                                String prefix = "documents/account/" + node + "/attempts/" + attempt + "/";
                                var plan = new DocumentPartAttemptLedger.Plan(attempt,
                                        new DocumentPartAttemptLedger.Location(node, "account", "benchmark", NAMESPACE), 0, Map.of(),
                                        parts.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(p.part(), p.subKey(),
                                                DocumentPartCodec.objectKey(prefix, p.part(), p.subKey()), p.bytes().length,
                                                p.sha256(), "application/protobuf")).toList());
                                var address = NodeAddress.newBuilder().setAccountId("account").setDocId(node.toString())
                                        .setGraphId("benchmark").setGraphAddressId("benchmark").build();
                                long start = System.nanoTime();
                                if (managed) {
                                    var result = (variant == 1 ? serial : parallel).stage(plan, parts, Duration.ofMinutes(2), Map.of());
                                    assertThat(result.attempt().state()).isEqualTo("VERIFIED");
                                    assertThat(result.parts()).hasSize(count);
                                } else {
                                    var result = new PartStorage().writePartObjects(counts.store, NAMESPACE, prefix, parts,
                                            address, null, "application/protobuf", Map.of(), true, 1);
                                    assertThat(result.partObjectKeys()).hasSize(count);
                                }
                                long elapsed = System.nanoTime() - start;
                                assertThat(counts.puts.sum()).isEqualTo(count);
                                assertThat(counts.gets.sum()).isEqualTo(managed ? count : 0);
                                if (sample >= 0) csv.append(count).append(',').append(parts.getFirst().bytes().length).append(',')
                                        .append(managed ? (variant == 1 ? "managed-1" : "managed-4") : "legacy")
                                        .append(',').append(sample).append(',').append(elapsed)
                                        .append(',').append(counts.puts.sum()).append(',').append(counts.gets.sum())
                                        .append(',').append(counts.putNanos.sum()).append(',').append(counts.getNanos.sum()).append('\n');
                                // Untimed verification of every actual stored fragment in both paths.
                                for (var part : parts) assertThat(opened.store().get(NAMESPACE,
                                        DocumentPartCodec.objectKey(prefix, part.part(), part.subKey()), null).data()).isEqualTo(part.bytes());
                            }
                        }
                    }
                }
                Path report = Path.of("build/reports/document-staging-benchmark.csv");
                Files.createDirectories(report.getParent());
                Files.writeString(report, csv);
                System.out.println("Document staging benchmark: " + report.toAbsolutePath());
            } finally {
                serial.close(); parallel.close();
                assertThat(serial.awaitIdle(Duration.ofSeconds(10))).isTrue();
                assertThat(parallel.awaitIdle(Duration.ofSeconds(10))).isTrue();
            }
        }
    }

    private static List<PartObject> parts(int count, int size) {
        // Synthetic protobuf fragments: this benchmark measures byte staging, not schema admission.
        byte[] bytes = Document.newBuilder().setDocId("x".repeat(size)).build().toByteArray();
        String digest = DocumentPartCodec.sha256Hex(bytes);
        var parts = new ArrayList<PartObject>();
        parts.add(new PartObject(DocumentPart.DOCUMENT_PART_CORE, "", bytes, digest));
        for (int i = 1; i < count; i++) parts.add(new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "set-" + i, bytes, digest));
        return List.copyOf(parts);
    }

    private static final class Counts {
        final LongAdder puts = new LongAdder(), gets = new LongAdder(), putNanos = new LongAdder(), getNanos = new LongAdder();
        final BlobStore store;
        Counts(BlobStore delegate) {
            store = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class},
                    (proxy, method, args) -> {
                        long start = System.nanoTime();
                        try {
                            if (method.getName().equals("put")) puts.increment();
                            if (method.getName().equals("get")) gets.increment();
                            return method.invoke(delegate, args);
                        } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                        finally {
                            if (method.getName().equals("put")) putNanos.add(System.nanoTime() - start);
                            if (method.getName().equals("get")) getNanos.add(System.nanoTime() - start);
                        }
                    });
        }
        void reset() { puts.reset(); gets.reset(); putNanos.reset(); getNanos.reset(); }
    }
}

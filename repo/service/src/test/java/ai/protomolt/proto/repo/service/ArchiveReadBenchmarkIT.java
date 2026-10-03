package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.archive.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import com.google.protobuf.ByteString;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real-adapter diagnostic. The unpinned baseline is unsafe during deletion and is test-only. */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "PROTOMOLT_ARCHIVE_READ_BENCHMARK", matches = "true")
@Timeout(600)
class ArchiveReadBenchmarkIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final int POOL = 24, WARMUPS = 2, SAMPLES = 6, READS_PER_WORKER = 4;
    private record Fixture(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest, byte[] expected) {}
    private record Observation(int worker, int read, long elapsed, long provider) {}
    private record Batch(long elapsed, long gets, long bytes, long statements, long transactions,
            double loadBefore, double loadAfter, List<Observation> observations) {}

    @Test void measurePinnedReadsAcrossIndependentAndSharedObjects() throws Exception {
        try (var database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                    POSTGRES.getPassword(), POOL, LedgerConfig.DEFAULT_MIGRATION_LOCATION));
                var opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(),
                        "region", S3.getRegion(), "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(),
                        "path-style", "true", "conditional-writes", "false"));
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            opened.ensureNamespace("read-benchmark");
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            var objects = new ArchiveObjectLedger(tx);
            var drives = new DriveLedger(tx);
            var drive = new DriveRecord();
            drive.driveId = UUID.randomUUID(); drive.accountId = "benchmark"; drive.name = "benchmark";
            drive.driveType = "CUSTOM"; drive.bucket = "read-benchmark"; drive.prefix = "archive";
            drives.insert(drive);
            new ManagedBackendLedger(tx).bind("benchmark", new ManagedBackendLedger.Profile(
                    new BackendIdentity("s3", "s3/v1", Map.of("endpoint", S3.getEndpoint().toString(),
                            "region", S3.getRegion(), "path-style", "true")), "benchmark"));
            var gets = new LongAdder(); var bytes = new LongAdder();
            var providerNanos = ThreadLocal.withInitial(() -> 0L);
            BlobStore measured = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                    new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
                        long start = System.nanoTime();
                        try {
                            Object result = method.invoke(opened.store(), args);
                            if (method.getName().equals("getBounded")) {
                                providerNanos.set(providerNanos.get() + System.nanoTime() - start);
                                gets.increment(); bytes.add(((BlobStore.GetResult) result).data().length);
                            }
                            return result;
                        } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
                if (!generation.equals("benchmark") || !realm.equals("benchmark")) throw new AssertionError("Wrong original backend");
                return measured;
            });
            var operations = new ArchiveOperations(ledger, drives, opened.store(), BridgeEngine.standard(), reader,
                    new ArchiveObjectWriter(new ArchiveUploadLedger(tx), opened.store(), "benchmark", opened.capabilities(), Duration.ofMinutes(5)));
            var caller = new RepositoryCaller("read-benchmark", true);
            operations.createArchive(caller, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId("benchmark").setName("records").setDriveName("benchmark")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            Statistics stats = database.entityManagerFactory().unwrap(SessionFactory.class).getStatistics();
            stats.setStatisticsEnabled(true);
            var csv = new StringBuilder("payload_bytes,sharing,concurrency,pool,path,sample,worker,read,elapsed_nanos,provider_nanos\n");
            var batches = new StringBuilder("payload_bytes,sharing,concurrency,pool,path,sample,operations,batch_nanos,gets,get_bytes,jdbc_statements,hibernate_transactions,host_load_before,host_load_after\n");
            for (int size : new int[]{4096, 262144}) {
                var fixtures = new ArrayList<Fixture>();
                for (int object = 0; object < 16; object++) {
                    byte[] payload = new byte[size];
                    new Random(1000L + object).nextBytes(payload);
                    var saved = operations.putEntry(caller, PutEntryRequest.newBuilder()
                            .setAddress(EntryAddress.newBuilder().setAccountId("benchmark").setArchive("records").setEntryId(UUID.randomUUID().toString()))
                            .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                                    .setData(ByteString.copyFrom(payload))).build());
                    fixtures.add(new Fixture(ledger.findEntry(UUID.fromString(saved.getEntryUuid())).orElseThrow(),
                            saved.getVersion(), saved.getManifest().getRenditions(0), payload));
                }
                for (boolean shared : new boolean[]{false, true}) for (int concurrency : new int[]{1, 4, 16}) {
                    for (int sample = -WARMUPS; sample < SAMPLES; sample++) for (int position = 0; position < 2; position++) {
                        boolean pinned = Math.floorMod(sample + position, 2) == 0;
                        Batch batch = runBatch(executor, fixtures, shared, concurrency, pinned, reader, objects, measured,
                                providerNanos, gets, bytes, stats);
                        assertThat(batch.gets()).isEqualTo((long) concurrency * READS_PER_WORKER);
                        assertThat(batch.bytes()).isEqualTo(batch.gets() * size);
                        long remaining = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_read_pins")
                                .getSingleResult()).longValue());
                        assertThat(remaining).isZero();
                        String prefix = size + "," + (shared ? "same_object" : "disjoint") + "," + concurrency + "," + POOL
                                + "," + (pinned ? "pinned" : "unsafe_unpinned_baseline") + "," + sample + ",";
                        for (var item : batch.observations()) csv.append(prefix).append(item.worker()).append(',').append(item.read())
                                .append(',').append(item.elapsed()).append(',').append(item.provider()).append('\n');
                        batches.append(prefix).append(batch.observations().size()).append(',').append(batch.elapsed()).append(',')
                                .append(batch.gets()).append(',').append(batch.bytes()).append(',').append(batch.statements()).append(',')
                                .append(batch.transactions()).append(',').append(batch.loadBefore()).append(',').append(batch.loadAfter()).append('\n');
                    }
                }
            }
            Path output = Path.of("build/reports/archive-read-benchmark");
            Files.createDirectories(output);
            Files.writeString(output.resolve("operations.csv"), csv);
            Files.writeString(output.resolve("batches.csv"), batches);
            Files.writeString(output.resolve("environment.txt"), "java=" + System.getProperty("java.runtime.version")
                    + "\nos=" + System.getProperty("os.name") + " " + System.getProperty("os.arch")
                    + "\nprocessors=" + Runtime.getRuntime().availableProcessors() + "\npool=" + POOL
                    + "\npostgres=postgres:18-alpine\nprovider=localstack/localstack:3.8\ncontainer_limits=none_explicit\n"
                    + "warmup_batches_per_path=" + WARMUPS + "\nmeasured_batches_per_path=" + SAMPLES
                    + "\nreads_per_worker=" + READS_PER_WORKER + "\n");
        }
    }

    private static Batch runBatch(ExecutorService executor, List<Fixture> fixtures, boolean shared, int concurrency,
            boolean pinned, ArchiveObjectReader reader, ArchiveObjectLedger objects, BlobStore store,
            ThreadLocal<Long> providerNanos, LongAdder gets, LongAdder bytes, Statistics stats) throws Exception {
        var ready = new CountDownLatch(concurrency);
        var start = new CountDownLatch(1);
        var tasks = new ArrayList<Future<List<Observation>>>();
        for (int worker = 0; worker < concurrency; worker++) {
            int index = worker;
            tasks.add(executor.submit(() -> {
                var result = new ArrayList<Observation>();
                var fixture = fixtures.get(shared ? 0 : index);
                ready.countDown();
                if (!start.await(30, TimeUnit.SECONDS)) throw new AssertionError("Benchmark start was not released");
                try {
                    for (int read = 0; read < READS_PER_WORKER; read++) {
                        providerNanos.set(0L);
                        long begin = System.nanoTime();
                        var actual = pinned ? reader.read(fixture.entry(), fixture.version(), fixture.manifest())
                                : unsafeUnpinnedRead(objects, store, fixture);
                        long elapsed = System.nanoTime() - begin;
                        assertThat(actual.data()).isEqualTo(fixture.expected());
                        result.add(new Observation(index, read, elapsed, providerNanos.get()));
                    }
                    return result;
                } finally { providerNanos.remove(); }
            }));
        }
        try {
            if (!ready.await(30, TimeUnit.SECONDS)) throw new AssertionError("Benchmark workers not ready");
            stats.clear(); gets.reset(); bytes.reset();
            double before = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
            long begin = System.nanoTime();
            start.countDown();
            var observations = new ArrayList<Observation>();
            for (var task : tasks) observations.addAll(task.get(60, TimeUnit.SECONDS));
            long elapsed = System.nanoTime() - begin;
            return new Batch(elapsed, gets.sum(), bytes.sum(), stats.getPrepareStatementCount(), stats.getTransactionCount(),
                    before, ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage(), observations);
        } finally {
            start.countDown();
            for (var task : tasks) if (!task.isDone()) task.cancel(true);
        }
    }

    /** Previous read lifecycle, solely to quantify coordination cost. Never use with concurrent mutation. */
    private static BlobStore.GetResult unsafeUnpinnedRead(ArchiveObjectLedger objects, BlobStore store, Fixture fixture) {
        var manifest = fixture.manifest();
        var readable = objects.readable(fixture.entry().entryUuid, fixture.version(), UUID.fromString(manifest.getStorageObjectId())).orElseThrow();
        var binding = readable.binding(); var location = binding.location();
        if (!location.accountId().equals(fixture.entry().accountId) || !location.archive().equals(fixture.entry().archive)
                || !location.objectKey().equals(manifest.getObjectKey()) || readable.size() != manifest.getSizeBytes()
                || !readable.sha256().equals(manifest.getSha256()) || !location.backendGeneration().equals("benchmark")
                || !binding.storageRealm().equals("benchmark")) throw new AssertionError("Baseline binding mismatch");
        var result = store.getBounded(location.bucket(), location.objectKey(), readable.providerVersion(), Math.toIntExact(readable.size()));
        if (result.data().length != readable.size() || !ArchiveManifests.sha256Hex(result.data()).equals(readable.sha256()))
            throw new AssertionError("Baseline byte verification failed");
        return result;
    }
}

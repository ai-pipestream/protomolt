package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Opt-in archive transport diagnostic; LocalStack is not a production capacity model. */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "PROTOMOLT_REPLICA_BENCHMARK", matches = "true")
@Timeout(600)
class RepositoryScaleBenchmarkIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer("localstack/localstack:3.8").withServices("s3");
    private static final int WORKERS = 8, SAMPLES = 3, CYCLES = 3;
    private final Path output = Path.of("build/reports/replica-scale", Instant.now().toString().replace(':', '-'));
    private final Queue<String> observations = new ConcurrentLinkedQueue<>();
    private final List<String> windows = new ArrayList<>();

    @Test void measureRealTransportAcrossReplicaCounts() throws Exception {
        Files.createDirectories(output);
        Files.writeString(output.resolve("environment.txt"), "java=" + System.getProperty("java.runtime.version")
                + "\nos=" + System.getProperty("os.name") + "\nprocessors=" + Runtime.getRuntime().availableProcessors()
                + "\nworkers=" + WORKERS + "\npayload_bytes=4096\nheap_per_child=256MiB\n"
                + "load_average_begin=" + java.lang.management.ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage() + "\n"
                + "Fixed mode fixes aggregate SQL connections only; CPU and heap are not capped in aggregate.\n"
                + "Configurations run in fixed order; retained history grows across windows. Warmup sample is -1.\n"
                + "Memory files are Linux post-window snapshots, not measured per-window peaks.\n"
                + "Provider timing, SQL acquisition wait and lock-wait duration are not instrumented.\n");
        try {
            for (String mode : List.of("fixed_sql", "added_sql")) for (int replicas : new int[]{1, 2, 4}) {
                int pool = mode.equals("fixed_sql") ? 8 / replicas : 4;
                String run = mode + "-" + replicas;
                String token = UUID.randomUUID().toString();
                try (var group = new Hosts()) {
                    var hosts = group.hosts;
                    for (int i = 0; i < replicas; i++) hosts.add(launch(run + "-" + i, token, pool));
                    var stubs = hosts.stream().map(host -> stub(host, token)).toList();
                    var headers = new Metadata(); headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                    DriveServiceGrpc.newBlockingStub(hosts.getFirst().channel()).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                            .withDeadlineAfter(20, TimeUnit.SECONDS).createDrive(CreateDriveRequest.newBuilder().setAccountId(run).setName("storage").build());
                    stubs.getFirst().withDeadlineAfter(20, TimeUnit.SECONDS).createArchive(CreateArchiveRequest.newBuilder()
                            .setArchive(Archive.newBuilder().setAccountId(run).setName("records").setDriveName("storage")
                                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                    var entries = new ArrayList<Entry>();
                    for (int worker = 0; worker < WORKERS; worker++) {
                        var address = EntryAddress.newBuilder().setAccountId(run).setArchive("records").setEntryId("entry-" + worker).build();
                        var original = payload(worker, 0);
                        var saved = stubs.get(worker % replicas).withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(put(address, original, 0));
                        entries.add(new Entry(address, original, saved.getVersion()));
                    }
                    for (int sample = -1; sample < SAMPLES; sample++) {
                        mixed(run, pool, sample, stubs, entries);
                        contended(run, pool, sample, stubs);
                        for (int i = 0; i < hosts.size(); i++) {
                            Files.writeString(output.resolve(run + "-" + sample + "-" + i + "-memory.txt"),
                                    Files.readString(Path.of("/proc", Long.toString(hosts.get(i).process().pid()), "status")));
                        }
                    }
                }
            }
        } finally {
            Files.writeString(output.resolve("requests.csv"), "run,pool_per_child,sample,phase,worker,host,operation,elapsed_nanos,status\n" + String.join("\n", observations) + "\n");
            Files.writeString(output.resolve("windows.csv"), "run,pool_per_child,sample,phase,operations,elapsed_nanos\n" + String.join("\n", windows) + "\n");
        }
    }

    private void mixed(String run, int pool, int sample, List<ArchiveServiceGrpc.ArchiveServiceBlockingStub> stubs,
            List<Entry> entries) throws Exception {
        var start = new CyclicBarrier(WORKERS + 1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<?>>();
            for (int w = 0; w < WORKERS; w++) {
                int worker = w;
                futures.add(executor.submit(() -> {
                    start.await(20, TimeUnit.SECONDS);
                    var entry = entries.get(worker);
                    for (int cycle = 0; cycle < CYCLES; cycle++) {
                        int index = Math.floorMod(worker + cycle + sample, stubs.size());
                        var writer = stubs.get(index);
                        var bytes = payload(worker, ++entry.sequence);
                        var request = put(entry.address, bytes, entry.version);
                        var saved = timed(run, pool, sample, "mixed", worker, index, "put", () -> writer.withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(request));
                        assertThat(saved.getVersion()).isEqualTo(++entry.version);
                        int next = (index + 1) % stubs.size();
                        var reader = stubs.get(next);
                        var current = timed(run, pool, sample, "mixed", worker, next, "read_current", () -> reader.withDeadlineAfter(20, TimeUnit.SECONDS)
                                .getEntry(GetEntryRequest.newBuilder().setAddress(entry.address).build()));
                        assertThat(current.getRenditions(0).getData()).isEqualTo(bytes);
                        var historical = timed(run, pool, sample, "mixed", worker, next, "read_history", () -> reader.withDeadlineAfter(20, TimeUnit.SECONDS)
                                .getEntry(GetEntryRequest.newBuilder().setAddress(entry.address).setVersion(entry.originalVersion).build()));
                        assertThat(historical.getRenditions(0).getData()).isEqualTo(entry.original);
                        var replay = timed(run, pool, sample, "mixed", worker, next, "retry", () -> reader.withDeadlineAfter(20, TimeUnit.SECONDS)
                                .putEntry(request.toBuilder().clearExpectedVersion().build()));
                        assertThat(replay.getVersion()).isEqualTo(saved.getVersion());
                        assertThat(replay.getManifest()).isEqualTo(saved.getManifest());
                    }
                    return null;
                }));
            }
            long begin = System.nanoTime(); start.await(20, TimeUnit.SECONDS);
            for (var future : futures) future.get(90, TimeUnit.SECONDS);
            windows.add(run + "," + pool + "," + sample + ",mixed," + (WORKERS * CYCLES * 4) + "," + (System.nanoTime() - begin));
        }
    }

    private void contended(String run, int pool, int sample, List<ArchiveServiceGrpc.ArchiveServiceBlockingStub> stubs) throws Exception {
        var address = EntryAddress.newBuilder().setAccountId(run).setArchive("records").setEntryId("hot-" + sample).build();
        var original = payload(100, 0);
        var baseline = stubs.getFirst().withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(put(address, original, 0));
        var start = new CyclicBarrier(WORKERS + 1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<Integer>>();
            for (int w = 0; w < WORKERS; w++) {
                int worker = w, host = w % stubs.size();
                futures.add(executor.submit(() -> {
                    start.await(20, TimeUnit.SECONDS);
                    try {
                        var saved = timed(run, pool, sample, "contended", worker, host, "put", () -> stubs.get(host).withDeadlineAfter(20, TimeUnit.SECONDS)
                                .putEntry(put(address, payload(worker, sample + 100), baseline.getVersion())));
                        assertThat(saved.getVersion()).isEqualTo(baseline.getVersion() + 1);
                        return worker;
                    } catch (StatusRuntimeException failure) {
                        if (failure.getStatus().getCode() != Status.Code.ABORTED) throw failure;
                        return -1;
                    }
                }));
            }
            long begin = System.nanoTime(); start.await(20, TimeUnit.SECONDS);
            var winners = new ArrayList<Integer>();
            for (var future : futures) { int winner = future.get(90, TimeUnit.SECONDS); if (winner >= 0) winners.add(winner); }
            windows.add(run + "," + pool + "," + sample + ",contended," + WORKERS + "," + (System.nanoTime() - begin));
            assertThat(winners).hasSize(1);
            for (var stub : stubs) {
                assertThat(stub.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(GetEntryRequest.newBuilder().setAddress(address).build())
                        .getRenditions(0).getData()).isEqualTo(payload(winners.getFirst(), sample + 100));
                assertThat(stub.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(GetEntryRequest.newBuilder().setAddress(address).setVersion(baseline.getVersion()).build())
                        .getRenditions(0).getData()).isEqualTo(original);
            }
        }
    }

    private <T> T timed(String run, int pool, int sample, String phase, int worker, int host, String operation, Callable<T> call) throws Exception {
        long begin = System.nanoTime(); String code = "OK";
        try { return call.call(); }
        catch (Exception failure) { code = Status.fromThrowable(failure).getCode().name(); throw failure; }
        finally { observations.add(run + "," + pool + "," + sample + "," + phase + "," + worker + "," + host + "," + operation + "," + (System.nanoTime() - begin) + "," + code); }
    }
    private static final class Entry {
        final EntryAddress address; final ByteString original; final long originalVersion;
        long version; int sequence;
        Entry(EntryAddress address, ByteString original, long version) { this.address = address; this.original = original; this.originalVersion = version; this.version = version; }
    }
    private static ByteString payload(int worker, int revision) {
        byte[] bytes = new byte[4096]; new Random(((long) worker << 32) + revision).nextBytes(bytes); return ByteString.copyFrom(bytes);
    }
    private static PutEntryRequest put(EntryAddress address, ByteString bytes, long expected) {
        return PutEntryRequest.newBuilder().setAddress(address).setExpectedVersion(expected).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(bytes)).build();
    }
    private static ArchiveServiceGrpc.ArchiveServiceBlockingStub stub(Host host, String token) {
        var headers = new Metadata(); headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return ArchiveServiceGrpc.newBlockingStub(host.channel()).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }
    private Host launch(String name, String token, int pool) throws Exception {
        Path ready = output.resolve(name + ".port").toAbsolutePath(), log = output.resolve(name + ".log").toAbsolutePath();
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m", "-cp",
                System.getProperty("protomolt.test.runtimeClasspath"), ReplicaHostProcess.class.getName(), ready.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        var env = builder.environment();
        env.put("TEST_JDBC", POSTGRES.getJdbcUrl()); env.put("TEST_DB_USER", POSTGRES.getUsername()); env.put("TEST_DB_PASSWORD", POSTGRES.getPassword());
        env.put("TEST_S3_ENDPOINT", S3.getEndpoint().toString()); env.put("TEST_S3_REGION", S3.getRegion()); env.put("TEST_S3_KEY", S3.getAccessKey());
        env.put("TEST_S3_SECRET", S3.getSecretKey()); env.put("TEST_API_TOKEN", token); env.put("TEST_POOL_SIZE", Integer.toString(pool));
        var process = builder.start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (process.isAlive() && System.nanoTime() < deadline) {
                if (Files.exists(ready)) return new Host(process, NettyChannelBuilder.forAddress("127.0.0.1", Integer.parseInt(Files.readString(ready)))
                        .usePlaintext().build());
                Thread.sleep(50);
            }
            throw new AssertionError("Replica startup failed: " + Files.readString(log));
        } catch (Throwable failure) {
            process.destroyForcibly(); if (!process.waitFor(10, TimeUnit.SECONDS)) failure.addSuppressed(new IllegalStateException("Child did not exit"));
            throw failure;
        }
    }
    private record Host(Process process, ManagedChannel channel) implements AutoCloseable {
        @Override public void close() throws Exception {
            channel.shutdownNow(); process.destroyForcibly();
            boolean channelStopped = channel.awaitTermination(10, TimeUnit.SECONDS);
            boolean processStopped = process.waitFor(10, TimeUnit.SECONDS);
            if (!channelStopped || !processStopped) throw new IllegalStateException("Replica did not terminate");
        }
    }
    private static final class Hosts implements AutoCloseable {
        final List<Host> hosts = new ArrayList<>();
        @Override public void close() throws Exception {
            Exception failure = null;
            for (var host : hosts.reversed()) try { host.close(); } catch (Exception e) {
                if (failure == null) failure = e; else failure.addSuppressed(e);
            }
            if (failure != null) throw failure;
        }
    }
}

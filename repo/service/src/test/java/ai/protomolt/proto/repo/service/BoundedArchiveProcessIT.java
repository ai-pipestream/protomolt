package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.file.*;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Production main, separate JVM and real SQL/Redis; no injected provider implementation. */
@Testcontainers
class BoundedArchiveProcessIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    @TempDir Path directory;

    @Test void productionProcessRestartsWithoutRelocatingDriveAndRetainsExactRetry() throws Exception {
        var env = environment();
        var address = EntryAddress.newBuilder().setAccountId(env.get("DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT"))
                .setArchive("records").setEntryId("entry").build();
        var payload = ByteString.copyFromUtf8("production archive process");
        var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(payload)).build();
        PutEntryResponse saved;
        try (var host = launch(env)) {
            var stub = host.archive(env);
            assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(host.channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getArchive(GetArchiveRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
            assertThatThrownBy(() -> DriveServiceGrpc.newBlockingStub(host.channel).withInterceptors(auth(env))
                    .withDeadlineAfter(5, TimeUnit.SECONDS).createDrive(CreateDriveRequest.getDefaultInstance()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
            stub.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId(address.getAccountId())
                    .setName("records").setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            saved = stub.putEntry(request);
            assertThat(stub.putEntry(request)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
            assertThat(stub.getEntry(GetEntryRequest.newBuilder().setAddress(address).build()).getRenditions(0).getData()).isEqualTo(payload);
        }
        String location = driveLocation(address.getAccountId());
        env.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "changed-default");
        try (var restarted = launch(env)) {
            assertThat(driveLocation(address.getAccountId())).isEqualTo(location);
            assertThat(restarted.archive(env).putEntry(request)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
            assertThat(restarted.archive(env).getEntry(GetEntryRequest.newBuilder().setAddress(address).build())
                    .getRenditions(0).getData()).isEqualTo(payload);
        }
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var update = connection.prepareStatement("UPDATE drives SET status='SUSPENDED' WHERE account_id=?")) {
            update.setString(1, address.getAccountId()); assertThat(update.executeUpdate()).isEqualTo(1);
        }
        assertFails(env, "Archive bootstrap drive must be active");
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var update = connection.prepareStatement("UPDATE drives SET status='ACTIVE',provider='s3' WHERE account_id=?")) {
            update.setString(1, address.getAccountId()); assertThat(update.executeUpdate()).isEqualTo(1);
        }
        assertFails(env, "Drive provider does not match the selected storage backend");
    }

    @Test void sigtermWaitsPastDrainDeadlineForAcceptedSqlCommitThenRestartReadsIt() throws Exception {
        var env = environment();
        var address = EntryAddress.newBuilder().setAccountId(env.get("DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT"))
                .setArchive("records").setEntryId("held").build();
        var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(ByteString.copyFromUtf8("before"))).build();
        var candidate = request.toBuilder().setExpectedVersion(1).setRenditions(0,
                request.getRenditions(0).toBuilder().setData(ByteString.copyFromUtf8("after drain"))).build();
        try (var host = launch(env);
                var lock = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var stub = host.archive(env);
            stub.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId(address.getAccountId())
                    .setName("records").setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            var saved = stub.putEntry(request);
            lock.setAutoCommit(false);
            try {
                int blocker;
                try (var query = lock.createStatement(); var result = query.executeQuery("SELECT pg_backend_pid()")) {
                    result.next(); blocker = result.getInt(1);
                }
                try (var query = lock.prepareStatement("SELECT entry_uuid FROM archive_entries WHERE entry_uuid=? FOR NO KEY UPDATE")) {
                    query.setObject(1, UUID.fromString(saved.getEntryUuid()));
                    try (var result = query.executeQuery()) { assertThat(result.next()).isTrue(); }
                }
                var pending = workers.submit(() -> stub.withDeadlineAfter(45, TimeUnit.SECONDS).putEntry(candidate));
                try (var observer = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                        var query = observer.prepareStatement("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)) AND query ILIKE '%archive_entries%' AND query ILIKE '%for%update%')")) {
                    query.setInt(1, blocker);
                    boolean blocked = false;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (!pending.isDone() && System.nanoTime() < deadline) {
                        try (var result = query.executeQuery()) { result.next(); blocked = result.getBoolean(1); }
                        if (blocked) break;
                        Thread.sleep(25);
                    }
                    assertThat(blocked).as("accepted write reached actual SQL commit lock").isTrue();
                    host.process.destroy();
                    assertThat(host.process.waitFor(11, TimeUnit.SECONDS)).as("SIGTERM must preserve accepted write beyond first drain timeout").isFalse();
                    assertThat(pending.isDone()).isFalse();
                    assertThat(Files.readString(host.log)).contains("Archive shutdown waiting for ARCHIVE_RPC");
                    lock.rollback();
                    assertThat(pending.get(10, TimeUnit.SECONDS).getVersion()).isEqualTo(2);
                    assertThat(host.process.waitFor(15, TimeUnit.SECONDS)).isTrue();
                    assertThat(host.process.exitValue()).isEqualTo(143);
                    assertThat(Files.readString(host.log)).doesNotContain("Exception in thread", "Shutdown failed");
                }
            } finally { lock.rollback(); }
        }
        try (var restarted = launch(env)) {
            assertThat(restarted.archive(env).getEntry(GetEntryRequest.newBuilder().setAddress(address).build())
                    .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("after drain"));
        }
    }

    @Test void invalidConfigurationExitsBeforeConnectingToUnavailableDatabase() throws Exception {
        var env = environment();
        env.put(LedgerConfig.ENV_JDBC_URL, "jdbc:postgresql://127.0.0.1:1/unavailable");
        env.remove("PROTOMOLT_API_TOKEN");
        assertFails(env, "PROTOMOLT_API_TOKEN is required");
        env.put("PROTOMOLT_API_TOKEN", "test-token");
        env.put("DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES", "not-a-number");
        assertFails(env, "DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES must be an integer");
        env.remove("DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES");
        env.put(RepoServiceConfig.ENV_BLOB_STORE, "s3");
        assertFails(env, "Bounded archive qualification requires managed Redis");
    }

    private Map<String, String> environment() {
        var env = RepoBoundedArchiveMainTest.environment();
        env.put("DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT", "process-" + UUID.randomUUID());
        env.put(RepoServiceConfig.ENV_GRPC_PORT, "0");
        env.put(LedgerConfig.ENV_JDBC_URL, POSTGRES.getJdbcUrl());
        env.put(LedgerConfig.ENV_USERNAME, POSTGRES.getUsername());
        env.put(LedgerConfig.ENV_PASSWORD, POSTGRES.getPassword());
        env.put(RepoServiceConfig.ENV_REDIS_URI, "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        env.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "process-archives");
        return env;
    }

    private Process start(Map<String, String> env, Path log) throws Exception {
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("protomolt.test.runtimeClasspath"), RepoBoundedArchiveMain.class.getName())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(key -> key.startsWith("DOCUMENT_PLATFORM_") || key.equals("PROTOMOLT_API_TOKEN"));
        builder.environment().putAll(env);
        return builder.start();
    }

    private Host launch(Map<String, String> env) throws Exception {
        Path log = directory.resolve(UUID.randomUUID() + ".log");
        var process = start(env, log);
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (process.isAlive() && System.nanoTime() < deadline) {
                var ready = Files.readAllLines(log).stream().filter(line -> line.startsWith("PROTOMOLT_ARCHIVE_READY port=")).findFirst();
                if (ready.isPresent()) return new Host(process, NettyChannelBuilder.forAddress("127.0.0.1",
                        Integer.parseInt(ready.get().substring(ready.get().indexOf('=') + 1))).usePlaintext().build(), log);
                Thread.sleep(50);
            }
            throw new AssertionError("Archive process failed readiness: " + Files.readString(log));
        } catch (Throwable failure) {
            process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); throw failure;
        }
    }

    private void assertFails(Map<String, String> env, String message) throws Exception {
        Path log = directory.resolve(UUID.randomUUID() + ".log");
        var process = start(env, log);
        try {
            assertThat(process.waitFor(20, TimeUnit.SECONDS)).as("invalid configuration must exit").isTrue();
            assertThat(process.exitValue()).isNotZero();
            assertThat(Files.readString(log)).contains(message).doesNotContain("PROTOMOLT_ARCHIVE_READY");
        } finally { if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); } }
    }

    private String driveLocation(String account) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT bucket,prefix FROM drives WHERE account_id=? AND name='storage'")) {
            query.setString(1, account);
            try (var result = query.executeQuery()) { assertThat(result.next()).isTrue(); return result.getString(1) + "/" + result.getString(2); }
        }
    }

    private static ClientInterceptor auth(Map<String, String> env) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), env.get("PROTOMOLT_API_TOKEN"));
        return MetadataUtils.newAttachHeadersInterceptor(headers);
    }

    private record Host(Process process, ManagedChannel channel, Path log) implements AutoCloseable {
        ArchiveServiceGrpc.ArchiveServiceBlockingStub archive(Map<String, String> env) {
            return ArchiveServiceGrpc.newBlockingStub(channel).withInterceptors(auth(env)).withDeadlineAfter(20, TimeUnit.SECONDS);
        }
        @Override public void close() throws Exception {
            try {
                if (process.isAlive()) process.destroy();
                assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("archive process must drain: %s", Files.readString(log)).isTrue();
                assertThat(process.exitValue()).as("expected SIGTERM exit on Linux").isEqualTo(143);
                assertThat(Files.readString(log)).doesNotContain("Exception in thread", "Shutdown failed");
            } finally {
                if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
                channel.shutdownNow(); channel.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }
}

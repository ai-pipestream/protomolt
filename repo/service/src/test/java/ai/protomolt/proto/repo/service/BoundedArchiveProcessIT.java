package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.redis.*;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
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

    @Test void sigkillDuringPublicationPreservesHeadAndRestartReclaimsUnpublishedBytes() throws Exception {
        var env = environment();
        env.put(RepoServiceConfig.ENV_SWEEP_INTERVAL_MS, "250");
        env.put(RepoServiceConfig.ENV_RECONCILE_MIN_AGE_MS, "0");
        var address = EntryAddress.newBuilder().setAccountId(env.get("DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT"))
                .setArchive("records").setEntryId("killed").build();
        var before = ByteString.copyFromUtf8("committed before crash");
        var after = ByteString.copyFromUtf8("candidate interrupted by crash");
        var initial = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(before)).build();
        var candidate = initial.toBuilder().setExpectedVersion(1).setRenditions(0,
                initial.getRenditions(0).toBuilder().setData(after)).build();
        UUID abandoned;
        String namespace, key;
        try (var direct = new RedisBlobStore(new RedisBlobStoreConfig(env.get(RepoServiceConfig.ENV_REDIS_URI),
                0, 1048576, "", RedisWritePolicy.CREATE_ONLY));
                var sql = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            var host = launch(env);
            try (var lock = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                    var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var stub = host.archive(env);
                stub.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId(address.getAccountId())
                        .setName("records").setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var saved = stub.putEntry(initial);
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
                    try (var query = sql.prepareStatement("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)) AND query ILIKE '%archive_entries%' AND query ILIKE '%for%update%')")) {
                        query.setInt(1, blocker);
                        boolean blocked = false;
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                        while (!pending.isDone() && System.nanoTime() < deadline) {
                            try (var result = query.executeQuery()) { result.next(); blocked = result.getBoolean(1); }
                            if (blocked) break;
                            Thread.sleep(25);
                        }
                        assertThat(blocked).as("candidate reached actual publication lock after provider write").isTrue();
                    }
                    try (var query = sql.prepareStatement("SELECT b.object_id,b.bucket,b.object_key,u.state FROM archive_object_bindings b JOIN archive_object_uploads u USING(object_id) WHERE b.account_id=? AND u.state<>'LIVE'")) {
                        query.setString(1, address.getAccountId());
                        try (var result = query.executeQuery()) {
                            assertThat(result.next()).isTrue();
                            abandoned = result.getObject(1, UUID.class); namespace = result.getString(2); key = result.getString(3);
                            assertThat(result.getString(4)).isEqualTo("VERIFIED");
                            assertThat(direct.get(namespace, key, null).data()).isEqualTo(after.toByteArray());
                            assertThat(result.next()).isFalse();
                        }
                    }
                    host.process.destroyForcibly();
                    assertThat(host.process.waitFor(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(host.process.exitValue()).as("Linux SIGKILL bypasses shutdown hooks").isEqualTo(137);
                    assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class)
                            .satisfies(failure -> assertThat(Status.fromThrowable(failure.getCause()).getCode()).isEqualTo(Status.Code.UNAVAILABLE));
                } finally { lock.rollback(); }
            } finally {
                if (host.process.isAlive()) { host.process.destroyForcibly(); host.process.waitFor(10, TimeUnit.SECONDS); }
                host.channel.shutdownNow(); assertThat(host.channel.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
            try (var restarted = launch(env)) {
                var old = restarted.archive(env).getEntry(GetEntryRequest.newBuilder().setAddress(address).build());
                assertThat(old.getRenditions(0).getData()).isEqualTo(before);
                var saved = restarted.archive(env).putEntry(candidate);
                assertThat(saved.getVersion()).isEqualTo(2);
                assertThat(saved.getManifest().getRenditions(0).getStorageObjectId()).isNotEqualTo(abandoned.toString());
                assertThatThrownBy(() -> restarted.archive(env).putEntry(candidate))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));
                var current = candidate.toBuilder().setExpectedVersion(2).build();
                assertThat(restarted.archive(env).putEntry(current)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
                awaitKilledUploadCleanup(sql, abandoned);
                String deletedNamespace = namespace, deletedKey = key;
                assertThatThrownBy(() -> direct.get(deletedNamespace, deletedKey, null)).isInstanceOf(BlobStore.BlobNotFoundException.class);
                assertThat(restarted.archive(env).getEntry(GetEntryRequest.newBuilder().setAddress(address).build())
                        .getRenditions(0).getData()).isEqualTo(after);
                assertThat(restarted.archive(env).getEntry(GetEntryRequest.newBuilder().setAddress(address).setVersion(1).build())
                        .getRenditions(0).getData()).isEqualTo(before);
                assertThat(restarted.archive(env).putEntry(current)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
            }
        }
    }

    private static void awaitKilledUploadCleanup(java.sql.Connection sql, UUID object) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(6);
        boolean observedActiveLease = false;
        try (var query = sql.prepareStatement("SELECT state,lease_until<=clock_timestamp(),(SELECT count(*) FROM archive_version_object_refs WHERE object_id=?) FROM archive_object_uploads WHERE object_id=?")) {
            query.setObject(1, object); query.setObject(2, object);
            while (System.nanoTime() < deadline) {
                try (var result = query.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getLong(3)).as("killed candidate never becomes a version reference").isZero();
                    String state = result.getString(1); boolean expired = result.getBoolean(2);
                    if (!expired) { observedActiveLease = true; assertThat(state).isEqualTo("VERIFIED"); }
                    if ("DELETED".equals(state)) {
                        assertThat(observedActiveLease).isTrue(); assertThat(expired).isTrue(); return;
                    }
                }
                Thread.sleep(250);
            }
        }
        throw new AssertionError("Killed upload was not reclaimed after its real five-minute lease");
    }

    @Test void concurrentBootstrapUsesOneWinningDriveLocationAcrossProcesses() throws Exception {
        // Initialize migrations before installing the real INSERT barrier.
        try (var seed = launch(environment())) { assertThat(seed.process.isAlive()).isTrue(); }
        var firstEnv = environment();
        var secondEnv = new HashMap<>(firstEnv);
        firstEnv.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "first-default");
        secondEnv.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "second-default");
        String account = firstEnv.get("DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT");
        Path firstLog = directory.resolve("bootstrap-first.log"), secondLog = directory.resolve("bootstrap-second.log");
        Process first = null, second = null;
        try (var lock = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            try (var sql = lock.createStatement()) {
                // The fixture account is a generated UUID, never caller-controlled SQL.
                sql.execute("CREATE FUNCTION hold_archive_bootstrap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        + "IF NEW.account_id='" + account + "' THEN PERFORM pg_advisory_xact_lock(6840319); END IF; RETURN NEW; END $$");
                sql.execute("CREATE TRIGGER hold_archive_bootstrap BEFORE INSERT ON drives FOR EACH ROW EXECUTE FUNCTION hold_archive_bootstrap()");
            }
            lock.setAutoCommit(false);
            try {
                int blocker;
                try (var sql = lock.createStatement(); var result = sql.executeQuery("SELECT pg_backend_pid(),pg_advisory_xact_lock(6840319)")) {
                    result.next(); blocker = result.getInt(1);
                }
                first = start(firstEnv, firstLog); second = start(secondEnv, secondLog);
                try (var observer = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                        var query = observer.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)) AND query ILIKE '%insert%drives%'")) {
                    query.setInt(1, blocker);
                    long waiting = 0, deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (first.isAlive() && second.isAlive() && System.nanoTime() < deadline) {
                        try (var result = query.executeQuery()) { result.next(); waiting = result.getLong(1); }
                        if (waiting == 2) break;
                        Thread.sleep(25);
                    }
                    assertThat(waiting).as("both production bootstraps must reach the competing INSERT").isEqualTo(2);
                }
            } finally {
                lock.rollback(); lock.setAutoCommit(true);
            }
            try (var a = awaitReady(first, firstLog); var b = awaitReady(second, secondLog)) {
                String location = driveLocation(account);
                assertThat(location).matches("(first|second)-default-.+");
                try (var sql = lock.prepareStatement("SELECT count(*) FROM drives WHERE account_id=? AND name='storage'")) {
                    sql.setString(1, account);
                    try (var result = sql.executeQuery()) { result.next(); assertThat(result.getLong(1)).isEqualTo(1); }
                }
                var writer = a.archive(firstEnv); var reader = b.archive(secondEnv);
                writer.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId(account)
                        .setName("records").setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records").setEntryId("shared").build();
                var payload = ByteString.copyFromUtf8("shared winning bootstrap");
                var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(payload)).build();
                var saved = writer.putEntry(request);
                assertThat(reader.getEntry(GetEntryRequest.newBuilder().setAddress(address).build()).getRenditions(0).getData()).isEqualTo(payload);
                assertThat(reader.putEntry(request)).isEqualTo(saved.toBuilder().setDeduplicated(true).build());
                assertThat(driveLocation(account)).isEqualTo(location);
            }
        } finally {
            for (var process : new Process[]{first, second}) {
                if (process != null && process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
            }
            try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                    var sql = connection.createStatement()) {
                sql.execute("DROP TRIGGER IF EXISTS hold_archive_bootstrap ON drives");
                sql.execute("DROP FUNCTION IF EXISTS hold_archive_bootstrap()");
            }
        }
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
        return awaitReady(process, log);
    }

    private Host awaitReady(Process process, Path log) throws Exception {
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

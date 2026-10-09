package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Separate JVMs and TCP, real PostgreSQL and LocalStack; no throughput claim. */
@Testcontainers
class RepositoryReplicaProcessIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @TempDir Path directory;

    @Test void survivingProcessReadsAndRepeatsCompletedWriteAfterWriterIsKilled() throws Exception {
        String token = UUID.randomUUID().toString();
        try (var first = launch("first", token); var second = launch("second", token)) {
            var a = NettyChannelBuilder.forAddress("127.0.0.1", first.port()).usePlaintext().build();
            io.grpc.ManagedChannel b = null;
            try {
                b = NettyChannelBuilder.forAddress("127.0.0.1", second.port()).usePlaintext().build();
                var headers = new Metadata();
                headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                var auth = MetadataUtils.newAttachHeadersInterceptor(headers);
                DriveServiceGrpc.newBlockingStub(a).withInterceptors(auth).withDeadlineAfter(20, TimeUnit.SECONDS)
                        .createDrive(CreateDriveRequest.newBuilder().setAccountId("process-account").setName("storage").build());
                var writer = ArchiveServiceGrpc.newBlockingStub(a).withInterceptors(auth);
                var survivor = ArchiveServiceGrpc.newBlockingStub(b).withInterceptors(auth);
                survivor.withDeadlineAfter(20, TimeUnit.SECONDS).createArchive(CreateArchiveRequest.newBuilder()
                        .setArchive(Archive.newBuilder().setAccountId("process-account").setName("records")
                                .setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var address = EntryAddress.newBuilder().setAccountId("process-account").setArchive("records").setEntryId("entry").build();
                var payload = ByteString.copyFromUtf8("durable across service processes");
                var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(payload)).build();
                var saved = writer.withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(request);
                assertCompetingRevisions(writer, survivor, request.toBuilder().setAddress(
                        address.toBuilder().setEntryId("contended")).build());
                assertStats(survivor, 2, 3);
                var abandoned = killWriterBlockedBeforeCommit(first, writer, request, saved);
                assertThat(first.process().isAlive()).isFalse();
                var replay = survivor.withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(request);
                assertThat(replay.getEntryUuid()).isEqualTo(saved.getEntryUuid());
                assertThat(replay.getVersion()).isEqualTo(saved.getVersion());
                assertThat(replay.getManifest().getRenditions(0).getStorageObjectId())
                        .isEqualTo(saved.getManifest().getRenditions(0).getStorageObjectId());
                var nextPayload = ByteString.copyFromUtf8("new revision from surviving process");
                var updated = survivor.withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(request.toBuilder()
                        .setRenditions(0, request.getRenditions(0).toBuilder().setData(nextPayload)).build());
                assertThat(updated.getVersion()).isEqualTo(saved.getVersion() + 1);
                var current = survivor.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(
                        GetEntryRequest.newBuilder().setAddress(address).build());
                assertThat(current.getRenditions(0).getData()).isEqualTo(nextPayload);
                var read = survivor.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(
                        GetEntryRequest.newBuilder().setAddress(address).setVersion(saved.getVersion()).build());
                assertThat(read.getRenditions(0).getData()).isEqualTo(payload);
                assertStats(survivor, 2, 4);
                awaitPhysicalCleanup(abandoned, second);
                var retainedAfterCleanup = survivor.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(
                        GetEntryRequest.newBuilder().setAddress(address).setVersion(saved.getVersion()).build());
                assertThat(retainedAfterCleanup.getRenditions(0).getData()).isEqualTo(payload);
                assertStats(survivor, 2, 4);
            } finally {
                a.shutdownNow();
                if (b != null) b.shutdownNow();
                a.awaitTermination(10, TimeUnit.SECONDS);
                if (b != null) b.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    private record Abandoned(UUID objectId, String bucket, String key) {}

    private void awaitPhysicalCleanup(Abandoned abandoned, Host survivor) throws Exception {
        try (var store = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(software.amazon.awssdk.regions.Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey()))).build();
                var connection = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT state,lease_until<=clock_timestamp() FROM archive_object_uploads WHERE object_id=?")) {
            var head = software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
                    .bucket(abandoned.bucket()).key(abandoned.key()).build();
            assertThat(store.headObject(head).contentLength()).isPositive();
            query.setObject(1, abandoned.objectId());
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(6);
            boolean deleted = false;
            while (System.nanoTime() < deadline) {
                assertThat(survivor.process().isAlive()).as("surviving recovery process").isTrue();
                try (var result = query.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    if ("DELETED".equals(result.getString(1))) {
                        assertThat(result.getBoolean(2)).as("real database lease elapsed before cleanup").isTrue();
                        deleted = true;
                        break;
                    }
                }
                Thread.sleep(1000);
            }
            assertThat(deleted).as("survivor reclaimed abandoned upload after its real five-minute lease").isTrue();
            assertThatThrownBy(() -> store.headObject(head))
                    .isInstanceOfSatisfying(software.amazon.awssdk.services.s3.model.S3Exception.class,
                            failure -> assertThat(failure.statusCode()).isEqualTo(404));
        }
    }

    private Abandoned killWriterBlockedBeforeCommit(Host host, ArchiveServiceGrpc.ArchiveServiceBlockingStub writer,
            PutEntryRequest original, PutEntryResponse saved) throws Exception {
        Abandoned abandoned = null;
        try (var lock = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lock.setAutoCommit(false);
            try {
                int blocker;
                try (var query = lock.createStatement(); var result = query.executeQuery("SELECT pg_backend_pid()")) {
                    assertThat(result.next()).isTrue();
                    blocker = result.getInt(1);
                }
                // Permit foreign-key checks, but stop commitSave's entry FOR UPDATE.
                try (var query = lock.prepareStatement("SELECT entry_uuid FROM archive_entries WHERE entry_uuid=? FOR NO KEY UPDATE")) {
                    query.setObject(1, UUID.fromString(saved.getEntryUuid()));
                    try (var result = query.executeQuery()) { assertThat(result.next()).isTrue(); }
                }
                var candidate = original.toBuilder().setExpectedVersion(saved.getVersion()).setRenditions(0,
                        original.getRenditions(0).toBuilder().setData(ByteString.copyFromUtf8("must never become visible"))).build();
                try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                    var pending = workers.submit(() -> writer.withDeadlineAfter(30, TimeUnit.SECONDS).putEntry(candidate));
                    boolean blocked = false;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                    try (var observer = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                            var query = observer.prepareStatement("""
                                    SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                                    WHERE ? = ANY(pg_blocking_pids(pid)) AND query ILIKE '%archive_entries%'
                                    AND query ILIKE '%for%update%')
                                    """)) {
                        query.setInt(1, blocker);
                        while (System.nanoTime() < deadline && !pending.isDone()) {
                            try (var result = query.executeQuery()) { result.next(); blocked = result.getBoolean(1); }
                            if (blocked) break;
                            Thread.sleep(25);
                        }
                        assertThat(blocked).as("writer observed waiting on the held entry lock").isTrue();
                        try (var staged = observer.prepareStatement("""
                                SELECT u.object_id,b.bucket,b.object_key FROM archive_object_uploads u
                                JOIN archive_object_bindings b USING(object_id)
                                WHERE b.entry_uuid=? AND u.state='VERIFIED' AND u.sha256=?
                                AND NOT EXISTS(SELECT 1 FROM archive_version_object_refs r WHERE r.object_id=u.object_id)
                                """)) {
                            staged.setObject(1, UUID.fromString(saved.getEntryUuid()));
                            staged.setString(2, java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                                    .digest(candidate.getRenditions(0).getData().toByteArray())));
                            try (var result = staged.executeQuery()) {
                                assertThat(result.next()).isTrue();
                                abandoned = new Abandoned(result.getObject(1, UUID.class), result.getString(2), result.getString(3));
                                assertThat(result.next()).as("exactly one verified unpublished candidate").isFalse();
                            }
                        }
                        host.close(); // No graceful hooks: the in-flight transaction loses its client process.
                        assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS))
                                .isInstanceOf(java.util.concurrent.ExecutionException.class)
                                .satisfies(failure -> assertThat(io.grpc.Status.fromThrowable(failure.getCause()).getCode())
                                        .isEqualTo(io.grpc.Status.Code.UNAVAILABLE));
                    } finally {
                        // Also terminate on failed observations before awaiting executor shutdown.
                        host.close();
                    }
                }
            } finally { lock.rollback(); }
        }
        return java.util.Objects.requireNonNull(abandoned);
    }

    private void assertStats(ArchiveServiceGrpc.ArchiveServiceBlockingStub stub, long entries, long versions) {
        var stats = stub.withDeadlineAfter(20, TimeUnit.SECONDS).getArchiveStats(GetArchiveStatsRequest.newBuilder()
                .setAccountId("process-account").setArchive("records").build()).getStats();
        assertThat(stats.getEntries()).isEqualTo(entries);
        assertThat(stats.getVersions()).isEqualTo(versions);
    }

    private void assertCompetingRevisions(ArchiveServiceGrpc.ArchiveServiceBlockingStub first,
            ArchiveServiceGrpc.ArchiveServiceBlockingStub second, PutEntryRequest initial) throws Exception {
        var baseline = first.withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(initial);
        var left = initial.toBuilder().setExpectedVersion(baseline.getVersion()).setRenditions(0,
                initial.getRenditions(0).toBuilder().setData(ByteString.copyFromUtf8("left contender"))).build();
        var right = initial.toBuilder().setExpectedVersion(baseline.getVersion()).setRenditions(0,
                initial.getRenditions(0).toBuilder().setData(ByteString.copyFromUtf8("right contender"))).build();
        var start = new java.util.concurrent.CyclicBarrier(2);
        try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var a = workers.submit(() -> contend(first, left, start));
            var b = workers.submit(() -> contend(second, right, start));
            var resultA = a.get(30, TimeUnit.SECONDS);
            var resultB = b.get(30, TimeUnit.SECONDS);
            assertThat(java.util.List.of(resultA.code(), resultB.code()))
                    .containsExactlyInAnyOrder(io.grpc.Status.Code.OK, io.grpc.Status.Code.ABORTED);
            var winner = resultA.code() == io.grpc.Status.Code.OK ? resultA : resultB;
            assertThat(winner.version()).isEqualTo(baseline.getVersion() + 1);
            for (var reader : java.util.List.of(first, second)) {
                var current = reader.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(
                        GetEntryRequest.newBuilder().setAddress(initial.getAddress()).build());
                assertThat(current.getRenditions(0).getData()).isEqualTo(winner.payload());
                var historical = reader.withDeadlineAfter(20, TimeUnit.SECONDS).getEntry(
                        GetEntryRequest.newBuilder().setAddress(initial.getAddress()).setVersion(baseline.getVersion()).build());
                assertThat(historical.getRenditions(0).getData()).isEqualTo(initial.getRenditions(0).getData());
            }
        }
    }

    private record Contender(io.grpc.Status.Code code, long version, ByteString payload) {}

    private Contender contend(ArchiveServiceGrpc.ArchiveServiceBlockingStub stub, PutEntryRequest request,
            java.util.concurrent.CyclicBarrier start) throws Exception {
        start.await(10, TimeUnit.SECONDS);
        try {
            var response = stub.withDeadlineAfter(20, TimeUnit.SECONDS).putEntry(request);
            return new Contender(io.grpc.Status.Code.OK, response.getVersion(), request.getRenditions(0).getData());
        } catch (io.grpc.StatusRuntimeException failure) {
            if (failure.getStatus().getCode() != io.grpc.Status.Code.ABORTED) throw failure;
            return new Contender(failure.getStatus().getCode(), 0, ByteString.EMPTY);
        }
    }

    private Host launch(String name, String token) throws Exception {
        Path ready = directory.resolve(name + ".port");
        Path log = directory.resolve(name + ".log");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("protomolt.test.runtimeClasspath"),
                ReplicaHostProcess.class.getName(), ready.toString()).redirectErrorStream(true).redirectOutput(log.toFile());
        var env = builder.environment();
        env.put("TEST_JDBC", POSTGRES.getJdbcUrl()); env.put("TEST_DB_USER", POSTGRES.getUsername());
        env.put("TEST_DB_PASSWORD", POSTGRES.getPassword()); env.put("TEST_S3_ENDPOINT", S3.getEndpoint().toString());
        env.put("TEST_S3_REGION", S3.getRegion()); env.put("TEST_S3_KEY", S3.getAccessKey());
        env.put("TEST_S3_SECRET", S3.getSecretKey()); env.put("TEST_API_TOKEN", token);
        var process = builder.start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (process.isAlive() && System.nanoTime() < deadline) {
                if (Files.exists(ready)) {
                    String value = Files.readString(ready);
                    if (!value.isBlank()) return new Host(process, Integer.parseInt(value));
                }
                Thread.sleep(50);
            }
            throw new AssertionError("Replica failed to become ready: " + Files.readString(log));
        } catch (Throwable failure) {
            try { new Host(process, 0).close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private record Host(Process process, int port) implements AutoCloseable {
        @Override public void close() throws Exception {
            if (process.isAlive()) process.destroyForcibly();
            if (!process.waitFor(10, TimeUnit.SECONDS)) throw new IllegalStateException("Replica process did not exit");
        }
    }
}

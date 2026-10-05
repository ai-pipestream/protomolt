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
                first.close(); // Forceful exit: no shutdown hooks or shared JVM objects can help the survivor.
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
            } finally {
                a.shutdownNow();
                if (b != null) b.shutdownNow();
                a.awaitTermination(10, TimeUnit.SECONDS);
                if (b != null) b.awaitTermination(10, TimeUnit.SECONDS);
            }
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

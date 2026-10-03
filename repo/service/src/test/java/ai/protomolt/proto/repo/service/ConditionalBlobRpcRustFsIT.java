package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.blob.grpc.RemoteBlobStore;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Proves conditional RPC status and adapter behavior against the pinned deployment store. */
@Testcontainers(disabledWithoutDocker = true)
class ConditionalBlobRpcRustFsIT {
    @Container static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> RUSTFS = new GenericContainer<>(
            DockerImageName.parse("rustfs/rustfs:1.0.0-beta.11-preview.1"))
            .withCommand("/data").withEnv("RUSTFS_VOLUMES", "/data")
            .withEnv("RUSTFS_ADDRESS", ":9000")
            .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
            .withEnv("RUSTFS_ACCESS_KEY", "conditional-test")
            .withEnv("RUSTFS_SECRET_KEY", "conditional-test-secret")
            .withExposedPorts(9000).waitingFor(Wait.forHttp("/health").forPort(9000));

    @Test void staleEtagsAbortAcrossGrpcAndRemoteAdapter() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                "http://" + RUSTFS.getHost() + ":" + RUSTFS.getMappedPort(9000),
                "us-east-1", "conditional-test", "conditional-test-secret",
                "conditional-rpc-it", 0, null, null, null, null, 0, 0L, true);
        try (var services = RepoServices.build(config)) {
            services.startInProcess("conditional-rustfs-it");
            var channel = InProcessChannelBuilder.forName("conditional-rustfs-it").build();
            try {
                DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                        .setName("state").setAccountId("acct-conditional-rpc").build());
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                var key = ConditionalBlobKey.newBuilder().setDriveName("state")
                        .setObjectKey("transcript/current").build();
                assertThatThrownBy(() -> documents.getBlobForUpdate(
                        GetBlobForUpdateRequest.newBuilder().setKey(key).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                                assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
                var first = documents.compareAndPutBlob(CompareAndPutBlobRequest.newBuilder()
                        .setKey(key).setIfAbsent(true).setData(ByteString.copyFromUtf8("one")).build());
                var observed = documents.getBlobForUpdate(GetBlobForUpdateRequest.newBuilder()
                        .setKey(key).build());
                assertThat(observed.getVersion().getEtag()).isEqualTo(first.getVersion().getEtag());
                var next = documents.compareAndPutBlob(CompareAndPutBlobRequest.newBuilder()
                        .setKey(key).setExpectedEtag(observed.getVersion().getEtag())
                        .setData(ByteString.copyFromUtf8("two")).build());
                assertThat(next.getVersion().getEtag()).isNotEqualTo(first.getVersion().getEtag());
                assertThatThrownBy(() -> documents.compareAndPutBlob(CompareAndPutBlobRequest.newBuilder()
                        .setKey(key).setExpectedEtag(first.getVersion().getEtag())
                        .setData(ByteString.copyFromUtf8("late")).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, error ->
                                assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));

                var remote = new RemoteBlobStore(documents, "state");
                var spec = new BlobStore.PutSpec("state", key.getObjectKey(),
                        "application/octet-stream", null, null);
                assertThatThrownBy(() -> remote.conditionalPut(spec, "late".getBytes(),
                        BlobStore.WriteCondition.matching(first.getVersion().getEtag())))
                        .isInstanceOf(BlobStore.BlobConflictException.class);
                assertThat(remote.getForUpdate("state", key.getObjectKey()).data())
                        .isEqualTo("two".getBytes());

                var largeKey = ConditionalBlobKey.newBuilder().setDriveName("state")
                        .setObjectKey("transcript/full-size").build();
                byte[] large = new byte[9 * 1024 * 1024];
                java.util.Arrays.fill(large, (byte) 7);
                var wide = documents.withMaxInboundMessageSize(10 * 1024 * 1024);
                var largeWritten = wide.compareAndPutBlob(CompareAndPutBlobRequest.newBuilder()
                        .setKey(largeKey).setIfAbsent(true).setData(ByteString.copyFrom(large)).build());
                var largeRead = wide.getBlobForUpdate(GetBlobForUpdateRequest.newBuilder()
                        .setKey(largeKey).build());
                assertThat(largeRead.getData().size()).isEqualTo(large.length);
                assertThat(largeRead.getVersion().getEtag())
                        .isEqualTo(largeWritten.getVersion().getEtag());
            } finally {
                channel.shutdownNow();
            }
        }
    }
}

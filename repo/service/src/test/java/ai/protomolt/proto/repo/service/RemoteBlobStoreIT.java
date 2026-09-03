package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.client.RemoteBlobStore;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test of the dogfood {@link RemoteBlobStore}: the
 * {@link BlobStore} port served by a repo-service's own blob RPCs, here over
 * the in-process transport against the fully wired stack (testcontainers
 * PostgreSQL + LocalStack S3). No mocks: every operation crosses the gRPC
 * boundary and lands in real object storage.
 */
@Testcontainers(disabledWithoutDocker = true)
class RemoteBlobStoreIT {

    private static final String DRIVE = "remote";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static RemoteBlobStore store;

    @BeforeAll
    static void boot() {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-remote-docs",
                0, null, null, null, null, 0, 0L);
        services = RepoServices.build(config);
        services.startInProcess("it-remote");
        channel = InProcessChannelBuilder.forName("it-remote").build();
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                .setName(DRIVE)
                .setAccountId("acct-remote")
                .build());
        store = new RemoteBlobStore(DocumentServiceGrpc.newBlockingStub(channel), DRIVE);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test
    void putGetHeadDeleteRoundTrip() {
        byte[] data = "remote-payload".getBytes(StandardCharsets.UTF_8);
        // The bucket coordinate is ignored by the repo-backed store: the
        // drive resolves the real bucket server-side.
        store.put(new BlobStore.PutSpec("ignored-bucket", "rt/one.bin", "text/plain", null, null),
                data);

        BlobStore.GetResult got = store.get("ignored-bucket", "rt/one.bin");
        assertThat(got.data()).isEqualTo(data);
        assertThat(got.contentType()).isEqualTo("text/plain");

        // headObject is a full fetch whose bytes are discarded (documented
        // gap: the v1 API has no cheap existence probe).
        store.headObject("ignored-bucket", "rt/one.bin");

        assertThat(store.delete("ignored-bucket", "rt/one.bin")).isTrue();
        assertThatThrownBy(() -> store.get("ignored-bucket", "rt/one.bin"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        // Idempotent re-delete.
        assertThat(store.delete("ignored-bucket", "rt/one.bin")).isFalse();
    }

    @Test
    void streamingPutVariantReadsTheStream() {
        byte[] data = "streamed-via-port".getBytes(StandardCharsets.UTF_8);
        store.put(new BlobStore.PutSpec("ignored-bucket", "rt/streamed.bin",
                        "application/octet-stream", null, null),
                new ByteArrayInputStream(data), data.length);
        assertThat(store.get("ignored-bucket", "rt/streamed.bin").data()).isEqualTo(data);
        store.delete("ignored-bucket", "rt/streamed.bin");
    }

    @Test
    void copyIsAClientSideGetPlusPut() {
        byte[] data = "copy-source".getBytes(StandardCharsets.UTF_8);
        store.put(new BlobStore.PutSpec("b", "cp/src.bin", "text/plain", null, null), data);
        store.copy("b", "cp/src.bin", "b", "cp/dst.bin");
        assertThat(store.get("b", "cp/dst.bin").data()).isEqualTo(data);
        store.delete("b", "cp/src.bin");
        store.delete("b", "cp/dst.bin");
    }

    @Test
    void absentGetAndHeadMapToBlobNotFound() {
        assertThatThrownBy(() -> store.get("ignored-bucket", "never/existed.bin"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThatThrownBy(() -> store.headObject("ignored-bucket", "never/existed.bin"))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    @Test
    void enumerateAndAdminOpsAreUnsupported() {
        assertThatThrownBy(() -> store.list("bucket", "prefix"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("repo-backed store");
        assertThatThrownBy(() -> store.deleteAll("bucket", List.of("a", "b")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("repo-backed store");
        assertThatThrownBy(() -> store.headBucket("bucket"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("repo-backed store");
    }
}

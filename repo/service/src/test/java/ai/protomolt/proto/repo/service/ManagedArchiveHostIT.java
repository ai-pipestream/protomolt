package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Actual host assembly, authentication, recovery startup and restart persistence. */
@Testcontainers
class ManagedArchiveHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Test void configuredHostMountsManagedWritesAndResumesCleanupAfterRestart() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(),
                "managed-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("host-original", "host-realm", true));
        String token = UUID.randomUUID().toString();
        String operation = UUID.randomUUID().toString();
        var address = EntryAddress.newBuilder().setAccountId("host-account").setArchive("records").setEntryId("entry").build();
        try (var host = RepoServices.build(config)) {
            var server = host.startNetty(0, token, null); // No explicit startLifecycle call.
            var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
            var metadata = new Metadata();
            metadata.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
            var credential = MetadataUtils.newAttachHeadersInterceptor(metadata);
            try {
                DriveServiceGrpc.newBlockingStub(channel).withInterceptors(credential).createDrive(
                        CreateDriveRequest.newBuilder().setAccountId("host-account").setName("storage").build());
                var archive = ArchiveServiceGrpc.newBlockingStub(channel).withInterceptors(credential);
                archive.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId("host-account")
                        .setName("records").setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var saved = archive.putEntry(PutEntryRequest.newBuilder().setAddress(address)
                        .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                                .setData(ByteString.copyFromUtf8("restart-safe content"))).build());
                assertThat(saved.getManifest().getRenditions(0).getStorageObjectId()).isNotBlank();
                var command = ArchiveMutationRequest.newBuilder().setOperationId(operation)
                        .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(address)).build();
                var receipt = ArchiveMutationServiceGrpc.newBlockingStub(channel).withInterceptors(credential).archiveMutation(command).getReceipt();
                assertThat(receipt.getEntryDeleted()).isTrue();
                assertThat(receipt.getObjectsTargeted()).isEqualTo(1);
                assertThat(receipt.getObjectsPending()).isEqualTo(1);
                assertThatThrownBy(() -> ArchiveMutationServiceGrpc.newBlockingStub(channel).getArchiveMutation(
                        GetArchiveMutationRequest.newBuilder().setAccountId("host-account").setOperationId(operation).build()))
                        .satisfies(failure -> assertThat(io.grpc.Status.fromThrowable(failure).getCode()).isEqualTo(io.grpc.Status.Code.UNAUTHENTICATED));
            } finally { channel.shutdownNow().awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS); }
        }
        try (var resumed = RepoServices.build(config)) {
            resumed.startInProcess("managed-resume-" + UUID.randomUUID()); // Starts recovery before serving.
            var caller = new RepositoryCaller(ai.protomolt.proto.actions.Caller.operator().name(), true);
            var lookup = GetArchiveMutationRequest.newBuilder().setAccountId("host-account").setOperationId(operation).build();
            var operations = resumed.archiveMutationRepository();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
            ArchiveMutationReceipt observed;
            do {
                observed = operations.getArchiveMutation(caller, lookup);
                if (observed.getState() == ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED) break;
                Thread.sleep(100);
            } while (System.nanoTime() < deadline);
            assertThat(observed.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
            assertThat(observed.getObjectsConfirmedAbsent()).isEqualTo(1);
            assertThat(observed.getStatusRevision()).isGreaterThan(1);
        }
    }

    @Test void failedListenerStartupClosesTheManagedComposition() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(),
                "managed-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("host-original", "host-realm", true));
        try (var occupied = new java.net.ServerSocket(0); var host = RepoServices.build(config)) {
            var failure = catchThrowable(() -> host.startNetty(occupied.getLocalPort()));
            assertThat(failure).isInstanceOf(java.io.UncheckedIOException.class);
            assertThat(failure.getCause().getSuppressed()).isEmpty();
            assertThatThrownBy(host::services).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
            assertThatThrownBy(host::archiveMutationRepository).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        }
    }
}

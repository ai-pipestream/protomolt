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
            var failure = catchThrowable(() -> host.startNetty(occupied.getLocalPort(), "synthetic-operator-key", null));
            assertThat(failure).isInstanceOf(java.io.UncheckedIOException.class);
            assertThat(failure.getCause().getSuppressed()).isEmpty();
            assertThatThrownBy(host::services).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
            assertThatThrownBy(host::archiveMutationRepository).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        }
    }

    /** Two independently assembled hosts in one JVM; not a process-failure or throughput benchmark. */
    @Test void independentHostsShareConcurrentWritesReadsAndRetainedVersions() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(),
                "replica-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("replica-original", "replica-realm", true));
        String account = "replica-" + UUID.randomUUID();
        String token = UUID.randomUUID().toString();
        try (var first = RepoServices.build(config); var second = RepoServices.build(config)) {
            var left = first.startNetty(0, token, null);
            var right = second.startNetty(0, token, null);
            var a = NettyChannelBuilder.forAddress("127.0.0.1", left.getPort()).usePlaintext().build();
            io.grpc.ManagedChannel b = null;
            try {
                b = NettyChannelBuilder.forAddress("127.0.0.1", right.getPort()).usePlaintext().build();
                var headers = new Metadata();
                headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                var credential = MetadataUtils.newAttachHeadersInterceptor(headers);
                DriveServiceGrpc.newBlockingStub(a).withInterceptors(credential)
                        .withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).createDrive(
                                CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
                var stubA = ArchiveServiceGrpc.newBlockingStub(a).withInterceptors(credential);
                var stubB = ArchiveServiceGrpc.newBlockingStub(b).withInterceptors(credential);
                stubB.withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).createArchive(
                        CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId(account)
                                .setName("records").setDriveName("storage")
                                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                    var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
                    for (int i = 0; i < 12; i++) {
                        final int index = i;
                        tasks.add(workers.submit(() -> {
                            var writer = index % 2 == 0 ? stubA : stubB;
                            var reader = index % 2 == 0 ? stubB : stubA;
                            var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records")
                                    .setEntryId("entry-" + index).build();
                            var bytes = ByteString.copyFromUtf8("replica payload " + index);
                            var request = PutEntryRequest.newBuilder().setAddress(address).addRenditions(
                                    RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder()
                                            .setName("original")).setData(bytes)).build();
                            var saved = writer.withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).putEntry(request);
                            var replay = reader.withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).putEntry(request);
                            assertThat(replay.getVersion()).isEqualTo(saved.getVersion());
                            assertThat(replay.getEntryUuid()).isEqualTo(saved.getEntryUuid());
                            assertThat(replay.getManifest().getRenditions(0).getStorageObjectId())
                                    .isEqualTo(saved.getManifest().getRenditions(0).getStorageObjectId());
                            var updated = reader.withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).putEntry(
                                    request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                                            .setData(ByteString.copyFromUtf8("updated " + index))).build());
                            assertThat(updated.getVersion()).isEqualTo(saved.getVersion() + 1);
                            var history = writer.withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).getEntry(
                                    GetEntryRequest.newBuilder().setAddress(address).setVersion(saved.getVersion()).build());
                            assertThat(history.getRenditions(0).getData()).isEqualTo(bytes);
                        }));
                    }
                    for (var task : tasks) task.get(60, java.util.concurrent.TimeUnit.SECONDS);
                }
                var stats = stubB.withDeadlineAfter(20, java.util.concurrent.TimeUnit.SECONDS).getArchiveStats(
                        GetArchiveStatsRequest.newBuilder().setAccountId(account).setArchive("records").build()).getStats();
                assertThat(stats.getEntries()).isEqualTo(12);
                assertThat(stats.getVersions()).isEqualTo(24);
            } finally {
                a.shutdownNow();
                if (b != null) b.shutdownNow();
                a.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
                if (b != null) b.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
    }
}

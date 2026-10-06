package ai.protomolt.proto.repo.service.consumer;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.service.BoundedArchiveOptions;
import ai.protomolt.proto.repo.service.ManagedStoragePolicy;
import ai.protomolt.proto.repo.service.RepoServiceConfig;
import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** An embedding application outside the service package using only public composition methods. */
@Testcontainers
class BoundedArchiveEmbeddingIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test void publicFactorySupportsLocalAndAuthenticatedRemoteArchiveUse() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                null, null, null, null, "embedded-archive", 0, "redis", null, null,
                "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), 0, 1024 * 1024)
                .withManagedStorage(new ManagedStoragePolicy("embedded-redis", "embedded-realm", true));
        var limits = new BoundedArchiveOptions(1024 * 1024, 2 * 1024 * 1024, 16, 64L * 1024 * 1024, 4);
        var caller = new RepositoryCaller("embedding-host", true);
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records").setEntryId("entry").build();
        var put = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setData(ByteString.copyFromUtf8("embedded content"))).build();
        try (var host = RepoServices.buildBoundedArchive(config, limits)) {
            host.driveRepository().createDrive(caller, CreateDriveRequest.newBuilder().setAccountId("account").setName("storage").build());
            host.archiveRepository().createArchive(caller, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                    .setAccountId("account").setName("records").setDriveName("storage")
                    .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            assertThat(host.archiveRepository().putEntry(caller, put).getVersion()).isEqualTo(1);
            String token = UUID.randomUUID().toString();
            var server = host.startBoundedArchiveNetty(0, token);
            var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
            try {
                var metadata = new Metadata();
                metadata.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                var remote = ArchiveServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
                assertThat(remote.putEntry(put).getVersion()).isEqualTo(1);
                assertThat(remote.getEntry(GetEntryRequest.newBuilder().setAddress(address).setVersion(1).build())
                        .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("embedded content"));
            } finally {
                channel.shutdownNow(); assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
            assertThatThrownBy(host::services).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> host.startHttp(0, "synthetic-operator-key")).isInstanceOf(UnsupportedOperationException.class);
        }
        try (var restarted = RepoServices.buildBoundedArchive(config, limits)) {
            assertThat(restarted.archiveRepository().getEntry(caller,
                    GetEntryRequest.newBuilder().setAddress(address).setVersion(1).build()).getRenditions(0).getData())
                    .isEqualTo(ByteString.copyFromUtf8("embedded content"));
        }
    }
}

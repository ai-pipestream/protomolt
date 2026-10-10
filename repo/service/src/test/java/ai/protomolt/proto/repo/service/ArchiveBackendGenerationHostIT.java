package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.engine.UnservedBackendGenerationException;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The production host over real PostgreSQL and the pinned RustFS image, composed a second
 * time under a generation the ledger has never bound. Every recorded archive object stays on
 * the provider under the first generation; the second host does not serve that generation, so
 * a bound read is a host configuration precondition failure and must reach library and
 * in-process gRPC callers as the same repository code and description.
 */
@Testcontainers
class ArchiveBackendGenerationHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final RustFsBenchmarkStore STORAGE = new RustFsBenchmarkStore();
    private static final Metadata.Key<String> TOKEN = Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);
    private static final String OPERATOR_KEY = "synthetic-host-operator-key";
    private static final String BACKEND_NOT_CONFIGURED = "Original archive backend is not configured on this host";
    private static final RepositoryCaller OPERATOR = new RepositoryCaller("operator", true);
    private static final RepositoryCaller MEMBER = new RepositoryCaller("member", false, Set.of("archive-account"), Set.of());

    @Test void hostUnderAnotherGenerationRefusesBoundReadsAsPreconditionForLibraryAndTransport() throws Exception {
        String recorded = "archive-generation-" + UUID.randomUUID();
        String other = "archive-generation-" + UUID.randomUUID();
        String account = "archive-account";
        var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records").setEntryId("entry-" + UUID.randomUUID()).build();
        var payload = ByteString.copyFromUtf8("archive generation fixture ".repeat(96));
        List<String> secrets = List.of(STORAGE.getEndpoint().toString(), STORAGE.getSecretKey(), STORAGE.getAccessKey(), recorded, other);

        try (var host = RepoServices.build(config(recorded)); var transport = new Transport(host)) {
            var operator = transport.archive(OPERATOR_KEY);
            transport.drives(OPERATOR_KEY).createDrive(CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
            operator.createArchive(CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder().setAccountId(account)
                    .setName("records").setDriveName("storage").setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
            var saved = operator.putEntry(PutEntryRequest.newBuilder().setAddress(address)
                    .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(payload)).build());
            assertThat(saved.getManifest().getRenditions(0).getStorageObjectId()).as("bound to the recorded generation").isNotBlank();
            var library = host.archiveRepository().getEntry(OPERATOR, GetEntryRequest.newBuilder().setAddress(address).build());
            assertThat(library.getRenditions(0).getData()).isEqualTo(payload);
            assertThat(operator.getEntry(GetEntryRequest.newBuilder().setAddress(address).build()).getRenditions(0).getData()).isEqualTo(payload);
        }
        assertThat(pins()).isZero();

        try (var host = RepoServices.build(config(other)); var transport = new Transport(host)) {
            var manifest = host.archiveRepository().getManifest(OPERATOR, GetEntryManifestRequest.newBuilder().setAddress(address).build()).getManifest();
            assertThat(manifest.getRenditions(0).getStorageObjectId()).isNotBlank();
            assertThat(transport.archive(OPERATOR_KEY).getEntryManifest(GetEntryManifestRequest.newBuilder().setAddress(address).build()).getManifest())
                    .as("catalog-only reads need no provider generation").isEqualTo(manifest);

            // Authorization precedes resolver and provider access: a caller without process authority never reaches the binding.
            assertThatThrownBy(() -> host.archiveRepository().getEntry(MEMBER, GetEntryRequest.newBuilder().setAddress(address).build()))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED);
                        assertThat(failure.getMessage()).isEqualTo("Repository account policy is not configured for this principal");
                    });
            assertThatThrownBy(() -> transport.archive("member-key").getEntry(GetEntryRequest.newBuilder().setAddress(address).build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));

            for (var request : List.of(GetEntryRequest.newBuilder().setAddress(address).build(),
                    GetEntryRequest.newBuilder().setAddress(address).setVersion(1).addRenditions("original").build())) {
                var raised = catchThrowable(() -> host.archiveRepository().getEntry(OPERATOR, request));
                if (!(raised instanceof RepositoryException)) System.out.println("ARCHIVE_READ_LEAK library -> " + chain(raised));
                assertThat(raised).as("library read under an unserved generation").isInstanceOf(RepositoryException.class);
                var failure = (RepositoryException) raised;
                assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                assertThat(failure.getMessage()).isEqualTo(BACKEND_NOT_CONFIGURED);
                for (var secret : secrets) assertThat(failure.getMessage()).doesNotContain(secret);
                assertThat(failure.getCause()).as("the resolver's typed refusal stays on the cause chain")
                        .isInstanceOf(UnservedBackendGenerationException.class);

                var status = catchThrowable(() -> transport.archive(OPERATOR_KEY).getEntry(request));
                assertThat(status).as("gRPC read under an unserved generation").isInstanceOf(StatusRuntimeException.class);
                var observed = ((StatusRuntimeException) status).getStatus();
                assertThat(observed.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                assertThat(observed.getDescription()).isEqualTo(BACKEND_NOT_CONFIGURED);
            }
            assertThat(pins()).as("failed reads release their pins").isZero();
        }
        assertThat(pins()).isZero();
    }

    private static RepoServiceConfig config(String generation) {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                STORAGE.getEndpoint().toString(), STORAGE.getRegion(), STORAGE.getAccessKey(), STORAGE.getSecretKey(), "archive-generation", 0,
                null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy(generation, "archive-generation-realm", true));
    }

    /** Authenticated in-process transport of the host; the operator key carries process authority, the member key does not. */
    private static final class Transport implements AutoCloseable {
        final ManagedChannel channel;
        Transport(RepoServices host) {
            String name = "archive-generation-host-" + UUID.randomUUID();
            CallerResolver credentials = token -> "member-key".equals(token) ? Optional.of(Caller.scoped("member", Set.of())) : Optional.empty();
            host.startInProcess(name, OPERATOR_KEY, credentials);
            channel = InProcessChannelBuilder.forName(name).maxInboundMessageSize(8 * 1024 * 1024).build();
        }
        ArchiveServiceGrpc.ArchiveServiceBlockingStub archive(String token) {
            var headers = new Metadata(); headers.put(TOKEN, token);
            return ArchiveServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        DriveServiceGrpc.DriveServiceBlockingStub drives(String token) {
            var headers = new Metadata(); headers.put(TOKEN, token);
            return DriveServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
        }
        @Override public void close() throws Exception {
            channel.shutdownNow();
            assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static String chain(Throwable failure) {
        var text = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            text.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append(" <- ");
        return text.toString();
    }

    private static long pins() throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT count(*) FROM archive_read_pins");
                var result = query.executeQuery()) {
            result.next();
            return result.getLong(1);
        }
    }
}

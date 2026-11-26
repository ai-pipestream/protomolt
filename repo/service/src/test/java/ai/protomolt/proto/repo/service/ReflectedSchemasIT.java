package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.SaveRejection;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Types read over gRPC server reflection through the served repository: the production
 * {@link RepoServices} composition over real PostgreSQL and LocalStack S3, saving through
 * {@code DocumentService.SaveDocument} a PARSED part whose {@code document.shape} is a
 * message nothing in this build admits. The producing service is a real Netty gRPC server
 * that serves the reflection protocol; the message is {@code grpc.health.v1.HealthCheckResponse},
 * which that server defines because it mounts the health service, and which the repository
 * contract and its default admitted schemas do not.
 */
@Testcontainers
class ReflectedSchemasIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final String DATASOURCE = "ds-reflected";
    private static final String PARSER = "probe";
    private static final String SHAPE_URL = "type.googleapis.com/grpc.health.v1.HealthCheckResponse";
    private static final HealthCheckResponse SHAPE = HealthCheckResponse.newBuilder()
            .setStatus(HealthCheckResponse.ServingStatus.SERVING).build();

    @Test
    void aTypeAConfiguredServerDefinesIsAdmittedTypedAndReadBackByteForByte() throws Exception {
        String account = "reflected-admitted";
        Server producer = producer(0, true);
        try (var services = RepoServices.build(config())) {
            services.withReflectedSchemas(sources(producer.getPort()));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);

                var saved = documents.saveDocument(save(account, "typed"));
                assertThat(saved.getAccession().getRevision()).isPositive();

                var read = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(address(account, "typed"))
                        .setSelection(PartSelection.newBuilder().addSelectors(
                                PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)))
                        .build());
                Any shape = read.getDocument().getParserResultsOrThrow(PARSER).getDocument().getShape();
                assertThat(shape.getTypeUrl()).isEqualTo(SHAPE_URL);
                assertThat(shape.getValue()).isEqualTo(SHAPE.toByteString());
                assertThat(shape.unpack(HealthCheckResponse.class)).isEqualTo(SHAPE);
                // The save was validated typed against the reflected definition, not accepted opaque.
                assertThat(read.getValidation().getStatus()).isEqualTo(ValidationStatus.VALIDATION_STATUS_TYPED_VALID);
                assertThat(read.getValidation().getOpaqueOccurrencesList()).isEmpty();
                assertThat(read.getValidation().getResolvedOccurrencesList())
                        .anySatisfy(occurrence -> {
                            assertThat(occurrence.getSchema().getTypeUrl()).isEqualTo(SHAPE_URL);
                            assertThat(occurrence.getSchema().getDescriptorSha256()).hasSize(64);
                            assertThat(occurrence.getPayloadLength()).isEqualTo(SHAPE.getSerializedSize());
                        });
            } finally {
                channel.shutdownNow();
            }
        } finally {
            producer.shutdownNow();
        }
    }

    @Test
    void theRevisionKeepsItsSchemaAfterTheServerIsGone() throws Exception {
        String account = "reflected-retained";
        Server producer = producer(0, true);
        try (var services = RepoServices.build(config())) {
            services.withReflectedSchemas(sources(producer.getPort()));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);
                documents.saveDocument(save(account, "kept"));
                producer.shutdownNow().awaitTermination();

                // A typed read decodes with the schema retained at save time; it asks nobody.
                var read = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(address(account, "kept")).build());
                assertThat(read.getDocument().getParserResultsOrThrow(PARSER).getDocument().getShape().getValue())
                        .isEqualTo(SHAPE.toByteString());
                // A new save of the type cannot be validated while its source is down.
                assertCode(() -> documents.saveDocument(save(account, "while-down")), Status.Code.UNAVAILABLE);
            } finally {
                channel.shutdownNow();
            }
        } finally {
            producer.shutdownNow();
        }
    }

    @Test
    void withoutASourceTheTypeIsMissing() {
        String account = "reflected-unconfigured";
        try (var services = RepoServices.build(config())) {
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);
                assertThat(rejection(() -> documents.saveDocument(save(account, "untyped"))).getReason())
                        .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void aServerThatDoesNotKnowTheTypeLeavesItMissing() throws Exception {
        String account = "reflected-unknown";
        // Serves reflection, mounts no health service: it does not define the shape.
        Server producer = producer(0, false);
        try (var services = RepoServices.build(config())) {
            services.withReflectedSchemas(sources(producer.getPort()));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);
                assertThat(rejection(() -> documents.saveDocument(save(account, "unknown"))).getReason())
                        .isEqualTo(RejectionReason.REJECTION_REASON_SCHEMA_MISSING);
            } finally {
                channel.shutdownNow();
            }
        } finally {
            producer.shutdownNow();
        }
    }

    @Test
    void aServerThatCannotBeAskedIsAnOutageNotAMissingSchemaAndTheSaveCommitsOnceItAnswers() throws Exception {
        String account = "reflected-outage";
        int port = freePort();
        Server producer = null;
        try (var services = RepoServices.build(config())) {
            services.withReflectedSchemas(sources(port));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);
                var request = save(account, "late");

                // Nothing listens: UNAVAILABLE, and no rejection is recorded for the operation.
                var outage = (StatusRuntimeException) catchThrowable(() -> documents.saveDocument(request));
                assertThat(outage).isNotNull();
                assertThat(outage.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                assertThat(StatusProto.fromThrowable(outage).getDetailsList())
                        .noneMatch(detail -> detail.is(SaveRejection.class));

                // The same operation commits once the server answers. No channel backoff
                // stands in the way: each refused ask redials, so a retry of the save is
                // all it takes, and until the dial lands the answer stays UNAVAILABLE.
                producer = producer(port, true);
                SaveDocumentResponse saved = null;
                long giveUp = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (saved == null) {
                    try {
                        saved = documents.saveDocument(request);
                    } catch (StatusRuntimeException pending) {
                        assertThat(pending.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                        assertThat(System.nanoTime()).isLessThan(giveUp);
                        Thread.sleep(20);
                    }
                }
                assertThat(saved.getAccession().getRevision()).isPositive();
                assertThat(saved.getReplayed()).isFalse();
            } finally {
                channel.shutdownNow();
            }
        } finally {
            if (producer != null) producer.shutdownNow();
        }
    }

    @Test
    void aTypeAModuleContributedIsNeverAskedOfTheSource() throws Exception {
        String account = "reflected-contributed";
        // The source is down, so a lookup would fail the save UNAVAILABLE.
        int port = freePort();
        try (var services = RepoServices.build(config())) {
            services.withAdmittedSchemas(List.of(HealthCheckResponse.getDescriptor().getFile()));
            services.withReflectedSchemas(sources(port));
            var channel = start(services);
            try {
                var documents = DocumentServiceGrpc.newBlockingStub(channel);
                intakeDrive(channel, account);
                assertThat(documents.saveDocument(save(account, "contributed")).getAccession().getRevision())
                        .isPositive();
            } finally {
                channel.shutdownNow();
            }
        }
    }

    /** A Netty gRPC server that serves reflection, with or without the health service's types. */
    private static Server producer(int port, boolean definesTheShape) throws IOException {
        var builder = NettyServerBuilder.forPort(port).addService(ProtoReflectionServiceV1.newInstance());
        if (definesTheShape) {
            builder.addService(new HealthStatusManager().getHealthService());
        }
        return builder.build().start();
    }

    private static ReflectedSchemas sources(int port) {
        return ReflectedSchemas.open(List.of("localhost:" + port), false, 5_000, ChannelFactory.standard());
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static RepoServiceConfig config() {
        return new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-reflected",
                0,
                null, null, 0, 0L,
                false, 0L, 0L, false, true, 0L,
                null, null, null, null, null, null,
                false, ManagedStoragePolicy.disabled(),
                0L);
    }

    private static ManagedChannel start(RepoServices services) {
        String name = "repo-reflected-" + UUID.randomUUID();
        services.startInProcess(name);
        return InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    private static void intakeDrive(ManagedChannel channel, String account) {
        DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder().setName("intake")
                .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
    }

    private static NodeAddress address(String account, String doc) {
        return NodeAddress.newBuilder().setAccountId(account).setDocId(doc)
                .setGraphId("intake:" + account).setGraphAddressId(DATASOURCE).build();
    }

    /** A new document whose PARSED part holds one parser result shaped as the producer's message. */
    private static SaveDocumentRequest save(String account, String doc) {
        var document = Document.newBuilder().setDocId(doc)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(account).setDatasourceId(DATASOURCE)
                        .setSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc))
                .putParserResults(PARSER, ParserResult.newBuilder()
                        .setParserName(PARSER)
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .setDocument(ParserDocument.newBuilder().setShape(Any.pack(SHAPE)))
                        .build())
                .build();
        return SaveDocumentRequest.newBuilder().setOperationId("op-" + account + "-" + doc)
                .setTarget(DocumentTarget.newBuilder().setAddress(address(account, doc)))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)))
                .build();
    }

    private static PartWrite put(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part))
                .setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    private static SaveRejection rejection(Runnable call) {
        var failure = (StatusRuntimeException) catchThrowable(call::run);
        assertThat(failure).as("the save was expected to be refused").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        for (var detail : StatusProto.fromThrowable(failure).getDetailsList()) {
            if (!detail.is(SaveRejection.class)) continue;
            try {
                return detail.unpack(SaveRejection.class);
            } catch (InvalidProtocolBufferException malformed) {
                throw new AssertionError("an unreadable SaveRejection detail", malformed);
            }
        }
        throw new AssertionError("the refusal carries no SaveRejection: " + failure.getStatus());
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, Status.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
    }
}

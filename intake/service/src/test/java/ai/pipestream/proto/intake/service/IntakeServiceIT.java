package ai.pipestream.proto.intake.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.intake.v1.RawPayload;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.service.RepoServices;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test of the intake service against a REAL repo-service:
 * testcontainers PostgreSQL 17 + LocalStack S3 behind {@link RepoServices},
 * intake mounted in front over the in-process transport — the all-in-one
 * embedding path, no fakes anywhere. Proves the receipt is honest: what
 * intake acknowledges is fetchable back out of the repository at the
 * receipt's address, and identical content dedupes.
 */
@Testcontainers(disabledWithoutDocker = true)
class IntakeServiceIT {

    static final String ACCOUNT = "acct-intake-it";
    static final String API_KEY = "it-key";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices repo;
    static IntakeServices intake;
    static ManagedChannel repoChannel;
    static ManagedChannel intakeChannel;
    static IntakeServiceGrpc.IntakeServiceBlockingStub intakeStub;

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "intake-it-docs",
                0,
                null, null, null, null, 0, 0L);
        repo = RepoServices.build(config);
        repo.startInProcess("intake-it-repo");
        repoChannel = InProcessChannelBuilder.forName("intake-it-repo").build();
        DriveServiceGrpc.newBlockingStub(repoChannel)
                .createDrive(
                        CreateDriveRequest.newBuilder()
                                .setName("intake")
                                .setAccountId(ACCOUNT)
                                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                                .build());

        intake =
                IntakeServices.build(
                        new IntakeServiceConfig(
                                0,
                                IntakeServiceConfig.INPROCESS_TARGET_PREFIX + "intake-it-repo",
                                IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES),
                        new InMemoryApiKeyIdentityResolver()
                                .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));
        intake.startInProcess("intake-it-service");
        intakeChannel = InProcessChannelBuilder.forName("intake-it-service").build();
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        intakeStub =
                IntakeServiceGrpc.newBlockingStub(intakeChannel)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    @AfterAll
    static void shutdown() {
        intakeChannel.shutdownNow();
        intake.close();
        repoChannel.shutdownNow();
        repo.close();
    }

    @Test
    void ingestedPayloadIsFetchableAtTheReceiptAddressAndDedupes() {
        byte[] payload = "intake integration payload".getBytes();
        IngestDocumentRequest request =
                IngestDocumentRequest.newBuilder()
                        .setRaw(
                                RawPayload.newBuilder()
                                        .setData(ByteString.copyFrom(payload))
                                        .setFilename("it.txt")
                                        .setMimeType("text/plain"))
                        .setDatasourceId("ds-it")
                        .build();

        IngestDocumentResponse receipt = intakeStub.ingestDocument(request);
        assertThat(receipt.getDocId()).isNotBlank();
        assertThat(receipt.getNodeId()).isNotBlank();
        assertThat(receipt.getDrive()).isEqualTo("intake");
        assertThat(receipt.getDeduplicated()).isFalse();
        assertThat(receipt.getAddress().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(receipt.getAddress().getGraphAddressId()).isEqualTo("ds-it");

        // The receipt is honest: the document is fetchable at its address.
        GetDocumentResponse fetched =
                ai.pipestream.proto.repo.v1.DocumentServiceGrpc.newBlockingStub(repoChannel)
                        .getDocumentByReference(
                                GetDocumentByReferenceRequest.newBuilder()
                                        .setAddress(receipt.getAddress())
                                        .build());
        assertThat(fetched.getDocument().getBlobBag().getBlob().getData().toByteArray())
                .isEqualTo(payload);
        assertThat(fetched.getDocument().getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(fetched.getDocument().getOwnership().getDatasourceId()).isEqualTo("ds-it");

        // Identical content dedupes; the existing coordinates are echoed.
        IngestDocumentResponse again = intakeStub.ingestDocument(request);
        assertThat(again.getDeduplicated()).isTrue();
        assertThat(again.getDocId()).isEqualTo(receipt.getDocId());
        assertThat(again.getNodeId()).isEqualTo(receipt.getNodeId());
    }

    @Test
    void httpLaneUploadIsFetchableAtTheReceiptAddress() throws Exception {
        try (IntakeHttpServer http = intake.startHttp(0)) {
            byte[] payload = "http lane integration payload".getBytes();
            java.net.http.HttpResponse<String> response =
                    java.net.http.HttpClient.newHttpClient()
                            .send(
                                    java.net.http.HttpRequest.newBuilder(
                                                    java.net.URI.create(
                                                            "http://localhost:" + http.port()
                                                                    + IntakeHttpServer.UPLOAD_PATH
                                                                    + "?datasource_id=ds-http-it&filename=it-http.txt"))
                                            .header("x-api-key", API_KEY)
                                            .header("Content-Type", "text/plain")
                                            .POST(
                                                    java.net.http.HttpRequest.BodyPublishers
                                                            .ofByteArray(payload))
                                            .build(),
                                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            IngestDocumentResponse.Builder receipt = IngestDocumentResponse.newBuilder();
            com.google.protobuf.util.JsonFormat.parser().merge(response.body(), receipt);
            assertThat(receipt.getDocId()).isNotBlank();
            assertThat(receipt.getSizeBytes()).isEqualTo(payload.length);
            assertThat(receipt.getDrive()).isEqualTo("intake");
            assertThat(receipt.getAddress().getAccountId()).isEqualTo(ACCOUNT);
            assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
            assertThat(receipt.getAddress().getGraphAddressId()).isEqualTo("ds-http-it");

            // The receipt is honest: the payload is fetchable at its address.
            GetDocumentResponse fetched =
                    ai.pipestream.proto.repo.v1.DocumentServiceGrpc.newBlockingStub(repoChannel)
                            .getDocumentByReference(
                                    GetDocumentByReferenceRequest.newBuilder()
                                            .setAddress(receipt.getAddress())
                                            .build());
            assertThat(fetched.getDocument().getBlobBag().getBlob().getData().toByteArray())
                    .isEqualTo(payload);
            assertThat(fetched.getDocument().getOwnership().getDatasourceId())
                    .isEqualTo("ds-http-it");
        }
    }

    @Test
    void unknownDriveSurfacesRepoErrorUnchanged() {
        assertThatThrownBy(
                        () ->
                                intakeStub.ingestDocument(
                                        IngestDocumentRequest.newBuilder()
                                                .setRaw(
                                                        RawPayload.newBuilder()
                                                                .setData(ByteString.copyFromUtf8("x")))
                                                .setDatasourceId("ds-it")
                                                .setDrive("no-such-drive")
                                                .build()))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e ->
                                assertThat(e.getStatus().getCode())
                                        .isIn(Status.Code.NOT_FOUND, Status.Code.FAILED_PRECONDITION));
    }
}

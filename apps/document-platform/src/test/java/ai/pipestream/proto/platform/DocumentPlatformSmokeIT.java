package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.intake.v1.RawPayload;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStoreConfig;
import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Smoke-proves the one-container platform through its OWN composition root
 * and its EXTERNAL surfaces only: every call in this test crosses a real
 * TCP port the container would expose. Ingest over the intake door, a
 * durable parse submitted through the registry's submit-workflow action and
 * completed by the running worker, the parsed result read back over repo's
 * public gRPC, the registry serving the fleet document model, and the
 * playground page serving.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DocumentPlatformSmokeIT {

    static final String ACCOUNT = "acct-platform";
    static final String API_KEY = "platform-smoke-key";
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @Container
    static final PostgreSQLContainer REPO_DB = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final PostgreSQLContainer JOBS_DB = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    @TempDir
    static Path work;

    static DocumentPlatform platform;
    static ManagedChannel intakeChannel;
    static ManagedChannel repoChannel;
    static URI registryBase;

    static IngestDocumentResponse receipt;

    @BeforeAll
    static void boot() throws Exception {
        platform = DocumentPlatform.start(
                new DocumentPlatformConfig(
                        new RepoServiceConfig(
                                0,
                                new LedgerConfig(
                                        REPO_DB.getJdbcUrl(),
                                        REPO_DB.getUsername(),
                                        REPO_DB.getPassword()),
                                LOCALSTACK.getEndpoint().toString(),
                                LOCALSTACK.getRegion(),
                                LOCALSTACK.getAccessKey(),
                                LOCALSTACK.getSecretKey(),
                                "platform-docs",
                                0,
                                null, null, null, null, 0, 0L),
                        new WorkflowRunStoreConfig(
                                JOBS_DB.getJdbcUrl(),
                                JOBS_DB.getUsername(),
                                JOBS_DB.getPassword(),
                                WorkflowRunStoreConfig.DEFAULT_POOL_SIZE,
                                WorkflowRunStoreConfig.DEFAULT_MIGRATION_LOCATION),
                        work.resolve("registry.git"),
                        0, 0, 0, 0,
                        null, null, null,
                        60L,
                        1),
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));

        repoChannel = NettyChannelBuilder.forAddress("127.0.0.1", platform.repoPort())
                .usePlaintext()
                .build();
        DriveServiceGrpc.newBlockingStub(repoChannel).createDrive(CreateDriveRequest.newBuilder()
                .setName("intake")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                .build());
        intakeChannel = NettyChannelBuilder.forAddress("127.0.0.1", platform.intakePort())
                .usePlaintext()
                .build();
        registryBase = URI.create("http://127.0.0.1:" + platform.registryPort());
    }

    @AfterAll
    static void shutdown() {
        if (intakeChannel != null) {
            intakeChannel.shutdownNow();
        }
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
        if (platform != null) {
            platform.close();
        }
    }

    @Test
    @Order(1)
    void theRegistryServesTheDocumentModelFromFirstBoot() throws Exception {
        HttpResponse<String> subjects = HTTP.send(
                HttpRequest.newBuilder(registryBase.resolve("/subjects")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(subjects.statusCode()).isEqualTo(200);
        assertThat(subjects.body()).contains(DocumentPlatform.DOCUMENT_SUBJECT);
    }

    @Test
    @Order(2)
    void aDocumentEntersThroughTheAuthenticatedDoorOverTcp() {
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        receipt = IntakeServiceGrpc.newBlockingStub(intakeChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .ingestDocument(IngestDocumentRequest.newBuilder()
                        .setRaw(RawPayload.newBuilder()
                                .setData(ByteString.copyFromUtf8(
                                        "Platform Smoke\n\nThe one container works."))
                                .setFilename("smoke.txt")
                                .setMimeType("text/plain"))
                        .setDatasourceId("ds-smoke")
                        .build());
        assertThat(receipt.getDocId()).isNotBlank();
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
    }

    @Test
    @Order(3)
    void aDurableParseSubmittedThroughTheRegistryActionCompletes() throws Exception {
        ObjectNode submit = MAPPER.createObjectNode();
        submit.put("workflowName", "parse-document");
        submit.set("input", MAPPER.readTree(JsonFormat.printer().print(
                ParseDocumentRequest.newBuilder().setAddress(receipt.getAddress()).build())));
        JsonNode submitted = postAction("submit-workflow", submit);
        assertThat(submitted.path("ok").asBoolean()).as(submitted.toString()).isTrue();
        String jobId = submitted.path("jobId").asText();
        assertThat(jobId).isNotBlank();

        // The platform's own worker loops claim and complete the job.
        String status = "";
        for (int i = 0; i < 120 && !"COMPLETED".equals(status); i++) {
            ObjectNode get = MAPPER.createObjectNode();
            get.put("jobId", jobId);
            JsonNode job = postAction("get-job", get);
            status = job.path("job").path("status").asText(job.path("status").asText());
            if ("FAILED".equals(status) || "DEAD".equals(status)) {
                throw new AssertionError("parse job " + status + ": " + job);
            }
            if (!"COMPLETED".equals(status)) {
                Thread.sleep(500);
            }
        }
        assertThat(status).isEqualTo("COMPLETED");

        Document stored = DocumentServiceGrpc.newBlockingStub(repoChannel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .addParts(DocumentPart.DOCUMENT_PART_PARSED)
                        .build())
                .getDocument();
        assertThat(stored.getParserResultsMap()).containsKey("text");
        assertThat(stored.getSearchMetadata().getTitle()).isEqualTo("Platform Smoke");
    }

    @Test
    @Order(4)
    void thePlaygroundServes() throws Exception {
        HttpResponse<String> page = HTTP.send(
                HttpRequest.newBuilder(
                                URI.create("http://127.0.0.1:" + platform.playgroundPort() + "/"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.body()).contains("Parser Playground");
    }

    static JsonNode postAction(String name, ObjectNode input) throws Exception {
        HttpResponse<String> response = HTTP.send(
                HttpRequest.newBuilder(registryBase.resolve("/protomolt/actions/" + name))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(input.toString()))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return MAPPER.readTree(response.body());
    }
}

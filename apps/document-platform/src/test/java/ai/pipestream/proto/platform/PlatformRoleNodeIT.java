package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.intake.v1.RawPayload;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The role-node deployment story at the PLATFORM level: the shipped binary
 * boots as specialized nodes purely by role list, exactly as
 * {@code PROTOMOLT_ROLES} does in a container. Node A is
 * {@code PROTOMOLT_ROLES=repo} (no jobs database, no registry, no search
 * config demanded); node B is {@code PROTOMOLT_ROLES=intake} reaching A
 * through {@code PROTOMOLT_REPO_TARGET}. A document ingested through B's
 * door is read back from A's store, and surfaces outside a node's role
 * list refuse by role name.
 */
@Testcontainers(disabledWithoutDocker = true)
class PlatformRoleNodeIT {

    static final String ACCOUNT = "acct-platform-rolenode";
    static final String API_KEY = "platform-role-node-key";

    @Container
    static final PostgreSQLContainer REPO_DB = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static DocumentPlatform repoNode;
    static DocumentPlatform intakeNode;
    static ManagedChannel repoChannel;
    static ManagedChannel intakeChannel;

    static DocumentPlatformConfig config(List<String> roles, RepoServiceConfig repo,
                                         Map<String, String> environment) {
        return new DocumentPlatformConfig(
                repo, null, null, 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, null, 0, 0,
                roles, environment);
    }

    @BeforeAll
    static void bootBothNodes() throws Exception {
        repoNode = DocumentPlatform.start(
                config(List.of("repo"),
                        new RepoServiceConfig(
                                0,
                                new LedgerConfig(REPO_DB.getJdbcUrl(),
                                        REPO_DB.getUsername(), REPO_DB.getPassword()),
                                LOCALSTACK.getEndpoint().toString(),
                                LOCALSTACK.getRegion(),
                                LOCALSTACK.getAccessKey(),
                                LOCALSTACK.getSecretKey(),
                                "platform-role-node-docs",
                                0,
                                null, null, null, null, 0, 0L),
                        Map.of()),
                null);

        intakeNode = DocumentPlatform.start(
                config(List.of("intake"), null,
                        Map.of("PROTOMOLT_REPO_TARGET",
                                "127.0.0.1:" + repoNode.repoPort())),
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));

        repoChannel = NettyChannelBuilder.forAddress("127.0.0.1", repoNode.repoPort())
                .usePlaintext().build();
        intakeChannel = NettyChannelBuilder.forAddress("127.0.0.1", intakeNode.intakePort())
                .usePlaintext().build();
    }

    @AfterAll
    static void shutdown() {
        if (intakeChannel != null) {
            intakeChannel.shutdownNow();
        }
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
        if (intakeNode != null) {
            intakeNode.close();
        }
        if (repoNode != null) {
            repoNode.close();
        }
    }

    @Test
    void theShippedBinaryRunsAsSpecializedNodesByRoleListAlone() {
        DriveServiceGrpc.newBlockingStub(repoChannel).createDrive(CreateDriveRequest.newBuilder()
                .setName("intake")
                .setAccountId(ACCOUNT)
                .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                .build());

        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        IngestDocumentResponse receipt = IntakeServiceGrpc.newBlockingStub(intakeChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .ingestDocument(IngestDocumentRequest.newBuilder()
                        .setRaw(RawPayload.newBuilder()
                                .setData(ByteString.copyFromUtf8(
                                        "Platform Role Node\n\nOne binary, many roles."))
                                .setFilename("platform-role-node.txt")
                                .setMimeType("text/plain"))
                        .setDatasourceId("ds-platform-role-node")
                        .build());
        assertThat(receipt.getDocId()).isNotBlank();

        Document stored = DocumentServiceGrpc.newBlockingStub(repoChannel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .build())
                .getDocument();
        assertThat(stored.getDocId()).isEqualTo(receipt.getDocId());
    }

    @Test
    void surfacesOutsideTheRoleListRefuseByRoleName() {
        assertThatThrownBy(intakeNode::repoPort)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'repo' is not mounted");
        assertThatThrownBy(repoNode::searchConsolePort)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'search-console' is not mounted");
        assertThatThrownBy(repoNode::metricsPort)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'metrics' is not mounted");
    }

    @Test
    void configurationIsOnlyDemandedForSelectedRoles() {
        assertThatThrownBy(() -> config(List.of("bogus-role"), null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown role 'bogus-role'")
                .hasMessageContaining("known roles");
        assertThatThrownBy(() -> config(List.of("jobs"), null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jobs config is required");
        assertThatThrownBy(() -> DocumentPlatform.start(
                config(List.of("intake"), null, Map.of()), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resolver is required");
        assertThatThrownBy(() -> config(List.of("metrics"), null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the metrics role reads the search index in process");

        assertThat(DocumentPlatformConfig.rolesFromEnvironment(" repo, Search-Console "))
                .containsExactly("repo", "search-console");
        assertThat(DocumentPlatformConfig.rolesFromEnvironment(null))
                .isEqualTo(DocumentPlatformConfig.DEFAULT_ROLES);
    }
}

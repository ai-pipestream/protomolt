package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.composer.Composer;
import ai.pipestream.proto.intake.service.IntakeModule;
import ai.pipestream.proto.intake.service.IntakeServiceConfig;
import ai.pipestream.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.intake.v1.RawPayload;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.service.RepoServiceModule;
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
 * Proves the role-node claim behind the ServiceModule SPI: the SAME module
 * code that composes the one-container platform also runs as two
 * specialized nodes wired over real TCP. Node A mounts only {@code repo};
 * node B mounts only {@code intake} and reaches A through
 * {@code PROTOMOLT_REPO_TARGET}. A document ingested through B's intake service is
 * read back from A's store.
 */
@Testcontainers(disabledWithoutDocker = true)
class RoleNodeIT {

    static final String ACCOUNT = "acct-rolenode";
    static final String API_KEY = "role-node-key";

    @Container
    static final PostgreSQLContainer REPO_DB = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static Composer.Node repoNode;
    static Composer.Node intakeNode;
    static RepoServiceModule repoModule;
    static IntakeModule intakeModule;
    static ManagedChannel repoChannel;
    static ManagedChannel intakeChannel;

    @BeforeAll
    static void bootBothNodes() {
        repoModule = new RepoServiceModule(new RepoServiceConfig(
                0,
                new LedgerConfig(REPO_DB.getJdbcUrl(), REPO_DB.getUsername(), REPO_DB.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "role-node-docs",
                0,
                null, null, null, null, 0, 0L));
        repoNode = Composer.emptyBuilder()
                .module(repoModule)
                .environment(Map.of())
                .build()
                .boot(List.of(RepoServiceModule.ROLE));

        intakeModule = new IntakeModule(new IntakeModule.Config(
                0, -1, IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES,
                new InMemoryApiKeyIdentityResolver()
                        .register(API_KEY, IntakeScope.unrestricted(ACCOUNT))));
        intakeNode = Composer.emptyBuilder()
                .module(intakeModule)
                .environment(Map.of(
                        "PROTOMOLT_REPO_TARGET", "127.0.0.1:" + repoModule.grpcPort()))
                .remoteOpener(target -> NettyChannelBuilder.forTarget(target).usePlaintext().build())
                .build()
                .boot(List.of(IntakeModule.ROLE));

        repoChannel = NettyChannelBuilder.forAddress("127.0.0.1", repoModule.grpcPort())
                .usePlaintext()
                .build();
        intakeChannel = NettyChannelBuilder.forAddress("127.0.0.1", intakeModule.grpcPort())
                .usePlaintext()
                .build();
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
    void aDocumentIngestedOnTheIntakeNodeLandsInTheRepoNode() {
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
                                        "Role Node\n\nTwo nodes, one toolkit."))
                                .setFilename("role-node.txt")
                                .setMimeType("text/plain"))
                        .setDatasourceId("ds-role-node")
                        .build());
        assertThat(receipt.getDocId()).isNotBlank();
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);

        Document stored = DocumentServiceGrpc.newBlockingStub(repoChannel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(receipt.getAddress())
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .build())
                .getDocument();
        assertThat(stored.getDocId()).isEqualTo(receipt.getDocId());
    }
}

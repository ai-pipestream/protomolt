package ai.pipestream.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.parse.v1.ParseCoordinatorServiceGrpc;
import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import ai.pipestream.proto.parse.v1.ParseDocumentResponse;
import ai.pipestream.proto.parse.v1.RoutingRule;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.service.RepoServiceConfig;
import ai.pipestream.proto.repo.service.RepoServices;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.ParseStatus;
import ai.pipestream.proto.repo.v1.ParserDocument;
import ai.pipestream.proto.repo.v1.ParserResult;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
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
 * End-to-end integration test of the coordinator against a REAL repo-service:
 * testcontainers PostgreSQL 17 + LocalStack S3 behind {@link RepoServices},
 * the coordinator mounted in front over the in-process transport, one fake
 * parser plugin at the end of the fan-out. Proves the persisted run is
 * honest: the PARSED part round-trips out of the repository and the folded
 * title landed on CORE, with the untouched BLOBS carried forward.
 */
@Testcontainers(disabledWithoutDocker = true)
class ParseCoordinatorIT {

    static final String ACCOUNT = "acct-parse-it";
    static final byte[] PAYLOAD = "hello parse world, a plainly textual payload".getBytes();
    static final NodeAddress ADDRESS = NodeAddress.newBuilder()
            .setDocId("doc-it-1")
            .setGraphAddressId("ds-parse")
            .setAccountId(ACCOUNT)
            .setGraphId("intake:" + ACCOUNT)
            .build();

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices repo;
    static ManagedChannel repoChannel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static FakeParserPlugin parser;
    static Server parserServer;
    static ParseCoordinatorServices coordinator;
    static ManagedChannel coordinatorChannel;
    static ParseCoordinatorServiceGrpc.ParseCoordinatorServiceBlockingStub stub;

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "parse-it-docs",
                0,
                null, null, null, null, 0, 0L);
        repo = RepoServices.build(config);
        repo.startInProcess("parse-it-repo");
        repoChannel = InProcessChannelBuilder.forName("parse-it-repo").build();
        documents = DocumentServiceGrpc.newBlockingStub(repoChannel);
        DriveServiceGrpc.newBlockingStub(repoChannel)
                .createDrive(CreateDriveRequest.newBuilder()
                        .setName("intake")
                        .setAccountId(ACCOUNT)
                        .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                        .build());

        parser = new FakeParserPlugin("textract", "9.9.0");
        parserServer = InProcessServerBuilder.forName("parse-it-parser")
                .directExecutor()
                .addService(parser)
                .build()
                .start();

        RoutingRules rules = RoutingRules.of(List.of(RoutingRule.newBuilder()
                .setRuleId("r-text")
                .setWhen("mime_type == 'text/plain'")
                .setParserName("textract")
                .setPriority(1)
                .build()));
        coordinator = ParseCoordinatorServices.build(
                new ParseCoordinatorConfig(
                        0, ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + "parse-it-repo",
                        "intake", 60),
                rules,
                ParserRegistry.of(Map.of(
                        "textract",
                        ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + "parse-it-parser")));
        coordinator.startInProcess("parse-it-door");
        coordinatorChannel = InProcessChannelBuilder.forName("parse-it-door").build();
        stub = ParseCoordinatorServiceGrpc.newBlockingStub(coordinatorChannel);
    }

    @AfterAll
    static void shutdown() {
        coordinatorChannel.shutdownNow();
        coordinator.close();
        parserServer.shutdownNow();
        repoChannel.shutdownNow();
        repo.close();
    }

    @Test
    void parsedPartRoundTripsAndTheFoldedTitleLandsOnCore() throws Exception {
        // Seed: a real intake save of a text document with an inline blob.
        documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(Document.newBuilder()
                        .setDocId("doc-it-1")
                        .setOwnership(OwnershipContext.newBuilder()
                                .setAccountId(ACCOUNT)
                                .setDatasourceId("ds-parse")
                                .setConnectorId("it"))
                        .setSearchMetadata(SearchMetadata.newBuilder()
                                .setSourceMimeType("text/plain"))
                        .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                                .setBlobId("blob-it-1")
                                .setData(ByteString.copyFrom(PAYLOAD))
                                .setMimeType("text/plain")
                                .setFilename("note.txt")
                                .setSizeBytes(PAYLOAD.length))))
                .setDrive("intake")
                .setConnectorId("it")
                .setUseDatasourceId(true)
                .setGraphId("intake:" + ACCOUNT)
                .build());

        parser.claimsToEmit = List.of(Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Folded Title").build())
                .build());
        parser.outputDocument = ParserDocument.newBuilder()
                .setShape(Any.pack(StringValue.of("textract-exhaust")))
                .putMetadata("pages", "1")
                .build();

        ParseDocumentResponse response = stub.parseDocument(
                ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());
        assertThat(response.getParserResultsMap()).containsOnlyKeys("textract");
        assertThat(response.getParserResultsMap().get("textract").getStatus())
                .isEqualTo(ParseStatus.PARSE_STATUS_OK);
        assertThat(response.getSearchMetadataFold().getWinnersMap())
                .containsEntry("title", "textract");
        assertThat(parser.payloadsSeen.getFirst()).isEqualTo(PAYLOAD);

        // The PARSED part round-trips out of the repository.
        Document parsed = documents.getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(ADDRESS)
                        .addParts(DocumentPart.DOCUMENT_PART_PARSED)
                        .build())
                .getDocument();
        assertThat(parsed.getParserResultsMap()).containsOnlyKeys("textract");
        ParserResult stored = parsed.getParserResultsMap().get("textract");
        assertThat(stored.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_OK);
        assertThat(stored.getParserVersion()).isEqualTo("9.9.0");
        assertThat(stored.getConfigFingerprint())
                .isEqualTo(ConfigFingerprints.fingerprint("r-text", Struct.getDefaultInstance()));
        assertThat(stored.getDocument().getShape().unpack(StringValue.class).getValue())
                .isEqualTo("textract-exhaust");

        // The folded title landed on CORE.
        Document core = documents.getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(ADDRESS)
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .build())
                .getDocument();
        assertThat(core.getSearchMetadata().getTitle()).isEqualTo("Folded Title");
        assertThat(core.getSearchMetadata().getSourceMimeType()).isEqualTo("text/plain");

        // The untouched BLOBS part was carried forward, not lost.
        Document blobs = documents.getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(ADDRESS)
                        .addParts(DocumentPart.DOCUMENT_PART_BLOBS)
                        .build())
                .getDocument();
        assertThat(blobs.getBlobBag().getBlob().getData().toByteArray()).isEqualTo(PAYLOAD);
    }
}

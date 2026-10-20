package ai.protomolt.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.parse.v1.ParseCoordinatorServiceGrpc;
import ai.protomolt.proto.parse.v1.ParseDocumentRequest;
import ai.protomolt.proto.parse.v1.ParseDocumentResponse;
import ai.protomolt.proto.parse.v1.RoutingRule;
import ai.protomolt.proto.repo.accession.V2RepositoryFixture;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v1.SearchMetadata;
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
 * testcontainers PostgreSQL 18 + LocalStack S3 behind {@link V2RepositoryFixture},
 * the coordinator mounted in front over the in-process transport, one fake
 * parser plugin at the end of the fan-out. Proves the persisted run is
 * honest: the PARSED part round-trips out of the repository and the folded
 * title landed on CORE, with the untouched BLOBS carried forward.
 */
@Testcontainers
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
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static V2RepositoryFixture repo;
    static ManagedChannel repoChannel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static FakeParserPlugin parser;
    static Server parserServer;
    static ParseCoordinatorServices coordinator;
    static ManagedChannel coordinatorChannel;
    static ParseCoordinatorServiceGrpc.ParseCoordinatorServiceBlockingStub stub;

    @BeforeAll
    static void boot() throws Exception {
        repo = new V2RepositoryFixture(POSTGRES, LOCALSTACK, "parse-it-repo", ACCOUNT);
        repoChannel = InProcessChannelBuilder.forName("parse-it-repo").build();
        documents = DocumentServiceGrpc.newBlockingStub(repoChannel);

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
        coordinator.startInProcess("parse-it-coordinator");
        coordinatorChannel = InProcessChannelBuilder.forName("parse-it-coordinator").build();
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
                                .setConnectorId("it")
                                .setSecurity(ai.protomolt.proto.repo.v1.DocumentSecurity.newBuilder()
                                        .addPermissions(ai.protomolt.proto.repo.v1.AccessRule.newBuilder()
                                                .setIdentityType("service").setIdentity("parse-coordinator").setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ))
                                        .addPermissions(ai.protomolt.proto.repo.v1.AccessRule.newBuilder()
                                                .setIdentityType("service").setIdentity("parse-coordinator").setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_WRITE))))
                        .setSearchMetadata(SearchMetadata.newBuilder()
                                .setSourceMimeType("text/plain"))
                        .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                                .setBlobId("blob-it-1")
                                .setData(ByteString.copyFrom(PAYLOAD))
                                .setMimeType("text/plain")
                                .setFilename("note.txt")
                                .setSizeBytes(PAYLOAD.length))))
                .setDrive("intake")
                .setOperationId("seed-parse-it")
                .setTarget(ai.protomolt.proto.repo.v2.DocumentTarget.newBuilder().setAddress(ADDRESS))
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setNone(true))
                .setOwner(ai.protomolt.proto.repo.v2.SaveOwner.newBuilder().setIntake(
                        ai.protomolt.proto.repo.v2.IntakeOrigin.newBuilder().setDatasourceId("ds-parse").setConnectorId("it")))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                .build());

        parser.claimsToEmit = List.of(Struct.newBuilder()
                .putFields("title", Value.newBuilder().setStringValue("Folded Title").build())
                .build());
        parser.outputDocument = ParserDocument.newBuilder()
                .setShape(Any.pack(SearchMetadata.newBuilder().setBody("textract-exhaust").build()))
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
        Document parsed = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(ADDRESS)
                        .setSelection(ai.protomolt.proto.repo.v2.PartSelection.newBuilder().addSelectors(ai.protomolt.proto.repo.v2.PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)))
                        .build())
                .getDocument();
        assertThat(parsed.getParserResultsMap()).containsOnlyKeys("textract");
        ParserResult stored = parsed.getParserResultsMap().get("textract");
        assertThat(stored.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_OK);
        assertThat(stored.getParserVersion()).isEqualTo("9.9.0");
        assertThat(stored.getConfigFingerprint())
                .isEqualTo(ConfigFingerprints.fingerprint("r-text", Struct.getDefaultInstance()));
        assertThat(stored.getDocument().getShape().unpack(SearchMetadata.class).getBody())
                .isEqualTo("textract-exhaust");

        // The folded title landed on CORE.
        Document core = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(ADDRESS)
                        .setSelection(ai.protomolt.proto.repo.v2.PartSelection.newBuilder().addSelectors(ai.protomolt.proto.repo.v2.PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                        .build())
                .getDocument();
        assertThat(core.getSearchMetadata().getTitle()).isEqualTo("Folded Title");
        assertThat(core.getSearchMetadata().getSourceMimeType()).isEqualTo("text/plain");

        // The untouched BLOBS part was carried forward, not lost.
        Document blobs = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(ADDRESS)
                        .setSelection(ai.protomolt.proto.repo.v2.PartSelection.newBuilder().addSelectors(ai.protomolt.proto.repo.v2.PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)))
                        .build())
                .getDocument();
        assertThat(blobs.getBlobBag().getBlob().hasStorageRef()).isTrue();
        assertThat(repo.retainedBlob(ADDRESS, 2)).isEqualTo(PAYLOAD);
    }
    private static ai.protomolt.proto.repo.v2.PartWrite put(DocumentPart part) {
        return ai.protomolt.proto.repo.v2.PartWrite.newBuilder().setSelector(
                ai.protomolt.proto.repo.v2.PartSelector.newBuilder().setPart(part))
                .setAction(ai.protomolt.proto.repo.v2.WriteAction.WRITE_ACTION_PUT).build();
    }
}

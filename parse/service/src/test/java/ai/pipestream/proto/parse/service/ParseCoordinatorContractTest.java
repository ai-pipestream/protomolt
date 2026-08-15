package ai.pipestream.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.parse.v1.ParseCoordinatorServiceGrpc;
import ai.pipestream.proto.parse.v1.ParseDocumentRequest;
import ai.pipestream.proto.parse.v1.ParseDocumentResponse;
import ai.pipestream.proto.parse.v1.PlannedParse;
import ai.pipestream.proto.parse.v1.RouteDocumentRequest;
import ai.pipestream.proto.parse.v1.RouteDocumentResponse;
import ai.pipestream.proto.parse.v1.RoutingRule;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the coordinator wire contract against a recording fake repo and
 * configurable fake parser plugins: what RouteDocument plans, which parses
 * ParseDocument runs, what each outcome is recorded as, how the claims fold,
 * and exactly which SaveDocument request persists it all.
 */
class ParseCoordinatorContractTest {

    static final String ACCOUNT = "acct-contract";
    static final String ALPHA_VERSION = "1.2.3";
    static final Struct ALPHA_CONFIG = Struct.newBuilder()
            .putFields("ocr", Value.newBuilder().setBoolValue(true).build())
            .build();
    static final byte[] PDF_PAYLOAD = "%PDF-1.7 fake pdf content for the contract".getBytes();
    static final NodeAddress ADDRESS = NodeAddress.newBuilder()
            .setDocId("doc-1")
            .setGraphAddressId("ds-c")
            .setAccountId(ACCOUNT)
            .setGraphId("intake:" + ACCOUNT)
            .build();

    static FakeDocumentService repo;
    static Server repoServer;
    static FakeParserPlugin alpha;
    static Server alphaServer;
    static FakeParserPlugin beta;
    static Server betaServer;
    static ParseCoordinatorServices coordinator;
    static ManagedChannel channel;
    static ParseCoordinatorServiceGrpc.ParseCoordinatorServiceBlockingStub stub;

    @BeforeAll
    static void boot() throws Exception {
        repo = new FakeDocumentService();
        String repoName = InProcessServerBuilder.generateName();
        repoServer = InProcessServerBuilder.forName(repoName)
                .directExecutor()
                .addService(repo)
                .build()
                .start();

        alpha = new FakeParserPlugin("alpha", ALPHA_VERSION);
        String alphaName = InProcessServerBuilder.generateName();
        alphaServer = InProcessServerBuilder.forName(alphaName)
                .directExecutor()
                .addService(alpha)
                .build()
                .start();
        beta = new FakeParserPlugin("beta", "0.9");
        String betaName = InProcessServerBuilder.generateName();
        betaServer = InProcessServerBuilder.forName(betaName)
                .directExecutor()
                .addService(beta)
                .build()
                .start();

        RoutingRules rules = RoutingRules.of(List.of(
                RoutingRule.newBuilder()
                        .setRuleId("r-pdf")
                        .setWhen("mime_type == 'application/pdf'")
                        .setParserName("alpha")
                        .setParserConfig(ALPHA_CONFIG)
                        .setPriority(10)
                        .build(),
                RoutingRule.newBuilder()
                        .setRuleId("r-any")
                        .setWhen("true")
                        .setParserName("beta")
                        .setPriority(5)
                        .build()));
        ParserRegistry parsers = ParserRegistry.of(java.util.Map.of(
                "alpha", ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + alphaName,
                "beta", ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + betaName));
        coordinator = ParseCoordinatorServices.build(
                new ParseCoordinatorConfig(
                        0, ParseCoordinatorConfig.INPROCESS_TARGET_PREFIX + repoName, "intake", 30),
                rules,
                parsers);
        String doorName = InProcessServerBuilder.generateName();
        coordinator.startInProcess(doorName);
        channel = InProcessChannelBuilder.forName(doorName).build();
        stub = ParseCoordinatorServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        coordinator.close();
        alphaServer.shutdownNow();
        betaServer.shutdownNow();
        repoServer.shutdownNow();
    }

    @BeforeEach
    void reset() {
        repo.saves.clear();
        repo.saveGate = null;
        repo.drive = "";
        alpha.reset();
        beta.reset();
        repo.seed(ADDRESS, seededDocument());
    }

    static Document seededDocument() {
        return Document.newBuilder()
                .setDocId("doc-1")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(ACCOUNT)
                        .setDatasourceId("ds-c")
                        .setConnectorId("intake"))
                .setSearchMetadata(SearchMetadata.newBuilder().setSourceMimeType("text/plain"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-1")
                        .setData(ByteString.copyFrom(PDF_PAYLOAD))
                        // The declared type lies; the magic bytes must win.
                        .setMimeType("text/plain")
                        .setFilename("report.pdf")
                        .setSizeBytes(PDF_PAYLOAD.length)))
                .build();
    }

    static Struct claims(Object... keyValues) {
        Struct.Builder claims = Struct.newBuilder();
        for (int i = 0; i < keyValues.length; i += 2) {
            Value.Builder value = Value.newBuilder();
            if (keyValues[i + 1] instanceof String s) {
                value.setStringValue(s);
            } else {
                value.setNumberValue(((Number) keyValues[i + 1]).doubleValue());
            }
            claims.putFields((String) keyValues[i], value.build());
        }
        return claims.build();
    }

    @Test
    void routeDocumentDryRunSniffsOverTheDeclarationAndPlansInPriorityOrder() {
        RouteDocumentResponse response = stub.routeDocument(
                RouteDocumentRequest.newBuilder().setAddress(ADDRESS).build());

        assertThat(response.getContentType()).isEqualTo("application/pdf");
        assertThat(response.getContentTypeSniffed()).isTrue();
        assertThat(response.getPlannedParsesList())
                .extracting(PlannedParse::getParserName)
                .containsExactly("alpha", "beta");
        assertThat(response.getPlannedParses(0).getMatchedRuleId()).isEqualTo("r-pdf");
        assertThat(response.getPlannedParses(0).getParserConfig()).isEqualTo(ALPHA_CONFIG);
        assertThat(response.getPlannedParses(1).getMatchedRuleId()).isEqualTo("r-any");
        // A dry run executes nothing and writes nothing.
        assertThat(repo.saves).isEmpty();
        assertThat(alpha.optionsSeen).isEmpty();
        assertThat(beta.optionsSeen).isEmpty();
    }

    @Test
    void routeDocumentRoutesAnInlineDocumentWithoutTheRepo() {
        Document inline = Document.newBuilder()
                .setDocId("inline-1")
                .setOwnership(OwnershipContext.newBuilder().setAccountId(ACCOUNT))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setData(ByteString.copyFromUtf8("just plain readable text"))
                        .setFilename("note.txt")))
                .build();
        RouteDocumentResponse response = stub.routeDocument(
                RouteDocumentRequest.newBuilder().setDocument(inline).build());
        assertThat(response.getContentType()).isEqualTo("text/plain");
        assertThat(response.getContentTypeSniffed()).isTrue();
        assertThat(response.getPlannedParsesList())
                .extracting(PlannedParse::getParserName)
                .containsExactly("beta");
    }

    @Test
    void parseDocumentRecordsResultsFoldsClaimsAndPersistsThePinnedSaveShape() throws Exception {
        alpha.claimsToEmit = List.of(claims("title", "Alpha Title", "language", "en"));
        alpha.outputDocument = ParserDocument.newBuilder()
                .setShape(Any.pack(StringValue.of("alpha-exhaust")))
                .build();
        beta.claimsToEmit = List.of(claims(
                "title", "Beta Title",
                "author", "Beta Author",
                "page_count", 5,
                "no_such_field", "ignored"));

        ParseDocumentResponse response = stub.parseDocument(
                ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());

        // Per-parser results, keyed by parser name.
        assertThat(response.getParserResultsMap()).containsOnlyKeys("alpha", "beta");
        ParserResult alphaResult = response.getParserResultsMap().get("alpha");
        assertThat(alphaResult.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_OK);
        assertThat(alphaResult.getParserVersion()).isEqualTo(ALPHA_VERSION);
        assertThat(alphaResult.getConfigFingerprint())
                .isEqualTo(ConfigFingerprints.fingerprint("r-pdf", ALPHA_CONFIG));
        assertThat(alphaResult.getDocument().getShape().unpack(StringValue.class).getValue())
                .isEqualTo("alpha-exhaust");
        assertThat(alphaResult.hasParsedDate()).isTrue();

        // The fold: the higher-priority parser's title wins; page_count is not
        // a string SearchMetadata field and no_such_field names nothing.
        assertThat(response.getSearchMetadataFold().getFoldedFieldsList())
                .containsExactlyInAnyOrder("title", "language", "author");
        assertThat(response.getSearchMetadataFold().getWinnersMap())
                .containsEntry("title", "alpha")
                .containsEntry("language", "alpha")
                .containsEntry("author", "beta");

        // The parser saw the sniffed type, the filename, and the rule config.
        assertThat(alpha.optionsSeen).hasSize(1);
        assertThat(alpha.optionsSeen.getFirst().getDocumentId()).isEqualTo("doc-1");
        assertThat(alpha.optionsSeen.getFirst().getContentType()).isEqualTo("application/pdf");
        assertThat(alpha.optionsSeen.getFirst().getFilename()).isEqualTo("report.pdf");
        assertThat(alpha.optionsSeen.getFirst().getConfig()).isEqualTo(ALPHA_CONFIG);
        assertThat(alpha.payloadsSeen.getFirst()).isEqualTo(PDF_PAYLOAD);

        // The pinned persist shape.
        assertThat(repo.saves).hasSize(1);
        SaveDocumentRequest save = repo.saves.getFirst();
        assertThat(save.getPartsWrittenList()).containsExactly(
                DocumentPart.DOCUMENT_PART_PARSED, DocumentPart.DOCUMENT_PART_CORE);
        assertThat(save.getCopyUnwrittenPartsFrom()).isEqualTo(ADDRESS);
        assertThat(save.getUseDatasourceId()).isTrue();
        assertThat(save.getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(save.getConnectorId()).isEqualTo("parse-coordinator");
        assertThat(save.getWrittenBy().getModuleId()).isEqualTo("parse-coordinator");
        assertThat(save.getDrive()).isEqualTo("intake");

        Document persisted = save.getDocument();
        assertThat(persisted.getParserResultsMap()).containsOnlyKeys("alpha", "beta");
        assertThat(persisted.getSearchMetadata().getTitle()).isEqualTo("Alpha Title");
        assertThat(persisted.getSearchMetadata().getLanguage()).isEqualTo("en");
        assertThat(persisted.getSearchMetadata().getAuthor()).isEqualTo("Beta Author");
        // Untouched loaded fields survive the fold.
        assertThat(persisted.getSearchMetadata().getSourceMimeType()).isEqualTo("text/plain");
        assertThat(persisted.getBlobBag().getBlob().getData().toByteArray()).isEqualTo(PDF_PAYLOAD);
    }

    @Test
    void theLoadedRowsDriveWinsOverTheConfiguredFallback() {
        repo.drive = "acct-drive";
        stub.parseDocument(ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());
        assertThat(repo.saves.getFirst().getDrive()).isEqualTo("acct-drive");
    }

    @Test
    void aParserFailureIsRecordedAsFailedAndTheSaveStillHappens() {
        alpha.failWith = Status.INTERNAL.withDescription("docling exploded");
        beta.claimsToEmit = List.of(claims("title", "Beta Title"));

        ParseDocumentResponse response = stub.parseDocument(
                ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());

        ParserResult failed = response.getParserResultsMap().get("alpha");
        assertThat(failed.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_FAILED);
        assertThat(failed.getError()).contains("docling exploded");
        assertThat(failed.hasDocument()).isFalse();
        assertThat(response.getParserResultsMap().get("beta").getStatus())
                .isEqualTo(ParseStatus.PARSE_STATUS_OK);
        // The failed parser folds nothing; the survivor's claim wins.
        assertThat(response.getSearchMetadataFold().getWinnersMap())
                .containsEntry("title", "beta");
        // The FAILED result is persisted, never omitted.
        assertThat(repo.saves).hasSize(1);
        assertThat(repo.saves.getFirst().getDocument().getParserResultsMap())
                .containsOnlyKeys("alpha", "beta");
        assertThat(repo.saves.getFirst().getDocument().getParserResultsMap().get("alpha").getStatus())
                .isEqualTo(ParseStatus.PARSE_STATUS_FAILED);
    }

    @Test
    void aParserCompletingWithoutADocumentIsRecordedAsFailed() {
        alpha.emitOutput = false;
        ParseDocumentResponse response = stub.parseDocument(
                ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());
        ParserResult failed = response.getParserResultsMap().get("alpha");
        assertThat(failed.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_FAILED);
        assertThat(failed.getError()).isEqualTo("parser completed without emitting a document");
    }

    @Test
    void warningsMakeTheResultPartialWithTheJoinedWarningsAsError() {
        beta.warnings = List.of("page 3 unreadable", "page 9 unreadable");
        ParseDocumentResponse response = stub.parseDocument(
                ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build());
        ParserResult partial = response.getParserResultsMap().get("beta");
        assertThat(partial.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_PARTIAL);
        assertThat(partial.getError()).isEqualTo("page 3 unreadable; page 9 unreadable");
        // A PARTIAL parse still stored its document.
        assertThat(partial.hasDocument()).isTrue();
    }

    @Test
    void parserOverrideBypassesRuleMatching() {
        ParseDocumentResponse response = stub.parseDocument(ParseDocumentRequest.newBuilder()
                .setAddress(ADDRESS)
                .addParserOverride("beta")
                .build());
        assertThat(response.getParserResultsMap()).containsOnlyKeys("beta");
        ParserResult result = response.getParserResultsMap().get("beta");
        assertThat(result.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_OK);
        // Override entries carry no rule identity and the default config.
        assertThat(result.getConfigFingerprint())
                .isEqualTo(ConfigFingerprints.fingerprint("", Struct.getDefaultInstance()));
        assertThat(alpha.optionsSeen).isEmpty();
        assertThat(beta.optionsSeen.getFirst().getConfig())
                .isEqualTo(Struct.getDefaultInstance());
    }

    @Test
    void anUnknownParserIsRecordedAsFailedNotThrown() {
        ParseDocumentResponse response = stub.parseDocument(ParseDocumentRequest.newBuilder()
                .setAddress(ADDRESS)
                .addParserOverride("ghost")
                .build());
        ParserResult ghost = response.getParserResultsMap().get("ghost");
        assertThat(ghost.getStatus()).isEqualTo(ParseStatus.PARSE_STATUS_FAILED);
        assertThat(ghost.getError()).isEqualTo("no registered parser 'ghost'");
        // Even an all-failed run persists its results.
        assertThat(repo.saves).hasSize(1);
        assertThat(repo.saves.getFirst().getDocument().getParserResultsMap())
                .containsOnlyKeys("ghost");
    }

    @Test
    void blankAddressFieldsAreRejectedLoudlyByName() {
        assertThatThrownBy(() -> stub.parseDocument(ParseDocumentRequest.newBuilder()
                .setAddress(ADDRESS.toBuilder().clearGraphId())
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("graph_id");
                });
        assertThatThrownBy(() -> stub.parseDocument(ParseDocumentRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("doc_id");
                });
        assertThatThrownBy(() -> stub.routeDocument(RouteDocumentRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("source");
                });
    }

    @Test
    void concurrentParsesOfTheSameDocumentSerializeTheSave() throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        repo.saveGate = gate;
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<ParseDocumentResponse> first = executor.submit(() -> stub.parseDocument(
                    ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build()));
            Future<ParseDocumentResponse> second = executor.submit(() -> stub.parseDocument(
                    ParseDocumentRequest.newBuilder().setAddress(ADDRESS).build()));

            // Wait for the first save to arrive (and block on the gate)...
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
            while (repo.saves.isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(repo.saves).hasSize(1);
            // ...then prove the second save is HELD BACK by the per-document
            // lock, not merely slow: it must not arrive while the first is
            // still in flight.
            Thread.sleep(300);
            assertThat(repo.saves).hasSize(1);

            gate.countDown();
            repo.saveGate = null;
            first.get();
            second.get();
            assertThat(repo.saves).hasSize(2);
        }
    }
}

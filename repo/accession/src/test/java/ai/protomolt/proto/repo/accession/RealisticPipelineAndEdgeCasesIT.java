package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end multi-stage pipeline lifecycle and Slice 1 review round 2 edge cases
 * (#335 section 4) against real PostgreSQL and LocalStack S3.
 */
class RealisticPipelineAndEdgeCasesIT {
    private static final String CONNECTOR = "connector-intake";
    private static final String PARSER_TIKA = "tika";
    private static final String CHUNKER_PARA = "chunker-para";
    private static final String CHUNKER_SENT = "chunker-sent";
    private static final String EMBEDDER_MINILM = "embedder-minilm";
    private static final String EMBEDDER_E5 = "embedder-e5";

    private static final String SEG_PARAGRAPH = "paragraph-v1";
    private static final String SEG_SENTENCE = "sentence-v1";
    private static final String EMB_MINILM = "minilm-l6";
    private static final String EMB_E5 = "e5-small";

    private AccessionFixture f;
    private String account;
    private NodeAddress address;

    @BeforeEach
    void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-pipeline");
    }

    @AfterEach
    void close() {
        f.close();
    }

    @Test
    void connectorRecrawlParserDualChunkerDualEmbedderPartialReadAndInvalidation() throws Exception {
        var allWriters = new String[]{
                CONNECTOR, PARSER_TIKA, CHUNKER_PARA, CHUNKER_SENT, EMBEDDER_MINILM, EMBEDDER_E5
        };

        // ------------------------------------------------------------------
        // Stage 1: Connector Initial Crawl (Rev 1: CORE + BLOBS)
        // ------------------------------------------------------------------
        var connectorCred = registerCaller(CONNECTOR);
        f.intakeGrant(account, connectorCred, "ds-1", true);
        var connectorCaller = writer(account, CONNECTOR, connectorCred, 1);

        var initialRawBytes = ByteString.copyFromUtf8("raw-pdf-bytes");
        var rev1Doc = Document.newBuilder()
                .setDocId(address.getDocId())
                .setOwnership(ownership(account, allWriters))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v1").setBody("Alpha Beta"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("raw-1")
                        .setSizeBytes(initialRawBytes.size())
                        .setData(initialRawBytes)))
                .build();

        var rev1Response = f.saves.save(connectorCaller, SaveDocumentRequest.newBuilder()
                .setOperationId("crawl-op-1")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(rev1Doc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                .setOwner(intakeOwner())
                .build());
        assertThat(rev1Response.getReplayed()).isFalse();
        assertThat(rev1Response.getAccession().getRevision()).isEqualTo(1L);
        assertThat(rev1Response.getAccession().getPreviousRevision()).isZero();
        assertThat(f.stack.puts.get()).as("CORE + BLOBS + 1 raw blob object").isEqualTo(3);

        // ------------------------------------------------------------------
        // Stage 2: Connector Re-Crawl with Optimistic CAS Retry & Replay (Rev 2)
        // ------------------------------------------------------------------
        f.stack.reset();
        var recrawlBody = "First paragraph.\n\nSecond paragraph.";
        var rev2CoreDoc = Document.newBuilder()
                .setDocId(address.getDocId())
                .setOwnership(ownership(account, allWriters))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v2").setBody(recrawlBody))
                .build();
        var recrawlOptimistic = SaveDocumentRequest.newBuilder()
                .setOperationId("recrawl-op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(rev2CoreDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .setOwner(intakeOwner())
                .build();

        assertThatThrownBy(() -> f.saves.save(connectorCaller, recrawlOptimistic))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.ABORTED);
                    var conflict = (RevisionConflict) failure.detail().orElseThrow();
                    assertThat(conflict.getCurrentRevision()).isEqualTo(1L);
                    assertThat(conflict.getAddress()).isEqualTo(address);
                });
        assertThat(f.catalog.outcome(account, "recrawl-op-2"))
                .as("CAS conflict claims no operation outcome")
                .isEmpty();
        assertThat(f.stack.puts.get()).isZero();

        // Retry with the SAME operation_id and ExpectedRevision.revision = 1
        var recrawlRetry = recrawlOptimistic.toBuilder().setExpected(at(1)).build();
        var rev2Response = f.saves.save(connectorCaller, recrawlRetry);
        assertThat(rev2Response.getReplayed()).isFalse();
        assertThat(rev2Response.getAccession().getRevision()).isEqualTo(2L);
        assertThat(rev2Response.getAccession().getPreviousRevision()).isEqualTo(1L);
        assertThat(f.stack.puts.get()).as("Only CORE uploaded; carried BLOBS is untouched").isEqualTo(1);
        assertThat(f.stack.opens.get()).isZero();

        // Replay the exact retry (operation_id = "recrawl-op-2", ExpectedRevision.revision = 1)
        var rev2Replay = f.saves.save(connectorCaller, recrawlRetry);
        assertThat(rev2Replay.getReplayed()).isTrue();
        assertThat(rev2Replay.getAccession().getRevision()).isEqualTo(2L);
        assertThat(rev2Replay.getAccession()).isEqualTo(rev2Response.getAccession());
        assertThat(f.stack.puts.get()).as("Replay uploads nothing").isEqualTo(1);

        // ------------------------------------------------------------------
        // Stage 3: Parser Step (Rev 3: PARSED added, CORE + BLOBS carried)
        // ------------------------------------------------------------------
        f.stack.reset();
        var tikaCred = registerCaller(PARSER_TIKA);
        f.stepGrant(account, tikaCred, PARSER_TIKA, false);
        var tikaCaller = writer(account, PARSER_TIKA, tikaCred, 1);

        var parsedDoc = Document.newBuilder()
                .setDocId(address.getDocId())
                .putParserResults(PARSER_TIKA, ParserResult.newBuilder()
                        .setParserName(PARSER_TIKA)
                        .setParserVersion("2.9")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .build())
                .build();
        var rev3Response = f.saves.save(tikaCaller, SaveDocumentRequest.newBuilder()
                .setOperationId("parse-op-3")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(2))
                .setDocument(parsedDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .setOwner(stepOwner(PARSER_TIKA))
                .build());
        assertThat(rev3Response.getAccession().getRevision()).isEqualTo(3L);
        assertThat(f.stack.puts.get()).as("Only PARSED is uploaded").isEqualTo(1);
        assertThat(f.stack.opens.get()).isZero();

        // ------------------------------------------------------------------
        // Stage 4: Concurrent Dual Chunker Fan-Out with CAS Conflict Recovery (Rev 4 & Rev 5)
        // ------------------------------------------------------------------
        var paraChunkerCred = registerCaller(CHUNKER_PARA);
        f.stepGrant(account, paraChunkerCred, CHUNKER_PARA, false);
        var paraChunkerCaller = writer(account, CHUNKER_PARA, paraChunkerCred, 1);

        var sentChunkerCred = registerCaller(CHUNKER_SENT);
        f.stepGrant(account, sentChunkerCred, CHUNKER_SENT, false);
        var sentChunkerCaller = writer(account, CHUNKER_SENT, sentChunkerCred, 1);

        // Both chunkers read Rev 3 CORE
        var rev3ForPara = f.reads.getDocument(paraChunkerCaller, GetDocumentRequest.newBuilder()
                .setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(3))
                .setSelection(PartSelection.newBuilder().addSelectors(partSelector(DocumentPart.DOCUMENT_PART_CORE)))
                .build());
        var rev3ForSent = f.reads.getDocument(sentChunkerCaller, GetDocumentRequest.newBuilder()
                .setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(3))
                .setSelection(PartSelection.newBuilder().addSelectors(partSelector(DocumentPart.DOCUMENT_PART_CORE)))
                .build());
        assertThat(rev3ForPara.getDocument().getSearchMetadata().getBody()).isEqualTo(recrawlBody);
        assertThat(rev3ForSent.getDocument().getSearchMetadata().getBody()).isEqualTo(recrawlBody);

        var paragraphSeg = bodySegmentation(SEG_PARAGRAPH, SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW, recrawlBody,
                segment("p0", 0, "First paragraph.", 0, 16),
                segment("p1", 1, "Second paragraph.", 18, 35));
        var sentenceSeg = bodySegmentation(SEG_SENTENCE, SegmentationKind.SEGMENTATION_KIND_SEMANTIC_BOUNDARY, recrawlBody,
                segment("s0", 0, "First paragraph.", 0, 16),
                segment("s1", 1, "Second paragraph.", 18, 35));

        f.stack.reset();
        var rev4Response = f.saves.save(paraChunkerCaller, SaveDocumentRequest.newBuilder()
                .setOperationId("chunk-para-op")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(3))
                .setDocument(semanticDoc(List.of(paragraphSeg), List.of()))
                .addWrites(writeSegmentation(SEG_PARAGRAPH))
                .setOwner(stepOwner(CHUNKER_PARA))
                .build());
        assertThat(rev4Response.getAccession().getRevision()).isEqualTo(4L);
        assertThat(f.stack.puts.get()).isEqualTo(1);

        // Chunker 2 attempts save at stale ExpectedRevision.revision = 3 -> ABORTED (current_revision = 4)
        f.stack.reset();
        var sentChunkStaleReq = SaveDocumentRequest.newBuilder()
                .setOperationId("chunk-sent-op")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(3))
                .setDocument(semanticDoc(List.of(sentenceSeg), List.of()))
                .addWrites(writeSegmentation(SEG_SENTENCE))
                .setOwner(stepOwner(CHUNKER_SENT))
                .build();
        assertThatThrownBy(() -> f.saves.save(sentChunkerCaller, sentChunkStaleReq))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.ABORTED);
                    var conflict = (RevisionConflict) failure.detail().orElseThrow();
                    assertThat(conflict.getCurrentRevision()).isEqualTo(4L);
                });
        assertThat(f.catalog.outcome(account, "chunk-sent-op")).isEmpty();
        assertThat(f.stack.puts.get()).isZero();

        // Chunker 2 retries with the SAME operation_id at ExpectedRevision.revision = 4 -> commits Rev 5
        var rev5Response = f.saves.save(sentChunkerCaller, sentChunkStaleReq.toBuilder().setExpected(at(4)).build());
        assertThat(rev5Response.getReplayed()).isFalse();
        assertThat(rev5Response.getAccession().getRevision()).isEqualTo(5L);
        assertThat(f.stack.puts.get()).isEqualTo(1);

        var rev5Manifest = rev5Response.getAccession().getManifest();
        assertThat(rev5Manifest.getPartsList())
                .extracting(entry -> DocumentSaves.describe(entry.getSelector()))
                .containsExactly("CORE", "BLOBS", "CHUNKS " + SEG_PARAGRAPH, "CHUNKS " + SEG_SENTENCE, "PARSED");
        assertThat(rev5Manifest.getPartsList())
                .allSatisfy(entry -> assertThat(entry.getState()).isEqualTo(PartState.PART_STATE_PRESENT));

        // ------------------------------------------------------------------
        // Stage 5: Concurrent Dual Embedder Fan-Out with CAS Conflict Recovery (Rev 6 & Rev 7)
        // ------------------------------------------------------------------
        var minilmCred = registerCaller(EMBEDDER_MINILM);
        f.stepGrant(account, minilmCred, EMBEDDER_MINILM, false);
        var minilmCaller = writer(account, EMBEDDER_MINILM, minilmCred, 1);

        var e5Cred = registerCaller(EMBEDDER_E5);
        f.stepGrant(account, e5Cred, EMBEDDER_E5, false);
        var e5Caller = writer(account, EMBEDDER_E5, e5Cred, 1);

        var paragraphDigest = rev5Manifest.getPartsList().stream()
                .filter(entry -> entry.getSelector().equals(segmentationSelector(SEG_PARAGRAPH)))
                .findFirst().orElseThrow().getSegmentationDigest();
        assertThat(paragraphDigest).isEqualTo(SegmentationDigests.digest(paragraphSeg));

        var minilmSet = embeddingSet(SEG_PARAGRAPH, EMB_MINILM, paragraphDigest, "p0", "p1");
        var e5Set = embeddingSet(SEG_PARAGRAPH, EMB_E5, paragraphDigest, "p0", "p1");

        f.stack.reset();
        var rev6Response = f.saves.save(minilmCaller, SaveDocumentRequest.newBuilder()
                .setOperationId("embed-minilm-op")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(5))
                .setDocument(semanticDoc(List.of(), List.of(minilmSet)))
                .addWrites(writeEmbedding(SEG_PARAGRAPH, EMB_MINILM))
                .setOwner(stepOwner(EMBEDDER_MINILM))
                .build());
        assertThat(rev6Response.getAccession().getRevision()).isEqualTo(6L);
        assertThat(f.stack.puts.get()).isEqualTo(1);

        // Embedder 2 attempts save at stale ExpectedRevision.revision = 5 -> ABORTED (current_revision = 6)
        f.stack.reset();
        var e5StaleReq = SaveDocumentRequest.newBuilder()
                .setOperationId("embed-e5-op")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(5))
                .setDocument(semanticDoc(List.of(), List.of(e5Set)))
                .addWrites(writeEmbedding(SEG_PARAGRAPH, EMB_E5))
                .setOwner(stepOwner(EMBEDDER_E5))
                .build();
        assertThatThrownBy(() -> f.saves.save(e5Caller, e5StaleReq))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.ABORTED);
                    var conflict = (RevisionConflict) failure.detail().orElseThrow();
                    assertThat(conflict.getCurrentRevision()).isEqualTo(6L);
                });
        assertThat(f.catalog.outcome(account, "embed-e5-op")).isEmpty();
        assertThat(f.stack.puts.get()).isZero();

        // Embedder 2 retries with the SAME operation_id at ExpectedRevision.revision = 6 -> commits Rev 7
        var rev7Response = f.saves.save(e5Caller, e5StaleReq.toBuilder().setExpected(at(6)).build());
        assertThat(rev7Response.getReplayed()).isFalse();
        assertThat(rev7Response.getAccession().getRevision()).isEqualTo(7L);
        assertThat(f.stack.puts.get()).isEqualTo(1);

        // ------------------------------------------------------------------
        // Stage 6: Selective Downstream Read on Rev 7
        // ------------------------------------------------------------------
        f.stack.reset();
        var selectiveRead = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder()
                .setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(7))
                .setSelection(PartSelection.newBuilder()
                        .addSelectors(partSelector(DocumentPart.DOCUMENT_PART_CORE))
                        .addSelectors(segmentationSelector(SEG_PARAGRAPH))
                        .addSelectors(embeddingSelector(SEG_PARAGRAPH, EMB_MINILM)))
                .build());
        assertThat(f.stack.opens.get()).as("Only CORE, paragraph-v1, and minilm-l6 are opened").isEqualTo(3);
        var readDoc = selectiveRead.getDocument();
        assertThat(readDoc.getSearchMetadata().getBody()).isEqualTo(recrawlBody);
        assertThat(readDoc.getSearchMetadata().getSegmentationsList())
                .extracting(Segmentation::getSegmentationId)
                .containsExactly(SEG_PARAGRAPH);
        assertThat(readDoc.getSearchMetadata().getEmbeddingSetsList())
                .extracting(EmbeddingSet::getEmbeddingSetId)
                .containsExactly(EMB_MINILM);
        assertThat(readDoc.hasBlobBag()).isFalse();
        assertThat(readDoc.getParserResultsCount()).isZero();

        // ------------------------------------------------------------------
        // Stage 7: Re-Crawl Content Update & SEGMENTATION_MISMATCH Gate (Rev 8)
        // ------------------------------------------------------------------
        f.stack.reset();
        var updatedBody = "Completely new text.";
        var updatedParaSeg = bodySegmentation(SEG_PARAGRAPH, SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW, updatedBody,
                segment("p0", 0, "Completely new text.", 0, 20));
        var updatedSentSeg = bodySegmentation(SEG_SENTENCE, SegmentationKind.SEGMENTATION_KIND_SEMANTIC_BOUNDARY, updatedBody,
                segment("s0", 0, "Completely new text.", 0, 20));
        var updatedDoc = Document.newBuilder()
                .setDocId(address.getDocId())
                .setOwnership(ownership(account, allWriters))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("v3")
                        .setBody(updatedBody)
                        .addSegmentations(updatedParaSeg)
                        .addSegmentations(updatedSentSeg))
                .build();

        // Attempt to update CORE + CHUNKS while leaving EMBEDDINGS("minilm-l6") and EMBEDDINGS("e5-small") carried forward
        var mismatchReq = SaveDocumentRequest.newBuilder()
                .setOperationId("recrawl-mismatch-op")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(7))
                .setDocument(updatedDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(writeSegmentation(SEG_PARAGRAPH))
                .addWrites(writeSegmentation(SEG_SENTENCE))
                .setOwner(intakeOwner())
                .build();
        assertThatThrownBy(() -> f.saves.save(connectorCaller, mismatchReq))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    var rejection = (SaveRejection) failure.detail().orElseThrow();
                    assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH);
                    assertThat(rejection.getViolationsList())
                            .extracting(v -> v.getPart().getEmbeddingSet(), FieldViolation::getRuleId)
                            .containsExactlyInAnyOrder(
                                    tuple(embeddingKey(SEG_PARAGRAPH, EMB_E5), "embedding-set.segmentation-digest"),
                                    tuple(embeddingKey(SEG_PARAGRAPH, EMB_MINILM), "embedding-set.segmentation-digest"));
                });
        assertThat(f.stack.puts.get()).as("Rejected before any stack upload").isZero();
        assertThat(f.catalog.outcome(account, "recrawl-mismatch-op").orElseThrow().outcome().getRejection().getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH);

        // Atomically write PUT on CORE + PUT on both CHUNKS + WRITE_ACTION_DELETE("re-crawled") on both EMBEDDINGS -> Rev 8
        f.stack.reset();
        var rev8Response = f.saves.save(connectorCaller, SaveDocumentRequest.newBuilder()
                .setOperationId("recrawl-clean-op-8")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(7))
                .setDocument(updatedDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(writeSegmentation(SEG_PARAGRAPH))
                .addWrites(writeSegmentation(SEG_SENTENCE))
                .addWrites(deleteEmbedding(SEG_PARAGRAPH, EMB_MINILM, "re-crawled"))
                .addWrites(deleteEmbedding(SEG_PARAGRAPH, EMB_E5, "re-crawled"))
                .setOwner(intakeOwner())
                .build());
        assertThat(rev8Response.getAccession().getRevision()).isEqualTo(8L);
        assertThat(f.stack.puts.get()).as("CORE + 2 CHUNKS uploaded; deleted embeddings upload nothing").isEqualTo(3);

        var rev8Manifest = rev8Response.getAccession().getManifest();
        for (var embId : List.of(EMB_MINILM, EMB_E5)) {
            assertThat(rev8Manifest.getPartsList())
                    .filteredOn(p -> p.getSelector().equals(embeddingSelector(SEG_PARAGRAPH, embId)))
                    .singleElement()
                    .satisfies(entry -> {
                        assertThat(entry.getState()).isEqualTo(PartState.PART_STATE_DELETED);
                        assertThat(entry.getDeletedReason()).isEqualTo("re-crawled");
                    });
        }
    }

    // #335 Section 4 item 1
    @Test
    void unwrittenPartContentInDocumentIsRejectedUnrecorded() {
        var coreOnly = Document.newBuilder()
                .setDocId(address.getDocId())
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("v1").setBody("Alpha"))
                .build();
        var seg = bodySegmentation("seg-1", SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW, "Alpha",
                segment("s0", 0, "Alpha", 0, 5));
        var emb = embeddingSet("seg-1", "emb-1", SegmentationDigests.digest(seg), "s0");

        var createStrays = Map.<String, Document>of(
                "stray-parsed", coreOnly.toBuilder().putParserResults(PARSER_TIKA, ParserResult.newBuilder()
                        .setParserName(PARSER_TIKA).setParserVersion("1").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build(),
                "stray-blobs", coreOnly.toBuilder().setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("b1").setSizeBytes(3).setData(ByteString.copyFromUtf8("abc")))).build(),
                "stray-chunks", coreOnly.toBuilder().setSearchMetadata(coreOnly.getSearchMetadata().toBuilder()
                        .addSegmentations(seg)).build(),
                "stray-embeddings", coreOnly.toBuilder().setSearchMetadata(coreOnly.getSearchMetadata().toBuilder()
                        .addEmbeddingSets(emb)).build());

        for (var entry : createStrays.entrySet()) {
            var opId = entry.getKey();
            var request = SaveDocumentRequest.newBuilder()
                    .setOperationId(opId)
                    .setTarget(DocumentTarget.newBuilder().setAddress(address))
                    .setExpected(ExpectedRevision.newBuilder().setNone(true))
                    .setDocument(entry.getValue())
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                    .setOwner(intakeOwner())
                    .build();
            assertMalformedUnrecorded(opId, request, "is present but the part is not written");
        }

        // Seed Rev 1 so we can test edits where only PARSED is written or deleted
        f.saves.save(processAuthority(), create("seed-rev-1", address, document(account, address.getDocId(), "v1")).build());
        f.stack.reset();

        // Edit putting only PARSED while supplying search_metadata.title (CORE content)
        var parsedWithCoreTitle = Document.newBuilder()
                .setDocId(address.getDocId())
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("sneaky-title"))
                .putParserResults(PARSER_TIKA, ParserResult.newBuilder()
                        .setParserName(PARSER_TIKA).setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build())
                .build();
        var editPutParsedWithCore = SaveDocumentRequest.newBuilder()
                .setOperationId("edit-stray-core")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .setDocument(parsedWithCoreTitle)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .setOwner(intakeOwner())
                .build();
        assertMalformedUnrecorded("edit-stray-core", editPutParsedWithCore, "Content for CORE is present but the part is not written");

        // Edit deleting PARSED while supplying parser_results content
        var parsedOnly = Document.newBuilder()
                .setDocId(address.getDocId())
                .putParserResults(PARSER_TIKA, ParserResult.newBuilder()
                        .setParserName(PARSER_TIKA).setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build())
                .build();
        var editDeleteParsedWithContent = SaveDocumentRequest.newBuilder()
                .setOperationId("edit-delete-with-content")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .setDocument(parsedOnly)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(partSelector(DocumentPart.DOCUMENT_PART_PARSED))
                        .setAction(WriteAction.WRITE_ACTION_DELETE)
                        .setDeletedReason("obsolete"))
                .setOwner(intakeOwner())
                .build();
        assertMalformedUnrecorded("edit-delete-with-content", editDeleteParsedWithContent,
                "Content for PARSED is present but the part is not written");
    }

    // #335 Section 4 item 2
    @Test
    void duplicateAndContradictoryPartSelectorsAreRejectedUnrecorded() {
        var baseDoc = document(account, address.getDocId(), "v1");
        f.saves.save(processAuthority(), create("seed-1", address, baseDoc).build());
        f.stack.reset();

        var parsedDoc = Document.newBuilder()
                .setDocId(address.getDocId())
                .putParserResults(PARSER_TIKA, baseDoc.getParserResultsOrThrow(PARSER_TIKA))
                .build();
        var seg = bodySegmentation("seg-1", SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW, "body of v1",
                segment("s0", 0, "body of v1", 0, 10));

        // 1. Duplicate PartWrite for DOCUMENT_PART_PARSED
        var dupParsedWrite = SaveDocumentRequest.newBuilder()
                .setOperationId("dup-parsed-write")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .setDocument(parsedDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .setOwner(intakeOwner())
                .build();
        assertMalformedUnrecorded("dup-parsed-write", dupParsedWrite, "Duplicate write selector PARSED");

        // 2. Duplicate PartWrite for CHUNKS("seg-1")
        var dupChunksWrite = SaveDocumentRequest.newBuilder()
                .setOperationId("dup-chunks-write")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .setDocument(semanticDoc(List.of(seg), List.of()))
                .addWrites(writeSegmentation("seg-1"))
                .addWrites(writeSegmentation("seg-1"))
                .setOwner(intakeOwner())
                .build();
        assertMalformedUnrecorded("dup-chunks-write", dupChunksWrite, "Duplicate write selector CHUNKS seg-1");

        // 3. Contradictory PartWrite(PARSED) + PartSource(PARSED)
        var writeAndSourceParsed = SaveDocumentRequest.newBuilder()
                .setOperationId("contradict-parsed")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .setDocument(parsedDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .addSources(PartSource.newBuilder()
                        .setSelector(partSelector(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(address)
                        .setFromRevision(1))
                .setOwner(intakeOwner())
                .build();
        assertMalformedUnrecorded("contradict-parsed", writeAndSourceParsed, "Selector PARSED is named twice");

        // 4. Contradictory PartWrite(CHUNKS("seg-1")) + PartSource(CHUNKS("seg-1"))
        var writeAndSourceChunks = SaveDocumentRequest.newBuilder()
                .setOperationId("contradict-chunks")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .setDocument(semanticDoc(List.of(seg), List.of()))
                .addWrites(writeSegmentation("seg-1"))
                .addSources(PartSource.newBuilder()
                        .setSelector(segmentationSelector("seg-1"))
                        .setFromAddress(address)
                        .setFromRevision(1))
                .setOwner(intakeOwner())
                .build();
        assertMalformedUnrecorded("contradict-chunks", writeAndSourceChunks, "Selector CHUNKS seg-1 is named twice");
    }

    // #335 Section 4 item 3
    @Test
    void unknownDriveIsRejectedAndNonDefaultDriveRoutesWritesAndReadsCorrectly() throws Exception {
        // 1. Unknown drive is rejected with INVALID_ARGUMENT / MALFORMED_REQUEST (unrecorded, zero stack puts)
        var unknownDriveReq = create("unknown-drive-op", address, document(account, address.getDocId(), "v1"))
                .setDrive("unknown-drive")
                .build();
        assertMalformedUnrecorded("unknown-drive-op", unknownDriveReq, "Unknown drive unknown-drive");

        // 2. Second CountingStack registered as "archive-stack" and mapped to drive name "archive"
        var archivePrefix = "archive/" + UUID.randomUUID();
        var archiveStackId = f.catalog.registerStack(StackRecord.newBuilder()
                .setStackId("archive-stack")
                .setProvider("s3")
                .setEndpoint(S3.getEndpoint().toString())
                .setNamespace(BUCKET + "/" + archivePrefix)
                .build());
        var archiveStack = new CountingStack(new BlobStoreStack(archiveStackId, new S3BlobStore(s3()), BUCKET, archivePrefix));
        var drives = new DocumentSaves.Drives("default", Map.of("default", f.stack, "archive", archiveStack));
        var saves = new DocumentSaves(f.catalog, drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
        var reads = new DocumentReads(f.catalog, drives, limits(), bytes -> () -> {}, Clock.systemUTC());

        var rawBlobBytes = ByteString.copyFromUtf8("archived-raw-payload");
        var seg = bodySegmentation("seg-1", SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW, "Archived body",
                segment("s0", 0, "Archived body", 0, 13));
        var archiveDoc = Document.newBuilder()
                .setDocId(address.getDocId())
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Archive Doc")
                        .setBody("Archived body")
                        .addSegmentations(seg))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-arch")
                        .setSizeBytes(rawBlobBytes.size())
                        .setData(rawBlobBytes)))
                .build();

        var saved = saves.save(processAuthority(), SaveDocumentRequest.newBuilder()
                .setOperationId("archive-save-1")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDrive("archive")
                .setDocument(archiveDoc)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                .addWrites(writeSegmentation("seg-1"))
                .setOwner(intakeOwner())
                .build());
        assertThat(saved.getAccession().getRevision()).isEqualTo(1L);
        assertThat(f.stack.puts.get()).as("Default stack receives zero puts").isZero();
        assertThat(archiveStack.puts.get()).as("Archive stack receives 1 raw blob + CORE + BLOBS + CHUNKS").isEqualTo(4);

        var readResp = reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        assertThat(f.stack.opens.get()).isZero();
        assertThat(archiveStack.opens.get()).isEqualTo(3);
        assertThat(readResp.getDocument().getSearchMetadata().getBody()).isEqualTo("Archived body");
        assertThat(readResp.getDocument().getSearchMetadata().getSegmentationsList())
                .extracting(Segmentation::getSegmentationId)
                .containsExactly("seg-1");

        var claimCheck = readResp.getDocument().getBlobBag().getBlob().getStorageRef();
        assertThat(claimCheck.getDriveName()).isEqualTo("archive-stack");
        var storedManifest = f.catalog.manifest(address, 1).orElseThrow();
        var blobRef = storedManifest.getPartsList().stream()
                .flatMap(p -> p.getBlobRefsList().stream())
                .filter(b -> b.getBlobId().equals("blob-arch"))
                .findFirst().orElseThrow().getRef();
        assertThat(blobRef.getStackId()).isEqualTo("archive-stack");
        try (var stream = drives.byId("archive-stack").open(blobRef)) {
            assertThat(stream.readAllBytes()).isEqualTo(rawBlobBytes.toByteArray());
        }
    }

    @Test
    void dynamicPerAccountDrivesIsolateStacksAcrossAccounts() {
        var accountB = "acct-b-" + UUID.randomUUID();
        var addressB = NodeAddress.newBuilder()
                .setDocId("doc-b-" + UUID.randomUUID())
                .setGraphId("graph-1")
                .setGraphAddressId("node-1")
                .setAccountId(accountB)
                .build();
        var stackBPrefix = "acct-b/" + UUID.randomUUID();
        var stackBId = f.catalog.registerStack(StackRecord.newBuilder()
                .setStackId("stack-b-" + UUID.randomUUID())
                .setProvider("s3")
                .setEndpoint(S3.getEndpoint().toString())
                .setNamespace(BUCKET + "/" + stackBPrefix)
                .build());
        var stackB = new CountingStack(new BlobStoreStack(stackBId, new S3BlobStore(s3()), BUCKET, stackBPrefix));

        var byAccountAndDrive = Map.of(
                account + "|default", f.stack,
                account + "|tenant-only", f.stack,
                accountB + "|default", stackB);
        var byStackId = Map.of(
                f.stack.stackId(), f.stack,
                stackB.stackId(), stackB);
        var dynamicDrives = DocumentSaves.Drives.dynamic(
                "default",
                (acct, drv) -> java.util.Optional.ofNullable(byAccountAndDrive.get(acct + "|" + drv)),
                id -> java.util.Optional.ofNullable(byStackId.get(id)));

        var saves = new DocumentSaves(f.catalog, dynamicDrives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
        var reads = new DocumentReads(f.catalog, dynamicDrives, limits(), bytes -> () -> {}, Clock.systemUTC());

        // Account A saves to "tenant-only" -> routes to f.stack (1 raw blob + CORE + BLOBS + PARSED = 4 puts)
        saves.save(processAuthority(), create("dyn-a-1", address, document(account, address.getDocId(), "A-title"))
                .setDrive("tenant-only")
                .build());
        assertThat(f.stack.puts.get()).isEqualTo(4);
        assertThat(stackB.puts.get()).isZero();

        // Account B attempts "tenant-only" (not provisioned for Account B) -> rejected unrecorded
        var unprovisioned = create("dyn-b-unprov", addressB, document(accountB, addressB.getDocId(), "B-title"))
                .setDrive("tenant-only")
                .build();
        assertThatThrownBy(() -> saves.save(processAuthority(), unprovisioned))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(failure.getMessage()).contains("Unknown drive tenant-only");
                });
        assertThat(f.catalog.outcome(accountB, "dyn-b-unprov")).isEmpty();
        assertThat(stackB.puts.get()).isZero();

        // Account B saves to default drive ("") -> routes to stackB
        saves.save(processAuthority(), create("dyn-b-1", addressB, document(accountB, addressB.getDocId(), "B-title")).build());
        assertThat(f.stack.puts.get()).isEqualTo(4);
        assertThat(stackB.puts.get()).isEqualTo(4);

        // Reads resolve each revision's stackId via dynamicDrives.byId(...)
        assertThat(reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("A-title");
        assertThat(reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(addressB).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("B-title");
    }

    // #335 Section 4 item 4
    @Test
    void malformedPageTokensAreRejectedWithInvalidArgument() {
        f.saves.save(processAuthority(), create("seed-page", address, document(account, address.getDocId(), "v1")).build());

        for (var badToken : List.of("0", "-1", "+5", "abc", "99999999999999999999999999")) {
            var request = ListRevisionsRequest.newBuilder()
                    .setAddress(address)
                    .setPageSize(10)
                    .setPageToken(badToken)
                    .build();
            assertThatThrownBy(() -> f.reads.listRevisions(processAuthority(), request))
                    .as("page_token=%s", badToken)
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(failure.getMessage()).contains("Invalid page token");
                    });
        }
    }

    // #335 Section 4 item 5
    @Test
    void intakeTargetDatasourceMismatchWithOwnershipOrOwnerIsRejected() {
        // 1. Target use_datasource_id = "ds-1", but document.ownership.datasource_id = "ds-2"
        var docWithDs2 = document(account, address.getDocId(), "v1").toBuilder()
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(account)
                        .setDatasourceId("ds-2")
                        .setSecurity(security()))
                .build();
        var mismatchOwnershipReq = create("mismatch-ownership-ds", address, docWithDs2)
                .setTarget(DocumentTarget.newBuilder().setUseDatasourceId("ds-1"))
                .build();
        assertMalformedUnrecorded("mismatch-ownership-ds", mismatchOwnershipReq,
                "The target datasource differs from the document's ownership");

        // 2. Target use_datasource_id = "ds-1", document.ownership.datasource_id = "ds-1",
        //    but owner.intake.datasource_id = "ds-2"
        var mismatchOwnerReq = create("mismatch-owner-ds", address, document(account, address.getDocId(), "v1"))
                .setTarget(DocumentTarget.newBuilder().setUseDatasourceId("ds-1"))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-2")))
                .build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), mismatchOwnerReq))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED);
                    var rejection = (SaveRejection) failure.detail().orElseThrow();
                    assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
                    assertThat(failure.getMessage()).contains("The intake owner differs from the document's datasource");
                });
        assertThat(f.catalog.outcome(account, "mismatch-owner-ds")).isEmpty();
        assertThat(f.stack.puts.get()).isZero();
    }

    // #335 Section 4 item 6
    @Test
    void sourcingEmptyAndDeletedPartsPreservesStateWithoutStackUploads() {
        // Rev 1: CORE + BLOBS + PARSED
        f.saves.save(processAuthority(), create("rev-1", address, document(account, address.getDocId(), "v1")).build());

        // Rev 2: PARSED = WRITE_ACTION_EMPTY
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder()
                .setOperationId("rev-2-empty")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .addWrites(PartWrite.newBuilder()
                        .setSelector(partSelector(DocumentPart.DOCUMENT_PART_PARSED))
                        .setAction(WriteAction.WRITE_ACTION_EMPTY))
                .setOwner(intakeOwner())
                .build());

        // Rev 3: PARSED = WRITE_ACTION_DELETE("redacted")
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder()
                .setOperationId("rev-3-delete")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(2))
                .addWrites(PartWrite.newBuilder()
                        .setSelector(partSelector(DocumentPart.DOCUMENT_PART_PARSED))
                        .setAction(WriteAction.WRITE_ACTION_DELETE)
                        .setDeletedReason("redacted"))
                .setOwner(intakeOwner())
                .build());

        // Rev 4: Source PARSED from Rev 2 (PART_STATE_EMPTY)
        f.stack.reset();
        var rev4 = f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder()
                .setOperationId("rev-4-source-empty")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(3))
                .addSources(PartSource.newBuilder()
                        .setSelector(partSelector(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(address)
                        .setFromRevision(2))
                .setOwner(intakeOwner())
                .build());
        assertThat(rev4.getAccession().getRevision()).isEqualTo(4L);
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.stack.opens.get()).isZero();

        var storedRev4 = f.catalog.manifest(address, 4).orElseThrow();
        var parsedRev4 = storedPart(storedRev4, partSelector(DocumentPart.DOCUMENT_PART_PARSED));
        assertThat(parsedRev4.hasRef()).isFalse();
        assertThat(parsedRev4.getEntry().getState()).isEqualTo(PartState.PART_STATE_EMPTY);
        assertThat(parsedRev4.getEntry().getSha256()).isEmpty();
        assertThat(parsedRev4.getEntry().getSizeBytes()).isZero();
        assertThat(parsedRev4.getEntry().getDeletedReason()).isEmpty();
        assertThat(parsedRev4.getEntry().getWrittenInRevision()).isEqualTo(4L);

        // Rev 5: Source PARSED from Rev 3 (PART_STATE_DELETED)
        f.stack.reset();
        var rev5 = f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder()
                .setOperationId("rev-5-source-deleted")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(4))
                .addSources(PartSource.newBuilder()
                        .setSelector(partSelector(DocumentPart.DOCUMENT_PART_PARSED))
                        .setFromAddress(address)
                        .setFromRevision(3))
                .setOwner(intakeOwner())
                .build());
        assertThat(rev5.getAccession().getRevision()).isEqualTo(5L);
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.stack.opens.get()).isZero();

        var storedRev5 = f.catalog.manifest(address, 5).orElseThrow();
        var parsedRev5 = storedPart(storedRev5, partSelector(DocumentPart.DOCUMENT_PART_PARSED));
        assertThat(parsedRev5.hasRef()).isFalse();
        assertThat(parsedRev5.getEntry().getState()).isEqualTo(PartState.PART_STATE_DELETED);
        assertThat(parsedRev5.getEntry().getDeletedReason()).isEqualTo("redacted");
        assertThat(parsedRev5.getEntry().getSha256()).isEmpty();
        assertThat(parsedRev5.getEntry().getSizeBytes()).isZero();
        assertThat(parsedRev5.getEntry().getWrittenInRevision()).isEqualTo(5L);
    }

    // ------------------------------------------------------------------ helpers

    private UUID registerCaller(String name) {
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, name);
        return credential;
    }

    private void assertMalformedUnrecorded(String opId, SaveDocumentRequest request, String expectedMessageSubstring) {
        f.stack.reset();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                .as(opId)
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    var rejection = (SaveRejection) failure.detail().orElseThrow();
                    assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                    assertThat(failure.getMessage()).contains(expectedMessageSubstring);
                });
        assertThat(f.catalog.outcome(account, opId)).as("outcome for %s", opId).isEmpty();
        assertThat(f.stack.puts.get()).as("stack puts for %s", opId).isZero();
    }

    private static PartSelector partSelector(DocumentPart part) {
        return PartSelector.newBuilder().setPart(part).build();
    }

    private static PartSelector segmentationSelector(String id) {
        return PartSelector.newBuilder().setSegmentationId(id).build();
    }

    private static PartSelector embeddingSelector(String segmentationId, String embeddingSetId) {
        return PartSelector.newBuilder().setEmbeddingSet(embeddingKey(segmentationId, embeddingSetId)).build();
    }

    private static EmbeddingSetKey embeddingKey(String segmentationId, String embeddingSetId) {
        return EmbeddingSetKey.newBuilder().setSegmentationId(segmentationId).setEmbeddingSetId(embeddingSetId).build();
    }

    private static PartWrite writeSegmentation(String id) {
        return PartWrite.newBuilder().setSelector(segmentationSelector(id)).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    private static PartWrite writeEmbedding(String segmentationId, String embeddingSetId) {
        return PartWrite.newBuilder().setSelector(embeddingSelector(segmentationId, embeddingSetId))
                .setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    private static PartWrite deleteEmbedding(String segmentationId, String embeddingSetId, String reason) {
        return PartWrite.newBuilder().setSelector(embeddingSelector(segmentationId, embeddingSetId))
                .setAction(WriteAction.WRITE_ACTION_DELETE).setDeletedReason(reason).build();
    }

    private static StoredPartEntry storedPart(StoredManifest manifest, PartSelector selector) {
        return manifest.getPartsList().stream()
                .filter(p -> p.getEntry().getSelector().equals(selector))
                .findFirst()
                .orElseThrow();
    }

    private static Segmentation bodySegmentation(String id, SegmentationKind kind, String body, Segment... segments) {
        var utf8 = body.getBytes(StandardCharsets.UTF_8);
        var builder = Segmentation.newBuilder()
                .setSegmentationId(id)
                .setKind(kind)
                .setSource(TextSource.newBuilder()
                        .setFieldPath("search_metadata.body")
                        .setBinding(SourceTextBinding.newBuilder()
                                .setResolvedTextSha256(DocumentSaves.sha256(ByteString.copyFrom(utf8)))
                                .setUtf8Length(utf8.length)))
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE);
        for (var seg : segments) builder.addSegments(seg);
        return builder.build();
    }

    private static Segment segment(String id, int ordinal, String text, long start, long end) {
        return Segment.newBuilder().setSegmentId(id).setOrdinal(ordinal).setText(text).setStart(start).setEnd(end).build();
    }

    private static EmbeddingSet embeddingSet(String segmentationId, String setId, String digest, String... segmentIds) {
        var builder = EmbeddingSet.newBuilder()
                .setEmbeddingSetId(setId)
                .setSegmentationId(segmentationId)
                .setSegmentationDigest(digest)
                .setModel(setId + "-v1")
                .setDimensions(2)
                .setSimilarity(Similarity.SIMILARITY_COSINE);
        for (var segId : segmentIds) {
            builder.addVectors(SegmentVector.newBuilder().setSegmentId(segId).addValues(0.5f).addValues(-0.25f));
        }
        return builder.build();
    }

    private static Document semanticDoc(List<Segmentation> segmentations, List<EmbeddingSet> embeddingSets) {
        return Document.newBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addAllSegmentations(segmentations)
                        .addAllEmbeddingSets(embeddingSets))
                .build();
    }
}

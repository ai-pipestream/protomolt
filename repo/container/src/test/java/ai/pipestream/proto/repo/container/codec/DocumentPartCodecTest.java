package ai.pipestream.proto.repo.container.codec;

import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.ParsedMetadata;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Byte-fidelity gate for the descriptor-driven four-part split:
 * {@code assemble(split(doc))} must reproduce the original document
 * byte-for-byte — over synthetic multi-part documents, degenerate
 * CORE-only documents, and a layout that maps a different field set (the
 * genericity proof that the codec is descriptor-driven, not Document-shaped).
 */
class DocumentPartCodecTest {

    private static final PartLayout LAYOUT = PartLayouts.document();

    @Test
    void roundTrip_fullDocument_isByteIdentical() throws Exception {
        Document doc = fullDoc();

        List<PartObject> parts = DocumentPartCodec.split(doc, LAYOUT);
        Document assembled = DocumentPartCodec.assemble(
                parts.stream().map(PartObject::bytes).toList(), Document.getDefaultInstance());

        assertThat(assembled.toByteArray()).isEqualTo(doc.toByteArray());
        // Canonical manifest order: CORE, BLOBS, CHUNKS sub-objects, PARSED.
        assertThat(parts.stream().map(PartObject::part))
                .containsExactly(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS,
                        DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_CHUNKS,
                        DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_PARSED);
        // Consecutive-run grouping: [A, A, B, A] -> runs A, B, A#2.
        assertThat(parts.stream().filter(p -> p.part() == DocumentPart.DOCUMENT_PART_CHUNKS)
                .map(PartObject::subKey))
                .containsExactly("title-384", "body-768", "title-384#2");
    }

    @Test
    void split_fullDocument_fragmentsAreValidDocumentsCarryingOnlyTheirPart() throws Exception {
        Document doc = fullDoc();
        List<PartObject> parts = DocumentPartCodec.split(doc, LAYOUT);

        for (PartObject p : parts) {
            Document.parseFrom(p.bytes()); // every fragment parses as a Document
        }

        Document blobs = Document.parseFrom(parts.get(1).bytes());
        assertThat(blobs.getDocId()).isEqualTo(doc.getDocId());
        assertThat(blobs.hasBlobBag()).isTrue();
        assertThat(blobs.hasSearchMetadata()).isFalse();
        assertThat(blobs.getParsedMetadataCount()).isZero();

        Document core = Document.parseFrom(parts.get(0).bytes());
        assertThat(core.hasSearchMetadata()).isTrue(); // parent kept, as it was set
        assertThat(core.getSearchMetadata().getSemanticResultsCount()).isZero();
        assertThat(core.getSearchMetadata().getTitle()).isEqualTo("A doc");
        assertThat(core.hasBlobBag()).isFalse();
        assertThat(core.getParsedMetadataCount()).isZero();

        // First chunk-set fragment: identity + the two consecutive title-384 results.
        Document chunk = Document.parseFrom(parts.get(2).bytes());
        assertThat(chunk.getDocId()).isEqualTo(doc.getDocId());
        assertThat(chunk.getSearchMetadata().getSemanticResultsList())
                .extracting(SemanticProcessingResult::getResultId)
                .containsExactly("title-384", "title-384");
        assertThat(chunk.getSearchMetadata().getTitle()).isEmpty();

        Document parsed = Document.parseFrom(parts.get(5).bytes());
        assertThat(parsed.getParsedMetadataCount()).isEqualTo(1);
        assertThat(parsed.hasBlobBag()).isFalse();
        assertThat(parsed.hasSearchMetadata()).isFalse();
    }

    @Test
    void roundTrip_coreOnlyDocument_isByteIdentical() throws Exception {
        Document doc = Document.newBuilder()
                .setDocId("core-only")
                .setSearchMetadata(ai.pipestream.proto.repo.v1.SearchMetadata.newBuilder()
                        .setTitle("Just a title").setBody("No parts here"))
                .build();

        List<PartObject> parts = DocumentPartCodec.split(doc, LAYOUT);

        assertThat(parts.stream().map(PartObject::part))
                .containsExactly(DocumentPart.DOCUMENT_PART_CORE);
        Document assembled = DocumentPartCodec.assemble(
                parts.stream().map(PartObject::bytes).toList(), Document.getDefaultInstance());
        assertThat(assembled.toByteArray()).isEqualTo(doc.toByteArray());
    }

    @Test
    void roundTrip_documentWithoutSearchMetadata_doesNotMaterializeIt() throws Exception {
        Document doc = Document.newBuilder()
                .setDocId("no-search-metadata")
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("b1").setData(ByteString.copyFromUtf8("bytes"))))
                .build();

        List<PartObject> parts = DocumentPartCodec.split(doc, LAYOUT);
        Document assembled = DocumentPartCodec.assemble(
                parts.stream().map(PartObject::bytes).toList(), Document.getDefaultInstance());

        assertThat(assembled.toByteArray()).isEqualTo(doc.toByteArray());
        Document core = Document.parseFrom(parts.get(0).bytes());
        assertThat(core.hasSearchMetadata()).isFalse(); // not materialized by the split
    }

    @Test
    void roundTrip_blankResultIds_groupAsSingletonRuns() throws Exception {
        Document doc = Document.newBuilder()
                .setDocId("blank-keys")
                .setSearchMetadata(ai.pipestream.proto.repo.v1.SearchMetadata.newBuilder()
                        .addSemanticResults(result("", "c1"))
                        .addSemanticResults(result("", "c2"))
                        .addSemanticResults(result("named", "c3")))
                .build();

        List<PartObject> parts = DocumentPartCodec.split(doc, LAYOUT);
        Document assembled = DocumentPartCodec.assemble(
                parts.stream().map(PartObject::bytes).toList(), Document.getDefaultInstance());

        assertThat(assembled.toByteArray()).isEqualTo(doc.toByteArray());
        assertThat(parts.stream().filter(p -> p.part() == DocumentPart.DOCUMENT_PART_CHUNKS)
                .map(PartObject::subKey))
                .containsExactly("set-0", "set-1", "named");
    }

    @Test
    void rootChecksum_isStableAndMatchesManifestDerivation() {
        Document doc = fullDoc();
        List<PartObject> parts = DocumentPartCodec.split(doc, LAYOUT);
        String root = DocumentPartCodec.rootChecksum(parts);

        // Same doc splits to the same root (dedupe invariant).
        assertThat(DocumentPartCodec.rootChecksum(DocumentPartCodec.split(doc, LAYOUT))).isEqualTo(root);

        // The manifest-derived root (partial-save path) matches the split-derived one.
        DocumentManifest.Builder m = DocumentManifest.newBuilder().setDocId(doc.getDocId());
        for (PartObject p : parts) {
            m.addParts(PartManifestEntry.newBuilder()
                    .setPart(p.part())
                    .setState(PartState.PART_STATE_PRESENT)
                    .setSubKey(p.subKey())
                    .setSha256(p.sha256()));
        }
        assertThat(DocumentPartCodec.rootChecksumFromManifest(m.build())).isEqualTo(root);
    }

    @Test
    void rootChecksum_changesWhenAChunkChanges() {
        String root = DocumentPartCodec.rootChecksum(DocumentPartCodec.split(fullDoc(), LAYOUT));

        Document changed = fullDoc().toBuilder()
                .setSearchMetadata(fullDoc().getSearchMetadata().toBuilder()
                        .setSemanticResults(0, result("title-384", "c1-changed")))
                .build();

        assertThat(DocumentPartCodec.rootChecksum(DocumentPartCodec.split(changed, LAYOUT)))
                .isNotEqualTo(root);
    }

    @Test
    void manifestJson_roundTrips() {
        DocumentManifest manifest = DocumentManifest.newBuilder()
                .setDocId("d1").setGraphAddressId("a1").setAccountId("acct").setDocVersion(3)
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setSubKey("title-384").setSha256("ab").setObjectKey("x/chunks/title-384-i.pb"))
                .build();
        String json = DocumentPartCodec.manifestToJson(manifest);
        assertThat(DocumentPartCodec.manifestFromJson(json)).isEqualTo(manifest);
    }

    @Test
    void objectKey_mapsEachPartToItsPath() {
        assertThat(DocumentPartCodec.objectKey("pre", DocumentPart.DOCUMENT_PART_CORE, ""))
                .isEqualTo("pre/core.pb");
        assertThat(DocumentPartCodec.objectKey("pre", DocumentPart.DOCUMENT_PART_BLOBS, ""))
                .isEqualTo("pre/blobs.pb");
        assertThat(DocumentPartCodec.objectKey("pre", DocumentPart.DOCUMENT_PART_PARSED, ""))
                .isEqualTo("pre/parsed.pb");
        assertThat(DocumentPartCodec.objectKey("pre", DocumentPart.DOCUMENT_PART_CHUNKS, "title-384"))
                .isEqualTo("pre/chunks/" + DocumentPartCodec.chunkFileName("title-384") + ".pb");
        assertThatThrownBy(() -> DocumentPartCodec.objectKey("pre", DocumentPart.DOCUMENT_PART_UNSPECIFIED, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void chunkFileName_isSafeAndCollisionFree() {
        String a = DocumentPartCodec.chunkFileName("a b");
        String b = DocumentPartCodec.chunkFileName("a!b");
        assertThat(a).matches("[A-Za-z0-9._-]+");
        assertThat(a).startsWith("a_b-");
        assertThat(a).isNotEqualTo(b); // sanitizes identically, hash suffix differs

        assertThat(DocumentPartCodec.chunkFileName("   ")).startsWith("set-");
        assertThat(DocumentPartCodec.chunkFileName("x".repeat(200)).length()).isLessThanOrEqualTo(96 + 1 + 8);
    }

    /**
     * Genericity proof: a layout over the same Document type but mapping ONLY
     * blob_bag to BLOBS leaves parsed_metadata and semantic_results in CORE —
     * CORE is always the complement of the mapped fields, for any mapping.
     */
    @Test
    void split_blobsOnlyLayout_leavesEverythingElseInCore() throws Exception {
        PartLayout blobsOnly = PartLayout.builder(Document.getDescriptor())
                .identityField("doc_id")
                .partField(DocumentPart.DOCUMENT_PART_BLOBS, "blob_bag")
                .build();
        Document doc = fullDoc();

        List<PartObject> parts = DocumentPartCodec.split(doc, blobsOnly);

        assertThat(parts.stream().map(PartObject::part))
                .containsExactly(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS);

        Document core = Document.parseFrom(parts.get(0).bytes());
        assertThat(core.getParsedMetadataCount()).isEqualTo(1); // unmapped -> CORE
        assertThat(core.getSearchMetadata().getSemanticResultsCount()).isEqualTo(4); // unmapped -> CORE
        assertThat(core.hasBlobBag()).isFalse();

        Document assembled = DocumentPartCodec.assemble(
                parts.stream().map(PartObject::bytes).toList(), Document.getDefaultInstance());
        assertThat(assembled.toByteArray()).isEqualTo(doc.toByteArray());
    }

    @Test
    void layout_rejectsUnknownFieldsAndBadPaths() {
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor()).identityField("nope"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .partField(DocumentPart.DOCUMENT_PART_BLOBS, "search_metadata.title"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .chunkedPart(DocumentPart.DOCUMENT_PART_CHUNKS, "semantic_results", "result_id"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .chunkedPart(DocumentPart.DOCUMENT_PART_CHUNKS,
                        "search_metadata.semantic_results", "no_such_key"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------------

    private static Document fullDoc() {
        return Document.newBuilder()
                .setDocId("synthetic-1")
                .setSearchMetadata(ai.pipestream.proto.repo.v1.SearchMetadata.newBuilder()
                        .setTitle("A doc")
                        // Two consecutive runs of one key, then a non-consecutive duplicate.
                        .addSemanticResults(result("title-384", "c1"))
                        .addSemanticResults(result("title-384", "c2"))
                        .addSemanticResults(result("body-768", "c3"))
                        .addSemanticResults(result("title-384", "c4")))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("b1").setFilename("f.pdf").setMimeType("application/pdf")
                        .setData(ByteString.copyFromUtf8("pdf-bytes"))))
                .putParsedMetadata("tika", ParsedMetadata.newBuilder()
                        .setParserName("tika")
                        .setData(Any.pack(StringValue.of("tika-exhaust"))).build())
                .setStructuredData(Any.pack(StringValue.of("customer-data")))
                .build();
    }

    private static SemanticProcessingResult result(String resultId, String chunkId) {
        return SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .addChunks(SemanticChunk.newBuilder().setChunkId(chunkId).setText("text of " + chunkId))
                .build();
    }
}

package ai.protomolt.proto.repo.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Keyed CHUNKS (segmentations) and EMBEDDINGS fragments: split, assemble and confinement. */
class SemanticPartsCodecTest {
    private static final PartLayout LAYOUT = PartLayouts.document();

    @Test void splitsOneObjectPerSegmentationAndEmbeddingSetInCodecOrder() throws Exception {
        var doc = document();
        var parts = DocumentPartCodec.split(doc, LAYOUT);
        assertThat(parts.stream().map(PartObject::part)).containsExactly(DocumentPart.DOCUMENT_PART_CORE,
                DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_CHUNKS,
                DocumentPart.DOCUMENT_PART_EMBEDDINGS, DocumentPart.DOCUMENT_PART_EMBEDDINGS);
        assertThat(parts.stream().map(PartObject::keyValues)).containsExactly(List.of(), List.of("s/1"), List.of("s2"),
                List.of("s/1", "m1"), List.of("s2", "m1"));
        assertThat(parts.get(1).subKey()).isEqualTo("s/1");
        assertThat(parts.get(3).subKey()).isEqualTo("s/1/m1");
        assertThat(parts.get(4).keyValues()).as("the key, not the joined sub key, is the identity")
                .isEqualTo(List.of("s2", "m1"));

        var core = Document.parseFrom(parts.get(0).bytes());
        assertThat(core.getSearchMetadata().getTitle()).isEqualTo("t");
        assertThat(core.getSearchMetadata().getSegmentationsCount()).isZero();
        assertThat(core.getSearchMetadata().getEmbeddingSetsCount()).isZero();
        var chunks = Document.parseFrom(parts.get(1).bytes());
        assertThat(chunks.getDocId()).isEqualTo("d");
        assertThat(chunks.getSearchMetadata().getSegmentationsList()).containsExactly(doc.getSearchMetadata().getSegmentations(0));
        assertThat(chunks.getSearchMetadata().hasTitle()).isFalse();
        var embeddings = Document.parseFrom(parts.get(4).bytes());
        assertThat(embeddings.getSearchMetadata().getEmbeddingSetsList()).containsExactly(doc.getSearchMetadata().getEmbeddingSets(1));

        var assembled = DocumentPartCodec.assemble(parts.stream().map(PartObject::bytes).toList(), Document.getDefaultInstance());
        assertThat(assembled.toByteArray()).isEqualTo(doc.toByteArray());
        for (var part : parts) {
            var deterministic = DocumentPartCodec.deterministic(part, Document.getDefaultInstance());
            assertThat(deterministic.keyValues()).isEqualTo(part.keyValues());
            assertThat(deterministic.part()).isEqualTo(part.part());
        }
    }

    @Test void duplicateKeysAreRefused() {
        var doc = document().toBuilder();
        doc.getSearchMetadataBuilder().addSegmentations(doc.getSearchMetadata().getSegmentations(1));
        assertThatThrownBy(() -> DocumentPartCodec.split(doc.build(), LAYOUT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("segmentations").hasMessageContaining("s2");
        var sets = document().toBuilder();
        sets.getSearchMetadataBuilder().addEmbeddingSets(sets.getSearchMetadata().getEmbeddingSets(0));
        assertThatThrownBy(() -> DocumentPartCodec.split(sets.build(), LAYOUT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding_sets");
    }

    @Test void confinementAcceptsExactlyTheSelectedObject() throws Exception {
        var parts = DocumentPartCodec.split(document(), LAYOUT);
        var chunks = Document.parseFrom(parts.get(1).bytes());
        var embeddings = Document.parseFrom(parts.get(3).bytes());
        DocumentFragmentConfinement.requireConfinedSegmentation(chunks, "s/1", "d");
        DocumentFragmentConfinement.requireConfinedEmbeddingSet(embeddings, "s/1", "m1", "d");

        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedSegmentation(chunks, "s2", "d"))
                .hasMessageContaining("differs from its selector");
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedSegmentation(chunks, "s/1", "other"))
                .hasMessageContaining("identity");
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedSegmentation(embeddings, "s/1", "d"))
                .hasMessageContaining("unowned");
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedEmbeddingSet(embeddings, "s/1", "m2", "d"))
                .hasMessageContaining("embedding_set_id differs");
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedEmbeddingSet(embeddings, "s/1", " ", "d"))
                .hasMessageContaining("blank");
        var two = chunks.toBuilder();
        two.getSearchMetadataBuilder().addSegmentations(chunks.getSearchMetadata().getSegmentations(0).toBuilder().setSegmentationId("x"));
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedSegmentation(two.build(), "s/1", "d"))
                .hasMessageContaining("exactly one");
        var titled = chunks.toBuilder();
        titled.getSearchMetadataBuilder().setTitle("leak");
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfinedSegmentation(titled.build(), "s/1", "d"))
                .hasMessageContaining("unowned");
        // The legacy CHUNKS check does not accept a segmentation object.
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(chunks, DocumentPart.DOCUMENT_PART_CHUNKS, "d"))
                .hasMessageContaining("unowned fields");
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(embeddings, DocumentPart.DOCUMENT_PART_EMBEDDINGS, "d"))
                .hasMessageContaining("Unsupported");
    }

    @Test void coreMayNotCarrySegmentationsOrEmbeddingSets() {
        var withSegmentation = Document.newBuilder().setDocId("d").setSearchMetadata(SearchMetadata.newBuilder()
                .addSegmentations(Segmentation.newBuilder().setSegmentationId("s"))).build();
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(withSegmentation, DocumentPart.DOCUMENT_PART_CORE, "d"))
                .hasMessageContaining("owned by another part");
        var withSet = Document.newBuilder().setDocId("d").setSearchMetadata(SearchMetadata.newBuilder()
                .addEmbeddingSets(EmbeddingSet.newBuilder().setEmbeddingSetId("m"))).build();
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(withSet, DocumentPart.DOCUMENT_PART_CORE, "d"))
                .hasMessageContaining("owned by another part");
        var plain = Document.newBuilder().setDocId("d").setSearchMetadata(SearchMetadata.newBuilder().setTitle("t")).build();
        DocumentFragmentConfinement.requireConfined(plain, DocumentPart.DOCUMENT_PART_CORE, "d");
    }

    @Test void layoutRefusesNonScalarKeys() {
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .keyedPart(DocumentPart.DOCUMENT_PART_CHUNKS, "search_metadata.segmentations", "source"))
                .hasMessageContaining("scalar");
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .keyedPart(DocumentPart.DOCUMENT_PART_CHUNKS, "search_metadata.segmentations"))
                .hasMessageContaining("at least one key");
    }

    static Document document() {
        var s1 = segmentation("s/1");
        var s2 = segmentation("s2");
        return Document.newBuilder().setDocId("d").setSearchMetadata(SearchMetadata.newBuilder().setTitle("t")
                .addSegmentations(s1).addSegmentations(s2)
                .addEmbeddingSets(embeddingSet("s/1", "m1")).addEmbeddingSets(embeddingSet("s2", "m1"))).build();
    }

    static Segmentation segmentation(String id) {
        return Segmentation.newBuilder().setSegmentationId(id).setKind(SegmentationKind.SEGMENTATION_KIND_SENTENCE)
                .setSource(TextSource.newBuilder().setFieldPath("search_metadata.body")
                        .setBinding(SourceTextBinding.newBuilder().setResolvedTextSha256("0".repeat(64)).setUtf8Length(1)))
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .addSegments(Segment.newBuilder().setSegmentId("a").setText("x").setStart(0).setEnd(1)).build();
    }

    static EmbeddingSet embeddingSet(String segmentation, String id) {
        return EmbeddingSet.newBuilder().setEmbeddingSetId(id).setSegmentationId(segmentation)
                .setSegmentationDigest("1".repeat(64)).setModel("m").setDimensions(2)
                .setSimilarity(Similarity.SIMILARITY_COSINE)
                .addVectors(SegmentVector.newBuilder().setSegmentId("a").addValues(0.5f).addValues(1f)).build();
    }
}

package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentFragmentConfinementTest {
    private static final UnknownFieldSet UNKNOWN = UnknownFieldSet.newBuilder()
            .addField(1000, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    private static Document chunks() {
        return Document.newBuilder().setDocId("doc").setSearchMetadata(SearchMetadata.newBuilder()
                .addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("run"))).build();
    }
    @Test void actualSplitFragmentsRespectTheirSlots() throws Exception {
        var document = chunks().toBuilder().setBlobBag(BlobBag.getDefaultInstance())
                .setSearchMetadata(chunks().getSearchMetadata().toBuilder().setUnknownFields(UNKNOWN))
                .putParserResults("parser", ParserResult.getDefaultInstance()).setUnknownFields(UNKNOWN).build();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            var parsed = Document.parseFrom(part.bytes());
            assertThatCode(() -> DocumentFragmentConfinement.requireConfined(parsed, part.part(), "doc")).doesNotThrowAnyException();
        }
    }
    @Test void rejectsContentInTheWrongSlotBeforeMerge() {
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(chunks(), DocumentPart.DOCUMENT_PART_CORE, "doc"))
                .isInstanceOf(IllegalArgumentException.class);
        var wrongOwner = chunks().toBuilder().setOwnership(OwnershipContext.newBuilder().setAccountId("forged")).build();
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(wrongOwner, DocumentPart.DOCUMENT_PART_CHUNKS, "doc"))
                .isInstanceOf(IllegalArgumentException.class);
        var wrongTitle = chunks().toBuilder().setSearchMetadata(chunks().getSearchMetadata().toBuilder().setTitle("injected")).build();
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(wrongTitle, DocumentPart.DOCUMENT_PART_CHUNKS, "doc"))
                .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void unknownFieldsMustBelongToTheOwnedSubtree() {
        var root = chunks().toBuilder().setUnknownFields(UNKNOWN).build();
        var parent = chunks().toBuilder().setSearchMetadata(chunks().getSearchMetadata().toBuilder().setUnknownFields(UNKNOWN)).build();
        for (var invalid : java.util.List.of(root, parent))
            assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(invalid, DocumentPart.DOCUMENT_PART_CHUNKS, "doc"))
                    .isInstanceOf(IllegalArgumentException.class);
        var owned = chunks().toBuilder().setSearchMetadata(chunks().getSearchMetadata().toBuilder()
                .setSemanticResults(0, chunks().getSearchMetadata().getSemanticResults(0).toBuilder().setUnknownFields(UNKNOWN))).build();
        assertThatCode(() -> DocumentFragmentConfinement.requireConfined(owned, DocumentPart.DOCUMENT_PART_CHUNKS, "doc")).doesNotThrowAnyException();
        assertThat(owned.getSearchMetadata().getSemanticResults(0).getUnknownFields()).isEqualTo(UNKNOWN);
    }
    @Test void mismatchedIdentityAndMissingPresentContentAreRejected() {
        assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(chunks(), DocumentPart.DOCUMENT_PART_CHUNKS, "other"))
                .isInstanceOf(IllegalArgumentException.class);
        var empty = Document.newBuilder().setDocId("doc").build();
        for (var part : java.util.List.of(DocumentPart.DOCUMENT_PART_BLOBS, DocumentPart.DOCUMENT_PART_PARSED, DocumentPart.DOCUMENT_PART_CHUNKS))
            assertThatThrownBy(() -> DocumentFragmentConfinement.requireConfined(empty, part, "doc"))
                    .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void unknownFieldsWithinBlobAndParserPayloadsRemainAllowed() {
        var blobs = Document.newBuilder().setDocId("doc")
                .setBlobBag(BlobBag.newBuilder().setUnknownFields(UNKNOWN)).build();
        var parsed = Document.newBuilder().setDocId("doc")
                .putParserResults("parser", ParserResult.newBuilder().setUnknownFields(UNKNOWN).build()).build();
        DocumentFragmentConfinement.requireConfined(blobs, DocumentPart.DOCUMENT_PART_BLOBS, "doc");
        DocumentFragmentConfinement.requireConfined(parsed, DocumentPart.DOCUMENT_PART_PARSED, "doc");
        assertThat(blobs.getBlobBag().getUnknownFields()).isEqualTo(UNKNOWN);
        assertThat(parsed.getParserResultsOrThrow("parser").getUnknownFields()).isEqualTo(UNKNOWN);
    }
    @Test void noncanonicalWireEncodingIsNotNormalizedOrRejected() throws Exception {
        // Unknown varint field 1000 precedes doc_id; its value uses a legal overlong encoding.
        byte[] wire = {(byte) 0xc0, 0x3e, (byte) 0x81, 0, 0x0a, 3, 'd', 'o', 'c'};
        var before = wire.clone();
        var parsed = Document.parseFrom(wire);
        DocumentFragmentConfinement.requireConfined(parsed, DocumentPart.DOCUMENT_PART_CORE, "doc");
        assertThat(wire).containsExactly(before);
        assertThat(parsed.toByteArray()).isNotEqualTo(wire);
        assertThat(parsed.getUnknownFields()).isEqualTo(UNKNOWN);
    }
}

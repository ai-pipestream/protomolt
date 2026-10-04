package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentRevisionAssemblyTest {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(1_000_000, 100, 100, 100);
    private static DocumentRevisionAssembly.Fragment fragment(DocumentPart part, String key, Document doc) {
        return new DocumentRevisionAssembly.Fragment(part, key, doc.toByteString());
    }
    private static Document core() { return Document.newBuilder().setDocId("doc").build(); }
    private static List<DocumentRevisionAssembly.Fragment> split(Document document) {
        return DocumentPartCodec.split(document, PartLayouts.document()).stream()
                .map(p -> new DocumentRevisionAssembly.Fragment(p.part(), p.subKey(), ByteString.copyFrom(p.bytes()))).toList();
    }
    @Test void completeAssemblyRetainsOriginalObjectsAndSemanticContent() throws Exception {
        var full = core().toBuilder().setBlobBag(BlobBag.getDefaultInstance())
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Title")
                        .addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("a"))
                        .addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("b")))
                .putParserResults("p", ParserResult.getDefaultInstance()).build();
        var parts = split(full);
        var result = DocumentRevisionAssembly.assemble(parts, "doc", LIMITS, () -> {});
        assertThat(result.document()).isEqualTo(full);
        assertThat(result.fragments()).containsExactlyElementsOf(parts);
        assertThat(result.fragments().getFirst().bytes()).isSameAs(parts.getFirst().bytes());
    }
    @Test void aggregateLimitIsCheckedBeforeParsingAndExactLimitPasses() throws Exception {
        var parts = split(core());
        int size = parts.getFirst().bytes().size();
        assertThat(DocumentRevisionAssembly.assemble(parts, "doc", new DocumentRevisionAssembly.Limits(size, 1, 10, 1), () -> {}).document()).isEqualTo(core());
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(parts, "doc", new DocumentRevisionAssembly.Limits(size - 1, 1, 10, 1), () -> {}))
                .hasMessageContaining("byte bound");
        var invalid = new DocumentRevisionAssembly.Fragment(DocumentPart.DOCUMENT_PART_CORE, "", ByteString.copyFrom(new byte[]{(byte) 0x80, (byte) 0x80}));
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(List.of(invalid), "doc", new DocumentRevisionAssembly.Limits(1, 1, 10, 1), () -> {}))
                .hasMessageContaining("byte bound");
    }
    @Test void rejectsDuplicateSlotsMissingCoreAndMisplacedFields() {
        var c = fragment(DocumentPart.DOCUMENT_PART_CORE, "", core());
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(List.of(c, c), "doc", LIMITS, () -> {})).hasMessageContaining("Duplicate");
        var bad = fragment(DocumentPart.DOCUMENT_PART_CHUNKS, "a", core());
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(List.of(bad), "doc", LIMITS, () -> {})).hasMessageContaining("CORE");
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(List.of(c, bad), "doc", LIMITS, () -> {})).hasMessageContaining("no entries");
    }
    @Test void retainsNoncanonicalWireBytesAndSupportsCoreOnlyRevision() throws Exception {
        var bytes = ByteString.copyFrom(new byte[]{(byte) 0xc0, 0x3e, (byte) 0x81, 0, 0x0a, 3, 'd', 'o', 'c'});
        var result = DocumentRevisionAssembly.assemble(List.of(new DocumentRevisionAssembly.Fragment(DocumentPart.DOCUMENT_PART_CORE, "", bytes)), "doc", LIMITS, () -> {});
        assertThat(result.fragments().getFirst().bytes()).isSameAs(bytes);
        assertThat(result.document().toByteString()).isNotEqualTo(bytes);
    }
    @Test void cancellationAndMalformedBytesCannotProduceAnAssembly() {
        var cancellation = new java.util.concurrent.CancellationException("caller cancelled");
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(split(core()), "doc", LIMITS, () -> { throw cancellation; })).isSameAs(cancellation);
        var malformed = new DocumentRevisionAssembly.Fragment(DocumentPart.DOCUMENT_PART_CORE, "", ByteString.copyFrom(new byte[]{(byte) 0x80}));
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(List.of(malformed), "doc", LIMITS, () -> {}))
                .isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
    }
    @Test void parsingDepthIsEnforced() {
        var nested = core().toBuilder().setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("b"))).build();
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(split(nested), "doc", new DocumentRevisionAssembly.Limits(1000, 10, 1, 10), () -> {}))
                .isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
    }
    @Test void endGroupCannotHideTrailingFragmentFields() {
        var wire = core().toByteString().concat(ByteString.copyFrom(new byte[]{0x0c}))
                .concat(Document.newBuilder().setDocId("other").build().toByteString());
        var part = new DocumentRevisionAssembly.Fragment(DocumentPart.DOCUMENT_PART_CORE, "", wire);
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(List.of(part), "doc", LIMITS, () -> {}))
                .isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
    }
    @Test void fragmentAndChunkBoundsApplyToTheWholeAssembly() throws Exception {
        var full = core().toBuilder().setSearchMetadata(SearchMetadata.newBuilder()
                .addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("a"))
                .addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("a"))).build();
        var parts = split(full);
        assertThat(DocumentRevisionAssembly.assemble(parts, "doc", new DocumentRevisionAssembly.Limits(1000, 2, 10, 2), () -> {}).document()).isEqualTo(full);
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(parts, "doc", new DocumentRevisionAssembly.Limits(1000, 1, 10, 2), () -> {}))
                .hasMessageContaining("fragment count");
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(parts, "doc", new DocumentRevisionAssembly.Limits(1000, 2, 10, 1), () -> {}))
                .hasMessageContaining("element bound");
        assertThatThrownBy(() -> DocumentRevisionAssembly.assemble(parts, "other", LIMITS, () -> {}))
                .hasMessageContaining("identity");
    }
}

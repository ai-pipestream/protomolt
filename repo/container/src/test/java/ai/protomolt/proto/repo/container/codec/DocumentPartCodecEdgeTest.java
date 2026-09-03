package ai.protomolt.proto.repo.container.codec;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DocumentPartCodec}/{@link PartLayout} failure and edge paths the
 * round-trip gate does not exercise: layout/document type mismatch, corrupt
 * fragment bytes, manifest JSON failure modes, the manifest-derived root's
 * PRESENT-only rule, layout immutability, and the SHA-256 hex primitive.
 */
class DocumentPartCodecEdgeTest {

    private static final PartLayout LAYOUT = PartLayouts.document();

    @Test
    void splitRejectsADocumentOfTheWrongMessageType() {
        NodeAddress notADocument = NodeAddress.newBuilder().setDocId("d1").build();

        assertThatThrownBy(() -> DocumentPartCodec.split(notADocument, LAYOUT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ai.protomolt.proto.repo.v1.Document")
                .hasMessageContaining("ai.protomolt.proto.repo.v1.NodeAddress");
    }

    @Test
    void assembleRejectsCorruptFragmentBytes() {
        List<byte[]> garbage = List.of("not a protobuf frame".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> DocumentPartCodec.assemble(garbage, Document.getDefaultInstance()))
                .isInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void assembleOfNoFragmentsIsTheDefaultInstance() throws Exception {
        Document assembled = DocumentPartCodec.assemble(List.of(), Document.getDefaultInstance());

        assertThat(assembled).isEqualTo(Document.getDefaultInstance());
    }

    @Test
    void manifestFromJsonRejectsGarbage() {
        assertThatThrownBy(() -> DocumentPartCodec.manifestFromJson("{ broken json"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Manifest JSON parse failed");
    }

    @Test
    void manifestFromJsonToleratesUnknownFields() {
        // Stored JSON written by a newer schema must stay readable.
        DocumentManifest manifest = DocumentPartCodec.manifestFromJson(
                "{\"docVersion\": 2, \"someFutureField\": {\"nested\": true}}");

        assertThat(manifest.getDocVersion()).isEqualTo(2);
    }

    @Test
    void rootChecksumFromManifestHashesOnlyThePresentEntries() {
        PartObject core = new PartObject(DocumentPart.DOCUMENT_PART_CORE, "", new byte[0], "aa");
        String expected = DocumentPartCodec.rootChecksum(List.of(core));

        DocumentManifest presentOnly = DocumentManifest.newBuilder()
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setSubKey("").setSha256("aa"))
                .build();
        assertThat(DocumentPartCodec.rootChecksumFromManifest(presentOnly)).isEqualTo(expected);

        // DELETED/EMPTY entries (tombstones, never-written parts) carry stored
        // hashes but must not move the root: they are not part of the body.
        DocumentManifest withNonPresent = presentOnly.toBuilder()
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                        .setState(PartState.PART_STATE_DELETED)
                        .setSubKey("set-1").setSha256("bb"))
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_BLOBS)
                        .setState(PartState.PART_STATE_EMPTY)
                        .setSubKey("").setSha256(""))
                .build();
        assertThat(DocumentPartCodec.rootChecksumFromManifest(withNonPresent)).isEqualTo(expected);
    }

    @Test
    void aBuiltLayoutIsImmutable() {
        PartLayout layout = PartLayout.builder(Document.getDescriptor())
                .identityField("doc_id")
                .partField(DocumentPart.DOCUMENT_PART_BLOBS, "blob_bag")
                .build();

        assertThatThrownBy(() -> layout.partFields().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> layout.chunkedFields().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void chunkedPartRejectsANonMessageParentAndANonRepeatedTarget() {
        // parser_results is a map field (repeated entries): not a singular message parent.
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .chunkedPart(DocumentPart.DOCUMENT_PART_CHUNKS, "parser_results.x", "result_id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a singular message field");

        // search_metadata.title exists but is a singular string, not a repeated message.
        assertThatThrownBy(() -> PartLayout.builder(Document.getDescriptor())
                .chunkedPart(DocumentPart.DOCUMENT_PART_CHUNKS, "search_metadata.title", "result_id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a repeated message field");
    }

    @Test
    void sha256HexMatchesTheReferenceVectors() {
        assertThat(DocumentPartCodec.sha256Hex(new byte[0]))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertThat(DocumentPartCodec.sha256Hex("abc".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }
}

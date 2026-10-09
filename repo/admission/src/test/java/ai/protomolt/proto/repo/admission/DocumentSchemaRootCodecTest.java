package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentSchemaRootCodecTest {
    private static final DocumentSchemaBinding CONTAINER = bind(Document.getDescriptor());
    private static final DocumentAnyRootInventory.Limits INVENTORY_LIMITS =
            new DocumentAnyRootInventory.Limits(1_000_000, 100, 30, 10, 10);

    @Test
    void roundTripsGeneratedCoreAndParsedRootLocatorsWithStableBytesAndDigest() throws Exception {
        for (var root : fixtures()) {
            var encoded = DocumentSchemaRootCodec.encode(root, () -> {});
            assertThat(encoded.bytes()).isEqualTo(root.toByteString());
            assertThat(encoded.sha256()).isEqualTo(sha256(encoded.bytes()));
            assertThat(DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC,
                    DocumentSchemaRootCodec.VERSION, encoded.bytes(), encoded.sha256(), () -> {})).isEqualTo(root);
        }
        assertThat(fixtures().getFirst().getSlot().getPart()).isEqualTo(DocumentPart.DOCUMENT_PART_CORE);
        assertThat(fixtures().getLast().getSlot().getPart()).isEqualTo(DocumentPart.DOCUMENT_PART_PARSED);
    }

    @Test
    void rejectsSemanticallyValidButNoncanonicalDuplicateOverlongAndReorderedWire() throws Exception {
        var root = fixtures().getFirst();
        var canonical = DocumentSchemaRootCodec.encode(root, () -> {}).bytes();
        var duplicate = canonical.concat(ByteString.copyFrom(new byte[]{8, 1}));
        var overlong = ByteString.copyFrom(new byte[]{8, (byte) 0x81, 0})
                .concat(canonical.substring(2));
        var reordered = reordered(root);
        for (var bytes : List.of(duplicate, overlong, reordered)) {
            assertThat(DocumentSchemaRootLocator.parseFrom(bytes)).isEqualTo(root);
            assertThatThrownBy(() -> DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC,
                    DocumentSchemaRootCodec.VERSION, bytes, sha256(bytes), () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("noncanonical");
        }
    }

    @Test
    void rejectsUnknownFieldsAtRootAndNestedMessages() throws Exception {
        var root = fixtures().getFirst();
        var rootUnknown = DocumentSchemaRootLocator.parseFrom(root.toByteString()
                .concat(ByteString.copyFrom(new byte[]{(byte) 0xa0, 0x06, 0x01})));
        assertThatThrownBy(() -> decode(rootUnknown)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown schema evidence fields");

        var originalStep = root.getAccess(0);
        var nestedUnknown = originalStep.toBuilder().setUnknownFields(unknownFields()).build();
        var nested = root.toBuilder().setAccess(0, nestedUnknown).build();
        assertThatThrownBy(() -> decode(nested)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown schema evidence fields");
    }

    @Test
    void rejectsWrongCodecVersionDigestAndInvalidLayoutOrPath() throws Exception {
        var root = fixtures().getFirst();
        var encoded = DocumentSchemaRootCodec.encode(root, () -> {});
        assertThatThrownBy(() -> DocumentSchemaRootCodec.decode("other-codec", 1, encoded.bytes(), encoded.sha256(), () -> {}))
                .hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC, 2,
                encoded.bytes(), encoded.sha256(), () -> {})).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC, 1,
                encoded.bytes(), "0".repeat(64), () -> {})).hasMessageContaining("digest mismatch");

        assertThatThrownBy(() -> DocumentSchemaEvidenceCodec.decode(DocumentSchemaRootCodec.CODEC,
                DocumentSchemaRootCodec.CODEC, DocumentSchemaRootCodec.VERSION, encoded.bytes(), encoded.sha256(),
                DocumentSchemaRootLocator.getDescriptor(), com.google.protobuf.Any.parser(), () -> {}))
                .hasMessageContaining("parser differs from bounded descriptor");

        var wrongLayout = root.toBuilder().setLayoutPolicy("other-layout").build();
        assertThatThrownBy(() -> decode(wrongLayout)).isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
        var wrongPath = root.toBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_PARSED)).build();
        assertThatThrownBy(() -> decode(wrongPath)).isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class);
    }

    @Test
    void enforcesByteLimitAndPropagatesCancellation() throws Exception {
        var overLimit = ByteString.copyFrom(new byte[DocumentSchemaRootCodec.MAX_BYTES + 1]);
        assertThatThrownBy(() -> DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC,
                DocumentSchemaRootCodec.VERSION, overLimit, "0".repeat(64), () -> {}))
                .hasMessageContaining("byte bound");

        var root = fixtures().getFirst();
        var encoded = DocumentSchemaRootCodec.encode(root, () -> {});
        var stopped = new CancellationException("test cancelled");
        assertThatThrownBy(() -> DocumentSchemaRootCodec.encode(root, () -> { throw stopped; })).isSameAs(stopped);
        assertThatThrownBy(() -> DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC,
                DocumentSchemaRootCodec.VERSION, encoded.bytes(), encoded.sha256(), () -> { throw stopped; }))
                .isSameAs(stopped);
    }

    private static DocumentSchemaRootLocator decode(DocumentSchemaRootLocator root) throws Exception {
        var bytes = root.toByteString();
        return DocumentSchemaRootCodec.decode(DocumentSchemaRootCodec.CODEC, DocumentSchemaRootCodec.VERSION,
                bytes, sha256(bytes), () -> {});
    }

    private static List<DocumentSchemaRootLocator> fixtures() throws Exception {
        var document = Document.newBuilder().setDocId("doc")
                .setStructuredData(com.google.protobuf.Any.newBuilder().setTypeUrl("fixture/opaque.Root"))
                .putParserResults("shape-a", ParserResult.newBuilder().setDocument(
                        ParserDocument.newBuilder().setShape(com.google.protobuf.Any.newBuilder().setTypeUrl("fixture/opaque.Root"))).build())
                .build();
        var roots = new java.util.ArrayList<DocumentSchemaRootLocator>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            var bytes = ByteString.copyFrom(part.bytes());
            var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).build();
            var inventory = DocumentAnyRootInventory.inspect(CONTAINER, slot, bytes, "doc", INVENTORY_LIMITS, () -> {});
            for (var root : inventory.roots()) roots.add(DocumentSchemaRootProjection.project(inventory, root, () -> {}));
        }
        return List.copyOf(roots);
    }

    private static ByteString reordered(DocumentSchemaRootLocator root) throws Exception {
        var bytes = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(bytes);
        output.writeUInt64(6, root.getFragmentSizeBytes());
        output.writeString(5, root.getFragmentSha256());
        output.writeMessage(4, root.getContainerSchema());
        for (var step : root.getAccessList()) output.writeMessage(7, step);
        output.writeString(3, root.getLayoutPolicy());
        output.writeMessage(2, root.getSlot());
        output.writeUInt32(1, root.getEncodingVersion());
        output.flush();
        return ByteString.copyFrom(bytes.toByteArray());
    }

    private static UnknownFieldSet unknownFields() {
        return UnknownFieldSet.newBuilder().addField(1000,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    }

    private static String sha256(ByteString bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }

    private static DocumentSchemaBinding bind(com.google.protobuf.Descriptors.Descriptor type) {
        var closure = DescriptorFingerprints.closure(type);
        return DocumentSchemaBinding.bind(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build(), closure.toByteString(),
                new ClosedDescriptorSet.Limits(16 * 1024 * 1024, 1024, 10000, 100), () -> {});
    }
}

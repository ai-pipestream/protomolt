package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentAnyRootInventoryTest {
    private static final DocumentSchemaBinding CONTAINER = bind(Document.getDescriptor());
    private static final DocumentAnyRootInventory.Limits LIMITS = new DocumentAnyRootInventory.Limits(100000, 1000, 30, 10, 10);
    private static final Any OPAQUE = Any.newBuilder().setTypeUrl("missing/unknown.Record")
            .setValue(ByteString.copyFrom(new byte[]{(byte) 0xff})).build();

    @Test void inventoriesActualSplitPartsWithoutResolvingOpaquePayloads() throws Exception {
        var document = Document.newBuilder().setDocId("doc").setStructuredData(OPAQUE)
                .putParserResults("one", parser()).putParserResults("two", parser()).build();
        for (var fragment : DocumentPartCodec.split(document, PartLayouts.document())) {
            var bytes = ByteString.copyFrom(fragment.bytes());
            var result = inspect(fragment.part(), bytes, LIMITS);
            assertThat(result.original()).isSameAs(bytes);
            assertThat(result.containerSchema()).isSameAs(CONTAINER);
            assertThat(result.layoutPolicy()).isEqualTo("protomolt-document-parts/v1");
            assertThat(result.fragmentSha256()).isEqualTo(DocumentPartCodec.sha256Hex(fragment.bytes()));
            assertThat(result.roots()).hasSize(fragment.part() == DocumentPart.DOCUMENT_PART_CORE ? 1 : 2);
            for (var root : result.roots()) {
                assertThat(root.envelope()).isEqualTo(OPAQUE);
                if (fragment.part() == DocumentPart.DOCUMENT_PART_CORE)
                    assertThat(root.access()).containsExactly(new DocumentSchemaOccurrences.Field(4));
                else {
                    assertThat(root.access()).hasSize(4);
                    assertThat(root.access().get(0)).isEqualTo(new DocumentSchemaOccurrences.Field(5));
                    assertThat(((DocumentSchemaOccurrences.MapKey) root.access().get(1)).value()).isIn("one", "two");
                    assertThat(root.access().subList(2, 4)).containsExactly(new DocumentSchemaOccurrences.Field(7), new DocumentSchemaOccurrences.Field(1));
                }
            }
            assertThatThrownBy(() -> result.roots().clear()).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test void rejectsDuplicateParserKeysBeforeGeneratedMapCollapse() throws Exception {
        var field = Document.getDescriptor().findFieldByNumber(5);
        for (String key : List.of("same", "")) {
            for (var result : List.of(parser(), ParserResult.getDefaultInstance())) {
                var entry = DynamicMessage.newBuilder(field.getMessageType())
                        .setField(field.getMessageType().findFieldByNumber(1), key)
                        .setField(field.getMessageType().findFieldByNumber(2), result).build();
                var raw = DynamicMessage.newBuilder(Document.getDescriptor()).setField(Document.getDescriptor().findFieldByNumber(1), "doc")
                        .addRepeatedField(field, entry).addRepeatedField(field, entry).build().toByteString();
                assertThat(Document.parseFrom(raw).getParserResultsCount()).isEqualTo(1);
                assertThatThrownBy(() -> inspect(DocumentPart.DOCUMENT_PART_PARSED, raw, LIMITS)).hasMessageContaining("duplicate parser key");
            }
        }
    }

    @Test void distinguishesAbsentRootsFromPresentEmptyEnvelopes() throws Exception {
        var document = Document.newBuilder().setDocId("doc");
        assertThat(inspect(DocumentPart.DOCUMENT_PART_CORE, document.build().toByteString(), LIMITS).roots()).isEmpty();
        var present = inspect(DocumentPart.DOCUMENT_PART_CORE, document.setStructuredData(Any.getDefaultInstance()).build().toByteString(), LIMITS);
        assertThat(present.roots()).hasSize(1);
        assertThat(present.roots().getFirst().envelope()).isEqualTo(Any.getDefaultInstance());
        var unknown = com.google.protobuf.UnknownFieldSet.newBuilder().addField(1000,
                com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var bytes = document.setStructuredData(OPAQUE.toBuilder().setUnknownFields(unknown)).build().toByteString();
        assertThatThrownBy(() -> inspect(DocumentPart.DOCUMENT_PART_CORE, bytes, LIMITS)).hasMessageContaining("unknown fields on root access path");
    }

    @Test void retainsOriginalNoncanonicalFragmentSeparatelyFromEffectiveAny() throws Exception {
        var canonical = Document.newBuilder().setDocId("doc").setStructuredData(OPAQUE).build().toByteString();
        var raw = canonical.concat(ByteString.copyFrom(new byte[]{10, 3, 'd', 'o', 'c'}));
        var result = inspect(DocumentPart.DOCUMENT_PART_CORE, raw, LIMITS);
        assertThat(result.original()).isSameAs(raw);
        assertThat(result.roots().getFirst().envelope()).isEqualTo(OPAQUE);
        assertThat(Document.parseFrom(raw).toByteString()).isEqualTo(canonical).isNotEqualTo(raw);
        assertThat(result.fragmentSha256()).isEqualTo(DocumentPartCodec.sha256Hex(raw.toByteArray()));
    }

    @Test void enforcesSlotIdentitySchemaAndIndependentBounds() throws Exception {
        var bytes = Document.newBuilder().setDocId("doc").putParserResults("one", parser()).putParserResults("two", parser()).build().toByteString();
        assertThatThrownBy(() -> inspect(DocumentPart.DOCUMENT_PART_CORE, bytes, LIMITS)).hasMessageContaining("another part");
        assertThatThrownBy(() -> inspect(DocumentPart.DOCUMENT_PART_BLOBS, bytes, LIMITS)).hasMessageContaining("CORE or PARSED");
        for (var bound : List.of(new DocumentAnyRootInventory.Limits(1, 1000, 30, 10, 10),
                new DocumentAnyRootInventory.Limits(100000, 1, 30, 10, 10),
                new DocumentAnyRootInventory.Limits(100000, 1000, 30, 1, 10),
                new DocumentAnyRootInventory.Limits(100000, 1000, 30, 10, 1))) {
            assertThatThrownBy(() -> inspect(DocumentPart.DOCUMENT_PART_PARSED, bytes, bound)).isInstanceOf(IllegalArgumentException.class);
        }
        var slot = DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build();
        assertThatThrownBy(() -> DocumentAnyRootInventory.inspect(CONTAINER, slot, bytes, "other", LIMITS, () -> {})).hasMessageContaining("identity");
        assertThatThrownBy(() -> DocumentAnyRootInventory.inspect(bind(Any.getDescriptor()), slot, bytes, "doc", LIMITS, () -> {})).hasMessageContaining("containing schema");
    }

    private static ParserResult parser() { return ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(OPAQUE)).build(); }
    private static DocumentAnyRootInventory.Result inspect(DocumentPart part, ByteString bytes, DocumentAnyRootInventory.Limits limits) throws Exception {
        return DocumentAnyRootInventory.inspect(CONTAINER, DocumentPublicationSlot.newBuilder().setPart(part).build(), bytes, "doc", limits, () -> {});
    }
    private static DocumentSchemaBinding bind(com.google.protobuf.Descriptors.Descriptor type) {
        var closure = DescriptorFingerprints.closure(type);
        return DocumentSchemaBinding.bind(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build(), closure.toByteString(),
                new ClosedDescriptorSet.Limits(16 * 1024 * 1024, 1024, 10000, 100), () -> {});
    }
}

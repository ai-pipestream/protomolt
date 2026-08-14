package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.MappingException;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AnyPayloadGateTest {

    @Test
    void offersEveryPayloadOnTheInstanceWithItsPath() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        List<String> paths = new ArrayList<>();
        List<String> types = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> {
            paths.add(path);
            types.add(unpacked.getDescriptorForType().getFullName());
        });
        DynamicMessage middle = DynamicMessage.newBuilder(fixtures.middle)
                .setField(fixtures.middle.findFieldByName("label"), "mid")
                .setField(fixtures.middle.findFieldByName("next"), Any.pack(fixtures.inner("Opinion", 12)))
                .build();
        DynamicMessage item = DynamicMessage.newBuilder(fixtures.item)
                .setField(fixtures.item.findFieldByName("payload"), Any.pack(fixtures.inner("Item", 1)))
                .build();
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("doc_id"), "doc-1")
                .setField(fixtures.envelope.findFieldByName("payload"), Any.pack(middle))
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"), Any.pack(fixtures.inner("A", 2)))
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"), Any.pack(fixtures.inner("B", 3)))
                .addRepeatedField(fixtures.envelope.findFieldByName("items"), item)
                .addRepeatedField(
                        fixtures.envelope.findFieldByName("keyed"),
                        fixtures.keyedEntry("cover", Any.pack(fixtures.inner("Cover", 4))))
                .build();

        gate.validate(message);

        assertThat(paths).containsExactly(
                "payload", "payload.next", "extras[0]", "extras[1]", "items[0].payload", "keyed[cover]");
        assertThat(types).containsExactly(
                fixtures.middle.getFullName(),
                fixtures.innerType.getFullName(),
                fixtures.innerType.getFullName(),
                fixtures.innerType.getFullName(),
                fixtures.innerType.getFullName(),
                fixtures.innerType.getFullName());
    }

    @Test
    void aRootMessageThatIsItselfAnAnyIsGated() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));

        gate.validate(Any.pack(fixtures.inner("Opinion", 12)));

        assertThat(paths).containsExactly("");
    }

    @Test
    void unknownTypeUrlFailsWithTheElementPath() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"), Any.pack(fixtures.inner("A", 1)))
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"), Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("extras[1]")
                .hasMessageContaining("type.googleapis.com/ai.pipestream.test.MissingType");
    }

    @Test
    void valueBytesWithoutATypeUrlAreMalformed() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("payload"), Any.newBuilder()
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("no type URL")
                .hasMessageContaining("payload");
    }

    @Test
    void unsetAndEmptyAnysContributeNothing() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("doc_id"), "doc-1")
                .setField(fixtures.envelope.findFieldByName("payload"), Any.getDefaultInstance())
                .build();

        gate.validate(message);

        assertThat(paths).isEmpty();
    }

    @Test
    void nestingUpToTheDepthCapPasses() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));

        gate.validate(fixtures.envelope(fixtures.chainOfAnys(AnyIndexing.MAX_EXPANSION_DEPTH)));

        assertThat(paths).hasSize(AnyIndexing.MAX_EXPANSION_DEPTH);
        assertThat(paths.get(0)).isEqualTo("payload");
        assertThat(paths.get(1)).isEqualTo("payload.next");
    }

    @Test
    void nestingBeyondTheDepthCapFailsInsteadOfRecursingForever() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        DynamicMessage message = fixtures.envelope(fixtures.chainOfAnys(13));

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("nesting exceeds")
                .hasMessageContaining("payload.next");
    }

    @Test
    void withoutValidatorsTheWalkIsSkippedEntirely() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        AnyPayloadGate gate = new AnyPayloadGate(fixtures.registry, List.of());
        DynamicMessage message = fixtures.envelope(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                .setValue(ByteString.copyFromUtf8("x"))
                .build());

        assertThatCode(() -> gate.validate(message)).doesNotThrowAnyException();
    }

    @Test
    void aThrowingValidatorAbortsTheDocument() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> {
            throw new IllegalStateException("declared rule violated at " + path);
        });

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(Any.pack(fixtures.inner("Opinion", 12)))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void validatePayloadsFalseSkipsTheValidatorsForThatFieldOnly() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        CatalogIndexingHintSource hints = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "payload",
                        ResolvedFieldHint.builder(IndexFieldKind.ANY).validatePayloads(false).build());
        List<String> offered = new ArrayList<>();
        AnyPayloadGate gate = new AnyPayloadGate(fixtures.registry,
                List.of((unpacked, path) -> offered.add(path)), hints);
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("payload"), Any.pack(fixtures.inner("A", 1)))
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"), Any.pack(fixtures.inner("B", 2)))
                .build();

        gate.validate(message);

        assertThat(offered).containsExactly("extras[0]");
    }

    @Test
    void validatePayloadsFalseStillFailsMalformedAnys() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        CatalogIndexingHintSource hints = new CatalogIndexingHintSource()
                .put(fixtures.envelope.getFullName(), "payload",
                        ResolvedFieldHint.builder(IndexFieldKind.ANY).validatePayloads(false).build());
        AnyPayloadGate gate = new AnyPayloadGate(fixtures.registry,
                List.of((unpacked, path) -> { }), hints);
        DynamicMessage message = fixtures.envelope(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                .setValue(ByteString.copyFromUtf8("x"))
                .build());

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("unknown type URL");
    }

    @Test
    void scalarMapsAndScalarFieldsAreIgnored() throws Exception {
        GateFixtures fixtures = GateFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .setField(fixtures.envelope.findFieldByName("doc_id"), "doc-1")
                .addRepeatedField(
                        fixtures.envelope.findFieldByName("labels"),
                        fixtures.labelsEntry("lang", "en"))
                .build();

        gate.validate(message);

        assertThat(paths).isEmpty();
    }

    private record GateFixtures(
            Descriptor envelope,
            Descriptor innerType,
            Descriptor middle,
            Descriptor item,
            DescriptorRegistry registry) {

        static GateFixtures create() throws Exception {
            FileDescriptor file = gateFile();
            Descriptor envelope = file.findMessageTypeByName("Envelope");
            Descriptor inner = file.findMessageTypeByName("InnerPayload");
            Descriptor middle = file.findMessageTypeByName("Middle");
            Descriptor item = file.findMessageTypeByName("Item");
            DescriptorRegistry registry = new DescriptorRegistry();
            registry.register(inner);
            registry.register(middle);
            return new GateFixtures(envelope, inner, middle, item, registry);
        }

        AnyPayloadGate gate(AnyPayloadValidator validator) {
            return new AnyPayloadGate(registry, List.of(validator));
        }

        DynamicMessage inner(String title, int pageCount) {
            return DynamicMessage.newBuilder(innerType)
                    .setField(innerType.findFieldByName("title"), title)
                    .setField(innerType.findFieldByName("page_count"), pageCount)
                    .build();
        }

        DynamicMessage envelope(Any payload) {
            return DynamicMessage.newBuilder(envelope)
                    .setField(envelope.findFieldByName("doc_id"), "doc-1")
                    .setField(envelope.findFieldByName("payload"), payload)
                    .build();
        }

        /** A chain of {@code count} Anys: the innermost packs InnerPayload, the rest pack Middle. */
        Any chainOfAnys(int count) {
            Any chain = Any.pack(inner("deep", 1));
            for (int i = 1; i < count; i++) {
                chain = Any.pack(DynamicMessage.newBuilder(middle)
                        .setField(middle.findFieldByName("next"), chain)
                        .build());
            }
            return chain;
        }

        DynamicMessage keyedEntry(String key, Any value) {
            Descriptor entry = envelope.findNestedTypeByName("KeyedEntry");
            return DynamicMessage.newBuilder(entry)
                    .setField(entry.findFieldByName("key"), key)
                    .setField(entry.findFieldByName("value"), value)
                    .build();
        }

        DynamicMessage labelsEntry(String key, String value) {
            Descriptor entry = envelope.findNestedTypeByName("LabelsEntry");
            return DynamicMessage.newBuilder(entry)
                    .setField(entry.findFieldByName("key"), key)
                    .setField(entry.findFieldByName("value"), value)
                    .build();
        }

        private static FileDescriptor gateFile() throws Exception {
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("any_payload_gate_doc.proto")
                    .setPackage("ai.pipestream.test.gate")
                    .setSyntax("proto3")
                    .addDependency("google/protobuf/any.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("InnerPayload")
                            .addField(stringField("title", 1))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("page_count")
                                    .setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Middle")
                            .addField(stringField("label", 1))
                            .addField(anyField("next", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Item")
                            .addField(anyField("payload", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Envelope")
                            .addField(stringField("doc_id", 1))
                            .addField(anyField("payload", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(anyField("extras", 3, FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("items")
                                    .setNumber(4)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.gate.Item")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("keyed")
                                    .setNumber(5)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.gate.Envelope.KeyedEntry")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("labels")
                                    .setNumber(6)
                                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName(".ai.pipestream.test.gate.Envelope.LabelsEntry")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addNestedType(mapEntry("KeyedEntry",
                                    anyField("value", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                            .addNestedType(mapEntry("LabelsEntry",
                                    stringField("value", 2))))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{AnyProto.getDescriptor()});
        }

        private static DescriptorProto.Builder mapEntry(
                String name, FieldDescriptorProto.Builder valueField) {
            return DescriptorProto.newBuilder()
                    .setName(name)
                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                    .addField(stringField("key", 1))
                    .addField(valueField);
        }

        private static FieldDescriptorProto.Builder stringField(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder anyField(
                String name, int number, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(".google.protobuf.Any")
                    .setLabel(label);
        }
    }
}

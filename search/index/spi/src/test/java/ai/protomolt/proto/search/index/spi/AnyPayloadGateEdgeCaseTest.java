package ai.protomolt.proto.search.index.spi;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Adversarial edge cases for {@link AnyPayloadGate}: oneof members, proto2 groups,
 * recursive message types, nested and non-string-keyed maps, hostile type URLs, hostile
 * payload bytes, Any-in-Any, well-known payload types, and the exact depth cap.
 */
class AnyPayloadGateEdgeCaseTest {

    @Test
    void anAnyInsideAOneofIsGatedUnderTheMemberFieldName() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage choice = DynamicMessage.newBuilder(fixtures.choice)
                .setField(fixtures.choice.findFieldByName("packed"), Any.pack(fixtures.inner("Opinion", 1)))
                .build();

        gate.validate(fixtures.envelopeWith("choice", choice));

        assertThat(paths).containsExactly("choice.packed");
    }

    @Test
    void theUnsetArmOfAOneofContributesNothing() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage choice = DynamicMessage.newBuilder(fixtures.choice)
                .setField(fixtures.choice.findFieldByName("text"), "no payload here")
                .build();

        gate.validate(fixtures.envelopeWith("choice", choice));

        assertThat(paths).isEmpty();
    }

    @Test
    void aRecursiveMessageTypeIsWalkedDownToItsLeafAny() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage leaf = DynamicMessage.newBuilder(fixtures.node)
                .setField(fixtures.node.findFieldByName("name"), "c")
                .setField(fixtures.node.findFieldByName("tag"), Any.pack(fixtures.inner("Leaf", 3)))
                .build();
        DynamicMessage middle = DynamicMessage.newBuilder(fixtures.node)
                .setField(fixtures.node.findFieldByName("name"), "b")
                .setField(fixtures.node.findFieldByName("child"), leaf)
                .build();
        DynamicMessage root = DynamicMessage.newBuilder(fixtures.node)
                .setField(fixtures.node.findFieldByName("name"), "a")
                .setField(fixtures.node.findFieldByName("child"), middle)
                .build();

        gate.validate(fixtures.envelopeWith("node", root));

        assertThat(paths).containsExactly("node.child.child.tag");
    }

    @Test
    void anAnyInAMapValueOfANestedMessageCarriesBothSubscripts() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage item = DynamicMessage.newBuilder(fixtures.item)
                .setField(fixtures.item.findFieldByName("payload"), Any.pack(fixtures.inner("Slot", 1)))
                .build();
        DynamicMessage bag = DynamicMessage.newBuilder(fixtures.bag)
                .addRepeatedField(
                        fixtures.bag.findFieldByName("slots"),
                        fixtures.mapEntryValue(fixtures.bag, "SlotsEntry", "s1", item))
                .build();

        gate.validate(fixtures.envelopeWith("bag", bag));

        assertThat(paths).containsExactly("bag.slots[s1].payload");
    }

    @Test
    void nonStringMapKeysRenderAsTheirValueInThePath() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        Descriptor entry = fixtures.envelope.findNestedTypeByName("NumberedEntry");
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.envelope.findFieldByName("numbered"),
                        DynamicMessage.newBuilder(entry)
                                .setField(entry.findFieldByName("key"), 7)
                                .setField(entry.findFieldByName("value"), Any.pack(fixtures.inner("Seven", 7)))
                                .build())
                .build();

        gate.validate(message);

        assertThat(paths).containsExactly("numbered[7]");
    }

    @Test
    void anEmptyMapKeyRendersAsAnEmptySubscript() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));

        gate.validate(fixtures.keyed("", Any.pack(fixtures.inner("Blank", 1))));

        assertThat(paths).containsExactly("keyed[]");
    }

    @Test
    void mapKeysCarryingPathPunctuationAreRenderedVerbatim() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));

        // The subscript is not escaped: a key carrying '.' or ']' produces an ambiguous but
        // lossless-by-concatenation path. Pinned so any future escaping is a deliberate change.
        gate.validate(fixtures.keyed("a.b]c", Any.pack(fixtures.inner("Punct", 1))));

        assertThat(paths).containsExactly("keyed[a.b]c]");
    }

    @Test
    void aTypeUrlWithANonStandardPrefixResolvesByItsTrailingTypeName() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> types = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) ->
                types.add(unpacked.getDescriptorForType().getFullName()));
        Any packed = Any.newBuilder()
                .setTypeUrl("types.example.com/" + fixtures.innerType.getFullName())
                .setValue(fixtures.inner("Opinion", 4).toByteString())
                .build();

        gate.validate(fixtures.envelope(packed));

        assertThat(types).containsExactly(fixtures.innerType.getFullName());
    }

    @Test
    void aBareTypeNameWithNoSlashIsRejectedAsSpecViolating() throws Exception {
        // The Any contract requires 'host/fully.qualified.TypeName'; renderers (JsonFormat)
        // reject slashless URLs, so the gate fails them by path instead of resolving them.
        EdgeFixtures fixtures = EdgeFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        Any packed = Any.newBuilder()
                .setTypeUrl(fixtures.innerType.getFullName())
                .setValue(fixtures.inner("Opinion", 4).toByteString())
                .build();

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(packed)))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("no '/'")
                .hasMessageContaining("payload");
    }

    @Test
    void malformedPayloadBytesForARegisteredTypeAreNotReportedAsAnUnknownTypeUrl() throws Exception {
        // A registered type with corrupt value bytes must not be misdiagnosed as unregistered.
        EdgeFixtures fixtures = EdgeFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        // Tag for field 1 (length-delimited) followed by a length of 127 and no bytes.
        Any truncated = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/" + fixtures.innerType.getFullName())
                .setValue(ByteString.copyFrom(new byte[]{0x0A, 0x7F}))
                .build();

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(truncated)))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload")
                .hasMessageNotContaining("unknown type URL")
                .hasMessageNotContaining("register the packed type");
    }

    @Test
    void aBlankButNonEmptyTypeUrlIsMalformedRatherThanUnset() throws Exception {
        // A whitespace-only type URL is neither unset nor renderable: it must fail, not vanish.
        EdgeFixtures fixtures = EdgeFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(Any.newBuilder().setTypeUrl(" ").build())))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void anAnyThatPacksAnAnyUnwrapsBothLevelsAtTheSamePath() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        List<String> types = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> {
            paths.add(path);
            types.add(unpacked.getDescriptorForType().getFullName());
        });

        gate.validate(fixtures.envelope(Any.pack(Any.pack(fixtures.inner("Opinion", 12)))));

        assertThat(paths).containsExactly("payload", "payload");
        assertThat(types).containsExactly(
                Any.getDescriptor().getFullName(), fixtures.innerType.getFullName());
    }

    @Test
    void aWellKnownTypePayloadIsGatedFromTheRegistrysBuiltInDescriptors() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> types = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) ->
                types.add(unpacked.getDescriptorForType().getFullName()));

        gate.validate(fixtures.envelope(Any.pack(Timestamp.newBuilder().setSeconds(42).build())));

        assertThat(types).containsExactly(Timestamp.getDescriptor().getFullName());
    }

    @Test
    void anAnyWithARegisteredTypeAndNoValueBytesOffersADefaultInstancePayload() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> types = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> {
            types.add(unpacked.getDescriptorForType().getFullName());
            assertThat(unpacked.getAllFields()).isEmpty();
        });
        Any empty = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/" + fixtures.innerType.getFullName())
                .build();

        gate.validate(fixtures.envelope(empty));

        assertThat(types).containsExactly(fixtures.innerType.getFullName());
    }

    @Test
    void anUnregisteredTypeNestedInsideAKnownPayloadFailsWithTheNestedPath() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        DynamicMessage middle = DynamicMessage.newBuilder(fixtures.middle)
                .setField(fixtures.middle.findFieldByName("next"), Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.protomolt.test.edge.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(Any.pack(middle))))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("payload.next")
                .hasMessageContaining("ai.protomolt.test.edge.MissingType");
    }

    @Test
    void nestingOneLevelPastTheCapFailsAtTheDeepestPath() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> { });
        DynamicMessage message = fixtures.envelope(
                fixtures.chainOfAnys(AnyIndexing.MAX_EXPANSION_DEPTH + 1));

        assertThatThrownBy(() -> gate.validate(message))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("nesting exceeds")
                .hasMessageContaining("payload.next.next.next.next.next.next.next");
    }

    @Test
    void siblingAnyChainsDoNotShareADepthBudget() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage message = DynamicMessage.newBuilder(fixtures.envelope)
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"),
                        fixtures.chainOfAnys(AnyIndexing.MAX_EXPANSION_DEPTH))
                .addRepeatedField(fixtures.envelope.findFieldByName("extras"),
                        fixtures.chainOfAnys(AnyIndexing.MAX_EXPANSION_DEPTH))
                .build();

        gate.validate(message);

        assertThat(paths).hasSize(2 * AnyIndexing.MAX_EXPANSION_DEPTH);
    }

    @Test
    void anErrorThrownByAValidatorPropagatesUnwrapped() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> {
            throw new AssertionError("hard failure at " + path);
        });

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(Any.pack(fixtures.inner("A", 1)))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("payload");
    }

    @Test
    void theFirstValidatorToThrowStopsTheWalk() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        List<String> second = new ArrayList<>();
        AnyPayloadGate gate = new AnyPayloadGate(fixtures.registry, List.of(
                (unpacked, path) -> {
                    throw new IllegalStateException("first says no at " + path);
                },
                (unpacked, path) -> second.add(path)));

        assertThatThrownBy(() -> gate.validate(fixtures.envelope(Any.pack(fixtures.inner("A", 1)))))
                .isInstanceOf(IllegalStateException.class);
        assertThat(second).isEmpty();
    }

    @Test
    void anAnyInsideAProto2GroupIsGated() throws Exception {
        EdgeFixtures fixtures = EdgeFixtures.create();
        Descriptor holder = EdgeFixtures.groupFile().findMessageTypeByName("GroupHolder");
        Descriptor wrap = holder.findNestedTypeByName("Wrap");
        List<String> paths = new ArrayList<>();
        AnyPayloadGate gate = fixtures.gate((unpacked, path) -> paths.add(path));
        DynamicMessage message = DynamicMessage.newBuilder(holder)
                .setField(holder.findFieldByName("wrap"), DynamicMessage.newBuilder(wrap)
                        .setField(wrap.findFieldByName("payload"), Any.pack(fixtures.inner("Grouped", 2)))
                        .build())
                .build();

        gate.validate(message);

        assertThat(paths).containsExactly("wrap.payload");
    }

    private record EdgeFixtures(
            Descriptor envelope,
            Descriptor innerType,
            Descriptor middle,
            Descriptor item,
            Descriptor bag,
            Descriptor node,
            Descriptor choice,
            DescriptorRegistry registry) {

        static EdgeFixtures create() throws Exception {
            FileDescriptor file = edgeFile();
            DescriptorRegistry registry = new DescriptorRegistry();
            registry.register(file.findMessageTypeByName("InnerPayload"));
            registry.register(file.findMessageTypeByName("Middle"));
            return new EdgeFixtures(
                    file.findMessageTypeByName("Envelope"),
                    file.findMessageTypeByName("InnerPayload"),
                    file.findMessageTypeByName("Middle"),
                    file.findMessageTypeByName("Item"),
                    file.findMessageTypeByName("Bag"),
                    file.findMessageTypeByName("Node"),
                    file.findMessageTypeByName("Choice"),
                    registry);
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

        DynamicMessage envelopeWith(String fieldName, DynamicMessage value) {
            return DynamicMessage.newBuilder(envelope)
                    .setField(envelope.findFieldByName(fieldName), value)
                    .build();
        }

        DynamicMessage keyed(String key, Any value) {
            Descriptor entry = envelope.findNestedTypeByName("KeyedEntry");
            return DynamicMessage.newBuilder(envelope)
                    .addRepeatedField(envelope.findFieldByName("keyed"), DynamicMessage.newBuilder(entry)
                            .setField(entry.findFieldByName("key"), key)
                            .setField(entry.findFieldByName("value"), value)
                            .build())
                    .build();
        }

        DynamicMessage mapEntryValue(
                Descriptor owner, String entryName, String key, DynamicMessage value) {
            Descriptor entry = owner.findNestedTypeByName(entryName);
            return DynamicMessage.newBuilder(entry)
                    .setField(entry.findFieldByName("key"), key)
                    .setField(entry.findFieldByName("value"), value)
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

        private static FileDescriptor edgeFile() throws Exception {
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("any_payload_gate_edge_doc.proto")
                    .setPackage("ai.protomolt.test.edge")
                    .setSyntax("proto3")
                    .addDependency("google/protobuf/any.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("InnerPayload")
                            .addField(stringField("title", 1))
                            .addField(int32Field("page_count", 2)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Middle")
                            .addField(stringField("label", 1))
                            .addField(anyField("next", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Item")
                            .addField(anyField("payload", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Bag")
                            .addField(messageField(
                                    "slots", 1, ".ai.protomolt.test.edge.Bag.SlotsEntry",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addNestedType(mapEntry("SlotsEntry", stringField("key", 1), messageField(
                                    "value", 2, ".ai.protomolt.test.edge.Item",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Node")
                            .addField(stringField("name", 1))
                            .addField(messageField(
                                    "child", 2, ".ai.protomolt.test.edge.Node",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(anyField("tag", 3, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Choice")
                            .addOneofDecl(OneofDescriptorProto.newBuilder().setName("pick"))
                            .addField(anyField("packed", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setOneofIndex(0))
                            .addField(stringField("text", 2).setOneofIndex(0)))
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Envelope")
                            .addField(stringField("doc_id", 1))
                            .addField(anyField("payload", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(anyField("extras", 3, FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(messageField(
                                    "keyed", 4, ".ai.protomolt.test.edge.Envelope.KeyedEntry",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(messageField(
                                    "numbered", 5, ".ai.protomolt.test.edge.Envelope.NumberedEntry",
                                    FieldDescriptorProto.Label.LABEL_REPEATED))
                            .addField(messageField(
                                    "bag", 6, ".ai.protomolt.test.edge.Bag",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(messageField(
                                    "node", 7, ".ai.protomolt.test.edge.Node",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(messageField(
                                    "choice", 8, ".ai.protomolt.test.edge.Choice",
                                    FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addNestedType(mapEntry("KeyedEntry", stringField("key", 1),
                                    anyField("value", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                            .addNestedType(mapEntry("NumberedEntry", int32Field("key", 1),
                                    anyField("value", 2, FieldDescriptorProto.Label.LABEL_OPTIONAL))))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{AnyProto.getDescriptor()});
        }

        /** Groups are proto2-only, so they need their own file. */
        static FileDescriptor groupFile() throws Exception {
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("any_payload_gate_group_doc.proto")
                    .setPackage("ai.protomolt.test.edge.group")
                    .setSyntax("proto2")
                    .addDependency("google/protobuf/any.proto")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("GroupHolder")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("wrap")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_GROUP)
                                    .setTypeName(".ai.protomolt.test.edge.group.GroupHolder.Wrap")
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addNestedType(DescriptorProto.newBuilder()
                                    .setName("Wrap")
                                    .addField(anyField(
                                            "payload", 1, FieldDescriptorProto.Label.LABEL_OPTIONAL))))
                    .build();
            return FileDescriptor.buildFrom(proto, new FileDescriptor[]{AnyProto.getDescriptor()});
        }

        private static DescriptorProto.Builder mapEntry(
                String name,
                FieldDescriptorProto.Builder keyField,
                FieldDescriptorProto.Builder valueField) {
            return DescriptorProto.newBuilder()
                    .setName(name)
                    .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                    .addField(keyField)
                    .addField(valueField);
        }

        private static FieldDescriptorProto.Builder stringField(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder int32Field(String name, int number) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        }

        private static FieldDescriptorProto.Builder messageField(
                String name, int number, String typeName, FieldDescriptorProto.Label label) {
            return FieldDescriptorProto.newBuilder()
                    .setName(name)
                    .setNumber(number)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(typeName)
                    .setLabel(label);
        }

        private static FieldDescriptorProto.Builder anyField(
                String name, int number, FieldDescriptorProto.Label label) {
            return messageField(name, number, ".google.protobuf.Any", label);
        }
    }
}

package ai.protomolt.proto.descriptors;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClosedDescriptorSetTest {
    private static final ClosedDescriptorSet.Limits LIMITS =
            new ClosedDescriptorSet.Limits(1_000_000, 200, 400, 100);

    @Test
    void decodesUsingOnlyRetainedDescriptorsInDependencyOrder() throws Exception {
        var envelope = file("envelope.proto").toBuilder().setPackage("archive")
                .addDependency("google/protobuf/any.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Envelope")
                        .addField(FieldDescriptorProto.newBuilder().setName("payload").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".google.protobuf.Any")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))).build();
        var descriptors = ClosedDescriptorSet.load(bytes(envelope, Any.getDescriptor().getFile().toProto()), LIMITS);
        assertThat(descriptors).extracting(d -> d.getName())
                .containsExactly("google/protobuf/any.proto", "envelope.proto");
        var type = descriptors.get(1).findMessageTypeByName("Envelope");
        var field = type.findFieldByName("payload");
        var payload = DynamicMessage.parseFrom(field.getMessageType(),
                Any.newBuilder().setTypeUrl("type.googleapis.com/example.Data")
                        .setValue(ByteString.copyFromUtf8("retained payload")).build().toByteString());
        var original = DynamicMessage.newBuilder(type).setField(field, payload).build();
        assertThat(DynamicMessage.parseFrom(type, original.toByteString())).isEqualTo(original);
        assertThatThrownBy(() -> descriptors.clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void preservesDeclaredImportOrderAndPublicImportIndexes() {
        var dependency = file("types.proto").toBuilder().setPackage("shared")
                .addMessageType(DescriptorProto.newBuilder().setName("Data")).build();
        var bridge = file("bridge.proto", "unrelated.proto", "types.proto").toBuilder()
                .addPublicDependency(1).build();
        var consumer = file("consumer.proto", "bridge.proto").toBuilder()
                .addMessageType(DescriptorProto.newBuilder().setName("Consumer")
                        .addField(FieldDescriptorProto.newBuilder().setName("data").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".shared.Data")))
                .build();
        var linked = ClosedDescriptorSet.load(bytes(consumer, bridge, dependency, file("unrelated.proto")), LIMITS);
        var linkedBridge = linked.stream().filter(f -> f.getName().equals("bridge.proto")).findFirst().orElseThrow();
        assertThat(linkedBridge.getDependencies()).extracting(f -> f.getName())
                .containsExactly("unrelated.proto", "types.proto");
        assertThat(linkedBridge.getPublicDependencies()).extracting(f -> f.getName()).containsExactly("types.proto");
        assertThat(linked.getLast().findMessageTypeByName("Consumer").findFieldByName("data")
                .getMessageType().getFullName()).isEqualTo("shared.Data");
    }

    @Test
    void rejectsAmbiguousMessageNamesAcrossDisconnectedFiles() {
        var type = DescriptorProto.newBuilder().setName("Event").build();
        var first = file("first.proto").toBuilder().setPackage("archive").addMessageType(type).build();
        var second = file("second.proto").toBuilder().setPackage("archive").addMessageType(type).build();
        reject(bytes(first, second), "duplicate message name: archive.Event");
        // The same full name can arise from a nested message or a longer package.
        var nested = file("nested.proto").toBuilder().setPackage("archive")
                .addMessageType(DescriptorProto.newBuilder().setName("Outer").addNestedType(type)).build();
        var packageType = file("package.proto").toBuilder().setPackage("archive.Outer").addMessageType(type).build();
        reject(bytes(nested, packageType), "duplicate message name: archive.Outer.Event");
    }

    @Test
    void rejectsMissingOrdinaryAndWellKnownImports() {
        for (String missing : new String[]{"missing.proto", "google/protobuf/any.proto"}) {
            reject(bytes(file("root.proto", missing)), "missing import");
        }
    }

    @Test
    void rejectsEmptyUnnamedDuplicateAndConflictingFiles() {
        reject(ByteString.EMPTY, "empty");
        reject(FileDescriptorSet.newBuilder().setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                .addField(99, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                .build()).build().toByteString(), "file count");
        reject(bytes(file("")), "filename");
        reject(bytes(file("a.proto"), file("a.proto")), "duplicate");
        reject(bytes(file("a.proto"), file("a.proto").toBuilder().setPackage("different").build()), "duplicate");
        reject(bytes(file("a.proto", "b.proto", "b.proto"), file("b.proto")), "duplicate import");
    }

    @Test
    void rejectsCyclesAndUnresolvedMessageTypes() {
        reject(bytes(file("a.proto", "a.proto")), "cycle");
        reject(bytes(file("a.proto", "b.proto"), file("b.proto", "a.proto")), "cycle");
        var invalid = file("bad.proto").toBuilder().addMessageType(DescriptorProto.newBuilder().setName("Bad")
                .addField(FieldDescriptorProto.newBuilder().setName("missing").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".Absent")
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))).build();
        reject(bytes(invalid), "invalid descriptor");
    }

    @Test
    void enforcesByteFileEdgeAndImportDepthBoundaries() {
        ByteString bytes = bytes(file("a.proto", "b.proto"), file("b.proto", "c.proto"), file("c.proto"));
        assertThat(ClosedDescriptorSet.load(bytes, new ClosedDescriptorSet.Limits(bytes.size(), 3, 2, 3))).hasSize(3);
        for (var limits : new ClosedDescriptorSet.Limits[]{
                new ClosedDescriptorSet.Limits(bytes.size() - 1, 3, 2, 3),
                new ClosedDescriptorSet.Limits(bytes.size(), 2, 2, 3),
                new ClosedDescriptorSet.Limits(bytes.size(), 3, 1, 3),
                new ClosedDescriptorSet.Limits(bytes.size(), 3, 2, 2)}) {
            assertThatThrownBy(() -> ClosedDescriptorSet.load(bytes, limits)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new ClosedDescriptorSet.Limits(0, 1, 1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ClosedDescriptorSet.Limits(1, 0, 1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ClosedDescriptorSet.Limits(1, 1, 0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ClosedDescriptorSet.Limits(1, 1, 1, 101)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void buildsLongReverseOrderedChainWithoutRecursiveGraphTraversal() {
        var set = FileDescriptorSet.newBuilder();
        for (int i = 99; i >= 0; i--) {
            set.addFile(i == 0 ? file("0.proto") : file(i + ".proto", (i - 1) + ".proto"));
        }
        assertThat(ClosedDescriptorSet.load(set.build().toByteString(), LIMITS)).hasSize(100);
    }

    @Test
    void rejectsMalformedAndExcessivelyNestedSerializedDescriptors() {
        reject(ByteString.copyFrom(new byte[]{10, 100, 1}), "invalid descriptor artifact");
        var message = DescriptorProto.newBuilder().setName("Nested").build();
        for (int i = 0; i < 110; i++) {
            message = DescriptorProto.newBuilder().setName("Nested").addNestedType(message).build();
        }
        reject(bytes(file("deep.proto").toBuilder().addMessageType(message).build()), "invalid descriptor artifact");
    }

    private static FileDescriptorProto file(String name, String... dependencies) {
        return FileDescriptorProto.newBuilder().setName(name).setSyntax("proto3")
                .addAllDependency(java.util.List.of(dependencies)).build();
    }

    private static ByteString bytes(FileDescriptorProto... files) {
        return FileDescriptorSet.newBuilder().addAllFile(java.util.List.of(files)).build().toByteString();
    }

    private static void reject(ByteString bytes, String message) {
        assertThatThrownBy(() -> ClosedDescriptorSet.load(bytes, LIMITS))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message);
    }
}

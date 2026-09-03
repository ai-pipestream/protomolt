package ai.protomolt.proto.helpers;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoFqnConflictDetectorTest {

    @Test
    void identicalDefinitionsAcrossSourcesAreAllowed() throws Exception {
        FileDescriptorProto a = messageFile("a.proto", "demo", "Foo", FieldDescriptorProto.Type.TYPE_STRING);
        FileDescriptorProto b = messageFile("b.proto", "demo", "Foo", FieldDescriptorProto.Type.TYPE_STRING);
        Map<String, FileDescriptorProto> sources = new LinkedHashMap<>();
        sources.put("ref-a", a);
        sources.put("ref-b", b);
        assertThatCode(() -> ProtoFqnConflictDetector.assertNoConflicts(sources))
                .doesNotThrowAnyException();
    }

    @Test
    void conflictingWireShapesAreRejected() {
        FileDescriptorProto a = messageFile("a.proto", "demo", "Foo", FieldDescriptorProto.Type.TYPE_STRING);
        FileDescriptorProto b = messageFile("b.proto", "demo", "Foo", FieldDescriptorProto.Type.TYPE_INT32);
        Map<String, FileDescriptorProto> sources = new LinkedHashMap<>();
        sources.put("ref-a", a);
        sources.put("ref-b", b);
        assertThatThrownBy(() -> ProtoFqnConflictDetector.assertNoConflicts(sources))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("demo.Foo")
                .extracting(ex -> ((ProtoSchemaValidationException) ex).getFqn())
                .isEqualTo("demo.Foo");
    }

    @Test
    void nestedConflictReportsInnerFqn() {
        DescriptorProto innerA = DescriptorProto.newBuilder()
                .setName("Inner")
                .addField(field("x", 1, FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProto innerB = DescriptorProto.newBuilder()
                .setName("Inner")
                .addField(field("x", 1, FieldDescriptorProto.Type.TYPE_INT32))
                .build();
        FileDescriptorProto a = FileDescriptorProto.newBuilder()
                .setName("a.proto").setPackage("demo")
                .addMessageType(DescriptorProto.newBuilder().setName("Outer").addNestedType(innerA))
                .build();
        FileDescriptorProto b = FileDescriptorProto.newBuilder()
                .setName("b.proto").setPackage("demo")
                .addMessageType(DescriptorProto.newBuilder().setName("Outer").addNestedType(innerB))
                .build();
        Map<String, FileDescriptorProto> sources = Map.of("a", a, "b", b);
        assertThatThrownBy(() -> ProtoFqnConflictDetector.assertNoConflicts(sources))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("demo.Outer.Inner");
    }

    @Test
    void rejectsUnsafeBinaryIdentifiers() {
        FileDescriptorProto evil = FileDescriptorProto.newBuilder()
                .setName("evil.proto")
                .setPackage("p")
                .addMessageType(DescriptorProto.newBuilder().setName("Data(){console}"))
                .build();
        assertThatThrownBy(() -> BinaryProtobufIdentifierValidator.validate("evil", evil))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("identifier");
    }

    @Test
    void base64RoundTripDetection() throws Exception {
        FileDescriptorProto ok = messageFile("ok.proto", "p", "Ok", FieldDescriptorProto.Type.TYPE_STRING);
        String encoded = Base64.getEncoder().encodeToString(ok.toByteArray());
        FileDescriptorProto parsed = BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(encoded);
        assertThat(parsed).isNotNull();
        BinaryProtobufIdentifierValidator.validate("ok", parsed);

        assertThat(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(
                "syntax = \"proto3\"; message X {}")).isNull();
    }

    @Test
    void validateAndAssertNoConflictsRunsBothChecks() {
        FileDescriptorProto evil = FileDescriptorProto.newBuilder()
                .setName("evil.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Bad()Name"))
                .build();
        assertThatThrownBy(() ->
                ProtoFqnConflictDetector.validateAndAssertNoConflicts(Map.of("evil", evil)))
                .isInstanceOf(ProtoSchemaValidationException.class);
    }

    private static FileDescriptorProto messageFile(
            String fileName, String pkg, String message, FieldDescriptorProto.Type fieldType) {
        return FileDescriptorProto.newBuilder()
                .setName(fileName)
                .setPackage(pkg)
                .addMessageType(DescriptorProto.newBuilder()
                        .setName(message)
                        .addField(field("a", 1, fieldType)))
                .build();
    }

    private static FieldDescriptorProto field(String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(type)
                .build();
    }
}

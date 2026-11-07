package ai.protomolt.proto.helpers;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HelpersEdgeCasesTest {

    @Test void validatorAcceptsNullAndEmptyAndRejectsGarbage() {
        // Null proto is documented as a no-op.
        assertDoesNotThrow(() -> BinaryProtobufIdentifierValidator.validate("s", null));
        // Empty bytes parse to the default FileDescriptorProto, which has nothing invalid.
        assertDoesNotThrow(() -> BinaryProtobufIdentifierValidator.validateBytes("s", new byte[0]));
        assertThrows(InvalidProtocolBufferException.class,
                () -> BinaryProtobufIdentifierValidator.validateBytes("s", new byte[]{1, 2, 3}));
        assertThrows(NullPointerException.class,
                () -> BinaryProtobufIdentifierValidator.validateBytes("s", null));
        assertNull(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor("not-base64!!!"));
        assertNull(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(""));
    }

    @Test void validatorAcceptsRealDescriptorProto() throws Exception {
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("ok.proto").setPackage("ok").build();
        String encoded = Base64.getEncoder().encodeToString(file.toByteArray());
        assertNotNull(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(encoded));
        BinaryProtobufIdentifierValidator.validate("ok", file);
        BinaryProtobufIdentifierValidator.validateBytes("ok", file.toByteArray());
    }

    @Test void conflictDetectorEmptyMapPasses() {
        assertDoesNotThrow(() -> ProtoFqnConflictDetector.assertNoConflicts(Collections.emptyMap()));
        assertDoesNotThrow(() -> ProtoFqnConflictDetector.validateAndAssertNoConflicts(Collections.emptyMap()));
    }

    @Test void conflictDetectorDetectsSameMessageInTwoFiles() {
        // Identical definitions are benign duplicates; conflicting ones must throw.
        DescriptorProtos.DescriptorProto msg = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Dup").build();
        DescriptorProtos.DescriptorProto changed = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Dup")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("extra").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .build())
                .build();
        DescriptorProtos.FileDescriptorProto a = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("a.proto").setPackage("dup").addMessageType(msg).build();
        DescriptorProtos.FileDescriptorProto sameAsA = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("a2.proto").setPackage("dup").addMessageType(msg).build();
        DescriptorProtos.FileDescriptorProto b = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("b.proto").setPackage("dup").addMessageType(changed).build();
        assertDoesNotThrow(() -> ProtoFqnConflictDetector.assertNoConflicts(Map.of("a", a, "a2", sameAsA)));
        Map<String, DescriptorProtos.FileDescriptorProto> sources = Map.of("a", a, "b", b);
        assertThrows(ProtoSchemaValidationException.class,
                () -> ProtoFqnConflictDetector.assertNoConflicts(sources));
    }

    @Test void typeConverterBoundaryAndInvalid() throws Exception {
        TypeConverter converter = new TypeConverter();
        Value maxLong = converter.toValue(Long.MAX_VALUE);
        assertEquals((double) Long.MAX_VALUE, maxLong.getNumberValue());
        assertNull(converter.fromValue(
                Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build()));
        assertNull(converter.fromValue(null));

        DescriptorProtos.DescriptorProto msg = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("M")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("n").setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .build())
                .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("m.proto").setPackage("m").addMessageType(msg).build();
        Descriptor d = FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("M");
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(new Object(), d.findFieldByName("n")));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType("not-a-number", d.findFieldByName("n")));

        Struct empty = Struct.getDefaultInstance();
        assertTrue(converter.messageToStruct(empty).getFieldsMap().isEmpty());
        assertEquals(empty, converter.structToMessage(empty, Struct.getDescriptor()));
    }

    @Test void anyHandlerTypeNameAndMismatch() {
        AnyHandler handler = new AnyHandler();
        com.google.protobuf.Any any = handler.pack(Struct.getDefaultInstance());
        assertEquals("google.protobuf.Struct", handler.getTypeName(any));
        assertEquals("type.googleapis.com/google.protobuf.Struct", handler.createTypeUrl("google.protobuf.Struct"));
        assertTrue(handler.is(any, Struct.getDescriptor()));
        assertTrue(handler.is(any, "google.protobuf.Struct"));
        assertFalse(handler.is(any, "other.Type"));
        assertNull(handler.unpackSafe(
                com.google.protobuf.Any.newBuilder().setTypeUrl("type.googleapis.com/nope.Type").build()));
    }

}

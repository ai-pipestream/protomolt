package ai.protomolt.proto.helpers;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the AnyHandler class.
 */
public class AnyHandlerTest {

    private static AnyHandler handler;
    private static Descriptor personDescriptor;
    private static DescriptorRegistry registry;

    @BeforeAll
    static void setUp() throws DescriptorValidationException {
        FileDescriptor fileDescriptor = createTestFileDescriptor();
        personDescriptor = fileDescriptor.findMessageTypeByName("Person");
        assertNotNull(personDescriptor);

        registry = new DescriptorRegistry();
        registry.registerFile(fileDescriptor);
        handler = new AnyHandler(registry);
    }

    @Test
    void testPackMessage() {
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "John Doe")
                .setField(personDescriptor.findFieldByName("age"), 30L)
                .setField(personDescriptor.findFieldByName("email"), "john@example.com")
                .build();

        Any packedAny = handler.pack(person);
        assertNotNull(packedAny);
        assertTrue(packedAny.getTypeUrl().contains("Person"));
        assertFalse(packedAny.getValue().isEmpty());
    }

    @Test
    void testUnpackMessage() throws Exception {
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "Jane Smith")
                .setField(personDescriptor.findFieldByName("age"), 25L)
                .setField(personDescriptor.findFieldByName("email"), "jane@example.com")
                .build();

        Any packedAny = handler.pack(person);
        Message unpackedPerson = handler.unpack(packedAny);

        assertNotNull(unpackedPerson);
        assertEquals("Jane Smith", unpackedPerson.getField(personDescriptor.findFieldByName("name")));
        assertEquals(25L, unpackedPerson.getField(personDescriptor.findFieldByName("age")));
        assertEquals("jane@example.com", unpackedPerson.getField(personDescriptor.findFieldByName("email")));
    }

    @Test
    void testUnpackWithDescriptor() throws Exception {
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "Bob Johnson")
                .setField(personDescriptor.findFieldByName("age"), 40L)
                .build();

        Any packedAny = handler.pack(person);
        DynamicMessage unpackedPerson = handler.unpackToDynamic(packedAny, personDescriptor);

        assertNotNull(unpackedPerson);
        assertEquals("Bob Johnson", unpackedPerson.getField(personDescriptor.findFieldByName("name")));
        assertEquals(40L, unpackedPerson.getField(personDescriptor.findFieldByName("age")));
    }

    @Test
    void testIsType() {
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "Test Person")
                .build();

        Any packedAny = handler.pack(person);

        assertTrue(handler.is(packedAny, personDescriptor));
        assertTrue(handler.is(packedAny, "ai.pipestream.test.Person"));
        assertFalse(handler.is(packedAny, "some.other.Type"));
    }

    @Test
    void testGetTypeName() {
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "Type Test")
                .build();

        Any packedAny = handler.pack(person);
        String typeName = handler.getTypeName(packedAny);

        assertEquals("ai.pipestream.test.Person", typeName);
    }

    @Test
    void testCreateTypeUrl() {
        String typeUrl = handler.createTypeUrl("ai.pipestream.test.Person");
        assertEquals("type.googleapis.com/ai.pipestream.test.Person", typeUrl);

        // Test idempotency - already has prefix
        String typeUrl2 = handler.createTypeUrl("type.googleapis.com/ai.pipestream.test.Person");
        assertEquals("type.googleapis.com/ai.pipestream.test.Person", typeUrl2);
    }

    @Test
    void testUnpackToStruct() throws Exception {
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "Struct Person")
                .setField(personDescriptor.findFieldByName("age"), 35L)
                .setField(personDescriptor.findFieldByName("email"), "struct@example.com")
                .build();

        Any packedAny = handler.pack(person);
        Struct struct = handler.unpackToStruct(packedAny);

        assertNotNull(struct);
        assertEquals("Struct Person", struct.getFieldsOrThrow("name").getStringValue());
        // int64 encodes as a string per the proto3 JSON convention
        assertEquals("35", struct.getFieldsOrThrow("age").getStringValue());
        assertEquals("struct@example.com", struct.getFieldsOrThrow("email").getStringValue());
    }

    @Test
    void testPackStruct() throws Exception {
        Struct struct = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("Packed Person").build())
                .putFields("age", Value.newBuilder().setNumberValue(45).build())
                .putFields("email", Value.newBuilder().setStringValue("packed@example.com").build())
                .build();

        Any packedAny = handler.packStruct(struct, "ai.pipestream.test.Person");
        assertNotNull(packedAny);
        assertTrue(packedAny.getTypeUrl().contains("Person"));

        // Unpack and verify
        Message unpacked = handler.unpack(packedAny);
        assertEquals("Packed Person", unpacked.getField(personDescriptor.findFieldByName("name")));
        assertEquals(45L, unpacked.getField(personDescriptor.findFieldByName("age")));
    }

    @Test
    void testPackStructWithUnknownType() {
        Struct struct = Struct.newBuilder()
                .putFields("field", Value.newBuilder().setStringValue("value").build())
                .build();

        assertThrows(IllegalArgumentException.class, () -> {
            handler.packStruct(struct, "unknown.UnknownType");
        });
    }

    @Test
    void testUnpackUnknownType() {
        Any unknownAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unknown.UnknownType")
                .setValue(ByteString.copyFromUtf8("data"))
                .build();

        assertThrows(InvalidProtocolBufferException.class, () -> {
            handler.unpack(unknownAny);
        });
    }

    @Test
    void testUnpackSafe() {
        // Valid unpack
        Message person = DynamicMessage.newBuilder(personDescriptor)
                .setField(personDescriptor.findFieldByName("name"), "Safe Person")
                .build();
        Any validAny = handler.pack(person);
        Message unpacked = handler.unpackSafe(validAny);
        assertNotNull(unpacked);

        // Invalid unpack
        Any invalidAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/invalid.Type")
                .setValue(ByteString.copyFromUtf8("data"))
                .build();
        Message unpackedInvalid = handler.unpackSafe(invalidAny);
        assertNull(unpackedInvalid);
    }

    @Test
    void testGetDescriptorRegistry() {
        assertSame(registry, handler.getDescriptorRegistry());
    }

    private static FileDescriptor createTestFileDescriptor() throws DescriptorValidationException {
        DescriptorProto personProto = DescriptorProto.newBuilder()
                .setName("Person")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("name").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("age").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_INT64))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("email").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_any.proto")
                .setPackage("ai.pipestream.test")
                .addMessageType(personProto)
                .build();

        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{});
    }
}

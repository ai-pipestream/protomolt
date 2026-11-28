package ai.protomolt.proto.helpers;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exact unsigned and signed integral boundary tests for {@link TypeConverter} (#987).
 */
class TypeConverterUnsignedTest {

    private static final long MAX_UINT32 = 0xFFFF_FFFFL;
    private static final long HIGH_UINT32 = 3_000_000_000L;
    private static final BigInteger MAX_UINT64_BIG =
            BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final String MAX_UINT64_TEXT = "18446744073709551615";

    private static TypeConverter converter;
    private static Descriptor unsignedDoc;

    @BeforeAll
    static void setUp() throws DescriptorValidationException {
        converter = new TypeConverter();
        DescriptorProto msg = DescriptorProto.newBuilder()
                .setName("UnsignedDoc")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("u32").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_UINT32))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("f32").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_FIXED32))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("u64").setNumber(3).setType(FieldDescriptorProto.Type.TYPE_UINT64))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("f64").setNumber(4).setType(FieldDescriptorProto.Type.TYPE_FIXED64))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("s32").setNumber(5).setType(FieldDescriptorProto.Type.TYPE_INT32))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("s64").setNumber(6).setType(FieldDescriptorProto.Type.TYPE_INT64))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("text").setNumber(7).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        FileDescriptor file = FileDescriptor.buildFrom(
                FileDescriptorProto.newBuilder()
                        .setName("unsigned_doc.proto")
                        .setSyntax("proto3")
                        .setPackage("ai.protomolt.test.unsigned.v1")
                        .addMessageType(msg)
                        .build(),
                new FileDescriptor[0]);
        unsignedDoc = file.findMessageTypeByName("UnsignedDoc");
    }

    private static FieldDescriptor field(String name) {
        return unsignedDoc.findFieldByName(name);
    }

    @Test
    void acceptsFullUint32AndFixed32RangeAcrossInputTypes() throws Exception {
        for (String name : new String[]{"u32", "f32"}) {
            FieldDescriptor fd = field(name);
            assertEquals(0, converter.convertToFieldType(0, fd));
            assertEquals((int) HIGH_UINT32, converter.convertToFieldType(HIGH_UINT32, fd));
            assertEquals((int) MAX_UINT32, converter.convertToFieldType(MAX_UINT32, fd));
            assertEquals((int) MAX_UINT32, converter.convertToFieldType("4294967295", fd));
            assertEquals((int) HIGH_UINT32, converter.convertToFieldType((double) HIGH_UINT32, fd));
            assertEquals((int) MAX_UINT32,
                    converter.convertToFieldType(BigInteger.valueOf(MAX_UINT32), fd));

            DynamicMessage built = DynamicMessage.newBuilder(unsignedDoc)
                    .setField(fd, converter.convertToFieldType(MAX_UINT32, fd))
                    .build();
            DynamicMessage wireDecoded = DynamicMessage.parseFrom(unsignedDoc, built.toByteString());
            assertEquals(MAX_UINT32, Integer.toUnsignedLong((Integer) wireDecoded.getField(fd)));
        }
    }

    @Test
    void rejectsNegativeFractionalAndOverflowingValuesForUint32() {
        FieldDescriptor u32 = field("u32");
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(-1, u32));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(-1L, u32));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType("-1", u32));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(-1.0, u32));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(MAX_UINT32 + 1L, u32));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType("4294967296", u32));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(BigInteger.valueOf(MAX_UINT32 + 1L), u32));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(2.5, u32));
    }

    @Test
    void acceptsFullUint64AndFixed64RangeAcrossInputTypes() throws Exception {
        for (String name : new String[]{"u64", "f64"}) {
            FieldDescriptor fd = field(name);
            assertEquals(0L, converter.convertToFieldType(0L, fd));
            assertEquals(-1L, converter.convertToFieldType(MAX_UINT64_TEXT, fd));
            assertEquals(-1L, converter.convertToFieldType(MAX_UINT64_BIG, fd));
            // 2^63 is exact in IEEE-754 double and valid for uint64, though outside signed int64.
            assertEquals(Long.MIN_VALUE, converter.convertToFieldType(0x1.0p63, fd));

            DynamicMessage built = DynamicMessage.newBuilder(unsignedDoc)
                    .setField(fd, converter.convertToFieldType(MAX_UINT64_BIG, fd))
                    .build();
            DynamicMessage wireDecoded = DynamicMessage.parseFrom(unsignedDoc, built.toByteString());
            assertEquals(MAX_UINT64_TEXT, Long.toUnsignedString((Long) wireDecoded.getField(fd)));
        }
    }

    @Test
    void rejectsNegativeFractionalAndOverflowingValuesForUint64() {
        FieldDescriptor u64 = field("u64");
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(-1, u64));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(-1L, u64));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType("-1", u64));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(-1.0, u64));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType("18446744073709551616", u64));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(MAX_UINT64_BIG.add(BigInteger.ONE), u64));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(0x1.0p64, u64));
        assertThrows(IllegalArgumentException.class, () -> converter.convertToFieldType(1.25, u64));
    }

    @Test
    void bigIntegerCoercesToSignedTypesAndStringsWithoutPrecisionLoss() {
        FieldDescriptor s32 = field("s32");
        FieldDescriptor s64 = field("s64");
        FieldDescriptor text = field("text");

        assertEquals(Integer.MAX_VALUE,
                converter.convertToFieldType(BigInteger.valueOf(Integer.MAX_VALUE), s32));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(BigInteger.valueOf(HIGH_UINT32), s32));

        assertEquals(HIGH_UINT32,
                converter.convertToFieldType(BigInteger.valueOf(HIGH_UINT32), s64));
        assertEquals(Long.MAX_VALUE,
                converter.convertToFieldType(BigInteger.valueOf(Long.MAX_VALUE), s64));
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(MAX_UINT64_BIG, s64));
        // 2^63 saturates Java's (long) cast to Long.MAX_VALUE; signed int64 must reject it.
        assertThrows(IllegalArgumentException.class,
                () -> converter.convertToFieldType(0x1.0p63, s64));

        assertEquals(MAX_UINT64_TEXT,
                converter.convertToFieldType(MAX_UINT64_BIG, text));
    }

    @Test
    void structRoundTripPreservesUnsigned32AndUnsigned64Boundaries() {
        Message original = DynamicMessage.newBuilder(unsignedDoc)
                .setField(field("u32"), (int) MAX_UINT32)
                .setField(field("u64"), -1L)
                .build();

        Struct struct = converter.messageToStruct(original);
        assertEquals((double) MAX_UINT32, struct.getFieldsOrThrow("u32").getNumberValue());
        assertEquals(MAX_UINT64_TEXT, struct.getFieldsOrThrow("u64").getStringValue());

        DynamicMessage restored = converter.structToMessage(struct, unsignedDoc);
        assertEquals((int) MAX_UINT32, restored.getField(field("u32")));
        assertEquals(-1L, restored.getField(field("u64")));

        Struct negativeStruct = Struct.newBuilder()
                .putFields("u32", Value.newBuilder().setNumberValue(-1).build())
                .build();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> converter.structToMessage(negativeStruct, unsignedDoc));
        assertTrue(ex.getMessage().contains("u32"));
    }
}

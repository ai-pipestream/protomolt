package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Type conversion on writes: {@link ProtoFieldMapperImpl#setValue} and
 * {@link ProtoFieldMapperImpl#appendValue} funnel values through the type converter,
 * so strings, numbers and booleans land on differently-typed fields.
 */
class SetValueConversionTest {

    private static Descriptor item;
    private static EnumDescriptor status;

    private final ProtoFieldMapperImpl mapper = new ProtoFieldMapperImpl(new DescriptorRegistry());

    @BeforeAll
    static void compile() throws Exception {
        var compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("conv/item.proto", """
                        syntax = "proto3";
                        package conv;
                        enum Status { STATUS_UNKNOWN = 0; STATUS_ACTIVE = 1; STATUS_DISABLED = 2; }
                        message Item {
                          string name = 1;
                          int64 big_count = 2;
                          float ratio = 3;
                          bool active = 4;
                          Status status = 5;
                          bytes blob = 6;
                          repeated int64 big_counts = 7;
                        }
                        """, "conv").build());
        var file = compiled.descriptorFor("conv/item.proto").orElseThrow();
        item = file.findMessageTypeByName("Item");
        status = file.findEnumTypeByName("Status");
    }

    private static DynamicMessage.Builder item() {
        return DynamicMessage.newBuilder(item);
    }

    @Test
    void stringConvertsToInt64() throws Exception {
        var builder = item();
        mapper.setValue(builder, "big_count", "123");
        assertEquals(123L, mapper.getValue(builder.build(), "big_count"));
    }

    @Test
    void integralDoubleConvertsToInt64() throws Exception {
        var builder = item();
        mapper.setValue(builder, "big_count", 7.0d);
        assertEquals(7L, mapper.getValue(builder.build(), "big_count"));
    }

    @Test
    void nonIntegralNumberRejectedForInt64() {
        var builder = item();
        assertThrows(IllegalArgumentException.class,
                () -> mapper.setValue(builder, "big_count", 1.5d));
    }

    @Test
    void nonIntegralNumberViaRuleIsWrappedInMappingException() {
        var builder = item();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.mapInPlace(builder, List.of("big_count = 1.5")));
        assertInstanceOf(IllegalArgumentException.class, e.getCause());
    }

    @Test
    void doubleConvertsToFloat() throws Exception {
        var builder = item();
        mapper.setValue(builder, "ratio", 0.5d);
        assertEquals(0.5f, mapper.getValue(builder.build(), "ratio"));
    }

    @Test
    void stringConvertsToBoolean() throws Exception {
        var builder = item();
        mapper.setValue(builder, "active", "true");
        assertEquals(true, mapper.getValue(builder.build(), "active"));
    }

    @Test
    void numberConvertsToString() throws Exception {
        var builder = item();
        mapper.setValue(builder, "name", 42L);
        assertEquals("42", mapper.getValue(builder.build(), "name"));
    }

    @Test
    void enumSetsViaEnumValueDescriptor() throws Exception {
        var builder = item();
        EnumValueDescriptor active = status.findValueByName("STATUS_ACTIVE");
        mapper.setValue(builder, "status", active);
        Object read = mapper.getValue(builder.build(), "status");
        assertEquals("STATUS_ACTIVE", ((EnumValueDescriptor) read).getName());
    }

    @Test
    void bytesRoundTrip() throws Exception {
        var builder = item();
        ByteString blob = ByteString.copyFromUtf8("payload");
        mapper.setValue(builder, "blob", blob);
        assertEquals(blob, mapper.getValue(builder.build(), "blob"));
    }

    @Test
    void appendListConvertsEachElementToTheFieldType() throws Exception {
        var builder = item();
        mapper.appendValue(builder, "big_counts", List.of(1, "2", 3.0d));
        assertEquals(List.of(1L, 2L, 3L), mapper.getValue(builder.build(), "big_counts"));
    }

    @Test
    void wholeStructAssignsToStructTypedField() throws Exception {
        Struct struct = Struct.newBuilder()
                .putFields("k", Value.newBuilder().setStringValue("v").build())
                .build();
        var builder = TestDescriptors.document();
        mapper.setValue(builder, "metadata", struct);
        assertEquals("v", mapper.getValue(builder.build(), "metadata.k"));
    }

    @Test
    void failedConversionMessageNamesTheRule() {
        var builder = item();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.mapInPlace(builder, List.of("name = \"abc\"", "big_count = name")));
        assertTrue(e.getMessage().contains("big_count = name"), e.getMessage());
        assertInstanceOf(NumberFormatException.class, e.getCause());
    }
}

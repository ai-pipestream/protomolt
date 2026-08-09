package ai.pipestream.proto.emit.parquet;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The descriptor-to-Parquet mapping itself, column by column: primitive widening and
 * annotations, presence-driven repetition, the three-level list and map shapes, field-id
 * stamping, and projection filtering. Round-trip behavior lives in {@code ParquetEmitterTest};
 * here the schema is the product under test.
 */
class ProtoParquetSchemasTest {

    private static final String PROTO = """
            syntax = "proto3";
            package pq.schema;
            import "google/protobuf/timestamp.proto";
            import "google/protobuf/struct.proto";
            message Scalars {
              int32 i32 = 1;
              sint32 s32 = 2;
              sfixed32 sf32 = 3;
              uint32 u32 = 4;
              fixed32 f32 = 5;
              int64 i64 = 6;
              sint64 s64 = 7;
              sfixed64 sf64 = 8;
              uint64 u64 = 9;
              fixed64 f64 = 10;
              float flt = 11;
              double dbl = 12;
              bool flag = 13;
              string text = 14;
              bytes blob = 15;
              Kind kind = 16;
              optional int32 maybe = 17;
              Child child = 18;
              repeated int64 numbers = 19;
              map<string, int64> counts = 20;
              map<string, Child> children = 21;
              map<string, google.protobuf.Timestamp> stamps = 22;
              google.protobuf.Timestamp at = 23;
              google.protobuf.Value anything = 24;
              google.protobuf.ListValue list_of_anything = 25;
            }
            message Child { string name = 1; }
            enum Kind { KIND_UNSPECIFIED = 0; KIND_A = 1; }
            message Node { Node next = 1; }
            """;

    private static final String PROTO2 = """
            syntax = "proto2";
            package pq.legacy;
            message Old {
              required string req = 1;
              optional int32 opt = 2;
            }
            """;

    private static FileDescriptor file() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pq/schema/scalars.proto", PROTO, "test").build());
        return compiled.descriptorFor("pq/schema/scalars.proto").orElseThrow();
    }

    private static MessageType schema() throws Exception {
        return ProtoParquetSchemas.schema(file().findMessageTypeByName("Scalars"));
    }

    private static PrimitiveType primitive(MessageType schema, String name) {
        return schema.getType(name).asPrimitiveType();
    }

    @Test
    void everyScalarFamilyMapsToItsSpecType() throws Exception {
        MessageType schema = schema();
        for (String name : new String[]{"i32", "s32", "sf32"}) {
            assertThat(primitive(schema, name).getPrimitiveTypeName())
                    .as(name).isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
        }
        // 32-bit unsigned and fixed-unsigned widen to int64 so no value ever changes sign.
        for (String name : new String[]{"u32", "f32", "i64", "s64", "sf64"}) {
            assertThat(primitive(schema, name).getPrimitiveTypeName())
                    .as(name).isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
            assertThat(primitive(schema, name).getLogicalTypeAnnotation()).as(name).isNull();
        }
        assertThat(primitive(schema, "u64").getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.intType(64, false));
        assertThat(primitive(schema, "f64").getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.intType(64, false));
        assertThat(primitive(schema, "flt").getPrimitiveTypeName())
                .isEqualTo(PrimitiveType.PrimitiveTypeName.FLOAT);
        assertThat(primitive(schema, "dbl").getPrimitiveTypeName())
                .isEqualTo(PrimitiveType.PrimitiveTypeName.DOUBLE);
        assertThat(primitive(schema, "flag").getPrimitiveTypeName())
                .isEqualTo(PrimitiveType.PrimitiveTypeName.BOOLEAN);
        assertThat(primitive(schema, "text").getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.stringType());
        assertThat(primitive(schema, "blob").getLogicalTypeAnnotation()).isNull();
        assertThat(primitive(schema, "kind").getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.enumType());
    }

    @Test
    void presenceDrivesRepetition() throws Exception {
        MessageType schema = schema();
        // proto3 plain scalars are required: defaults are values, not absence.
        assertThat(schema.getType("i32").getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
        // The optional keyword, singular messages, and well-known message types track presence.
        assertThat(schema.getType("maybe").getRepetition()).isEqualTo(Type.Repetition.OPTIONAL);
        assertThat(schema.getType("child").getRepetition()).isEqualTo(Type.Repetition.OPTIONAL);
        assertThat(schema.getType("at").getRepetition()).isEqualTo(Type.Repetition.OPTIONAL);
        assertThat(schema.getType("anything").getRepetition())
                .isEqualTo(Type.Repetition.OPTIONAL);
    }

    @Test
    void proto2FieldsTrackPresenceToo() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("pq/legacy/old.proto", PROTO2, "test").build());
        Descriptor old = compiled.descriptorFor("pq/legacy/old.proto").orElseThrow()
                .findMessageTypeByName("Old");
        MessageType schema = ProtoParquetSchemas.schema(old);
        assertThat(schema.getType("req").getRepetition()).isEqualTo(Type.Repetition.OPTIONAL);
        assertThat(schema.getType("opt").getRepetition()).isEqualTo(Type.Repetition.OPTIONAL);
    }

    @Test
    void wellKnownTypesGetLakeNativeColumns() throws Exception {
        MessageType schema = schema();
        PrimitiveType at = primitive(schema, "at");
        assertThat(at.getPrimitiveTypeName()).isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
        assertThat(at.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.timestampType(
                true, LogicalTypeAnnotation.TimeUnit.MICROS));
        assertThat(primitive(schema, "anything").getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.jsonType());
        assertThat(primitive(schema, "list_of_anything").getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.jsonType());
    }

    @Test
    void repeatedFieldsAreThreeLevelLists() throws Exception {
        GroupType numbers = schema().getType("numbers").asGroupType();
        assertThat(numbers.getRepetition()).isEqualTo(Type.Repetition.OPTIONAL);
        assertThat(numbers.getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.listType());
        GroupType list = numbers.getType("list").asGroupType();
        assertThat(list.getRepetition()).isEqualTo(Type.Repetition.REPEATED);
        Type element = list.getType("element");
        assertThat(element.getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
        assertThat(element.asPrimitiveType().getPrimitiveTypeName())
                .isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
    }

    @Test
    void mapsAreAnnotatedKeyValueGroups() throws Exception {
        MessageType schema = schema();

        GroupType counts = schema.getType("counts").asGroupType();
        assertThat(counts.getLogicalTypeAnnotation()).isEqualTo(LogicalTypeAnnotation.mapType());
        GroupType keyValue = counts.getType("key_value").asGroupType();
        assertThat(keyValue.getRepetition()).isEqualTo(Type.Repetition.REPEATED);
        assertThat(keyValue.getType("key").getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
        // Scalar map values are always present.
        assertThat(keyValue.getType("value").getRepetition()).isEqualTo(Type.Repetition.REQUIRED);

        // Message values track presence: the value column is optional.
        GroupType children = schema.getType("children").asGroupType()
                .getType("key_value").asGroupType();
        assertThat(children.getType("value").getRepetition())
                .isEqualTo(Type.Repetition.OPTIONAL);
        assertThat(children.getType("value").asGroupType().getType("name").getRepetition())
                .isEqualTo(Type.Repetition.REQUIRED);

        // A Timestamp map value is a required lake-native column, not a group.
        GroupType stamps = schema.getType("stamps").asGroupType()
                .getType("key_value").asGroupType();
        Type stampValue = stamps.getType("value");
        assertThat(stampValue.getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
        assertThat(stampValue.asPrimitiveType().getLogicalTypeAnnotation())
                .isEqualTo(LogicalTypeAnnotation.timestampType(true,
                        LogicalTypeAnnotation.TimeUnit.MICROS));
    }

    @Test
    void fieldIdsStampTheColumnsTheyResolve() throws Exception {
        Descriptor type = file().findMessageTypeByName("Scalars");
        ProtoParquetSchemas.FieldIdResolver ids = path -> switch (path) {
            case "text" -> 1;
            case "child" -> 2;
            case "child.name" -> 3;
            case "counts" -> 4;
            case "counts.key" -> 5;
            case "counts.value" -> 6;
            case "numbers.element" -> 7;
            default -> null;
        };
        MessageType withIds = ProtoParquetSchemas.schema(type, ids);
        assertThat(withIds.getType("text").getId().intValue()).isEqualTo(1);
        assertThat(withIds.getType("child").getId().intValue()).isEqualTo(2);
        assertThat(withIds.getType("child").asGroupType().getType("name").getId().intValue())
                .isEqualTo(3);
        assertThat(withIds.getType("counts").getId().intValue()).isEqualTo(4);
        GroupType keyValue = withIds.getType("counts").asGroupType()
                .getType("key_value").asGroupType();
        assertThat(keyValue.getType("key").getId().intValue()).isEqualTo(5);
        assertThat(keyValue.getType("value").getId().intValue()).isEqualTo(6);
        // Structural nodes the resolver names nothing for stay id-less.
        assertThat(keyValue.getId()).isNull();
        assertThat(withIds.getType("i32").getId()).isNull();
        assertThat(withIds.getType("numbers").asGroupType().getType("list").asGroupType()
                .getType("element").getId().intValue()).isEqualTo(7);

        // The no-resolver default leaves every node id-less.
        MessageType plain = ProtoParquetSchemas.schema(type);
        assertThat(plain.getType("text").getId()).isNull();
        assertThat(plain.getType("child").getId()).isNull();
    }

    @Test
    void projectionFiltersTopLevelColumnsOnly() throws Exception {
        Descriptor type = file().findMessageTypeByName("Scalars");

        MessageType projected = ProtoParquetSchemas.schema(type,
                ProtoParquetSchemas.FieldIdResolver.NONE, Set.of("text", "child"));
        assertThat(projected.getFields()).extracting(Type::getName)
                .containsExactly("text", "child");
        // Nested structure under a kept column is untouched.
        assertThat(projected.getType("child").asGroupType().containsField("name")).isTrue();

        // Empty (or null) projection keeps every column.
        assertThat(ProtoParquetSchemas.schema(type, ProtoParquetSchemas.FieldIdResolver.NONE,
                Set.of()).getFields()).hasSize(type.getFields().size());
        assertThat(ProtoParquetSchemas.schema(type, ProtoParquetSchemas.FieldIdResolver.NONE,
                null).getFields()).hasSize(type.getFields().size());

        // A projection naming only unknown columns is a caller error, not an empty file.
        assertThatThrownBy(() -> ProtoParquetSchemas.schema(type,
                ProtoParquetSchemas.FieldIdResolver.NONE, Set.of("nope")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Projection selected no columns")
                .hasMessageContaining("nope");
    }

    @Test
    void recursionIsRejectedWithTheCycleNamed() throws Exception {
        Descriptor node = file().findMessageTypeByName("Node");
        assertThatThrownBy(() -> ProtoParquetSchemas.schema(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive message type")
                .hasMessageContaining("pq.schema.Node");
    }
}

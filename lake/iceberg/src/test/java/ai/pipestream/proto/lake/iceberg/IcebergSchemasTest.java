package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The pure descriptor&lt;-&gt;Iceberg-schema converter, exercised directly rather than through the
 * sink: every scalar's target type, the JSON well-known types collapsed to strings, timestamps
 * as zoned, the recursion guard, and the reverse direction's awkward corners (illegal column
 * names, non-integral map keys that cannot be proto map keys, a struct whose name collides with
 * its parent). These are the branches the append tests never reach.
 */
class IcebergSchemasTest {

    private static FileDescriptor scalars;
    private static FileDescriptor wellKnown;
    private static FileDescriptor shapes;
    private static FileDescriptor recursive;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("s/scalars.proto", """
                        syntax = "proto3";
                        package s;
                        enum Color { UNKNOWN = 0; RED = 1; }
                        message Scalars {
                          int32 i32 = 1;  sint32 s32 = 2;  sfixed32 sf32 = 3;
                          uint32 u32 = 4;  fixed32 f32 = 5;
                          int64 i64 = 6;   sint64 s64 = 7;  sfixed64 sf64 = 8;
                          uint64 u64 = 9;  fixed64 f64 = 10;
                          float fl = 11;   double db = 12;  bool bo = 13;
                          string st = 14;  Color col = 15;  bytes by = 16;
                        }
                        """, "test")
                .add("w/wk.proto", """
                        syntax = "proto3";
                        package w;
                        import "google/protobuf/timestamp.proto";
                        import "google/protobuf/struct.proto";
                        message WellKnown {
                          google.protobuf.Timestamp at = 1;
                          google.protobuf.Struct s = 2;
                          google.protobuf.Value v = 3;
                          google.protobuf.ListValue lv = 4;
                        }
                        """, "test")
                .add("h/shapes.proto", """
                        syntax = "proto3";
                        package h;
                        message Container {
                          repeated string tags = 1;
                          map<string, int64> attrs = 2;
                          Inner inner = 3;
                        }
                        message Inner { string name = 1; }
                        """, "test")
                .add("r/recursive.proto", """
                        syntax = "proto3";
                        package r;
                        message Node { string v = 1; Node next = 2; }
                        message A { B b = 1; }
                        message B { A a = 1; }
                        """, "test")
                .build());
        scalars = compiled.descriptorFor("s/scalars.proto").orElseThrow();
        wellKnown = compiled.descriptorFor("w/wk.proto").orElseThrow();
        shapes = compiled.descriptorFor("h/shapes.proto").orElseThrow();
        recursive = compiled.descriptorFor("r/recursive.proto").orElseThrow();
    }

    @Test
    void everyScalarMapsToItsIcebergType() {
        Schema schema = IcebergSchemas.fromDescriptor(scalars.findMessageTypeByName("Scalars"));
        // 32-bit signed integrals stay 32-bit; everything wider (incl. unsigned 32) widens to long.
        assertThat(schema.findField("i32").type()).isInstanceOf(Types.IntegerType.class);
        assertThat(schema.findField("s32").type()).isInstanceOf(Types.IntegerType.class);
        assertThat(schema.findField("sf32").type()).isInstanceOf(Types.IntegerType.class);
        assertThat(schema.findField("u32").type()).isInstanceOf(Types.LongType.class);
        assertThat(schema.findField("f32").type()).isInstanceOf(Types.LongType.class);
        assertThat(schema.findField("i64").type()).isInstanceOf(Types.LongType.class);
        assertThat(schema.findField("u64").type()).isInstanceOf(Types.LongType.class);
        assertThat(schema.findField("fl").type()).isInstanceOf(Types.FloatType.class);
        assertThat(schema.findField("db").type()).isInstanceOf(Types.DoubleType.class);
        assertThat(schema.findField("bo").type()).isInstanceOf(Types.BooleanType.class);
        assertThat(schema.findField("st").type()).isInstanceOf(Types.StringType.class);
        // Enums carry their symbolic name, so they land as strings.
        assertThat(schema.findField("col").type()).isInstanceOf(Types.StringType.class);
        assertThat(schema.findField("by").type()).isInstanceOf(Types.BinaryType.class);
    }

    @Test
    void timestampIsZonedAndJsonWellKnownTypesBecomeStrings() {
        Schema schema = IcebergSchemas.fromDescriptor(wellKnown.findMessageTypeByName("WellKnown"));
        assertThat(schema.findField("at").type()).isInstanceOf(Types.TimestampType.class);
        assertThat(((Types.TimestampType) schema.findField("at").type()).shouldAdjustToUTC())
                .as("google.protobuf.Timestamp maps to timestamptz").isTrue();
        // Struct/Value/ListValue have no flat Iceberg shape, so they are carried as JSON text.
        assertThat(schema.findField("s").type()).isInstanceOf(Types.StringType.class);
        assertThat(schema.findField("v").type()).isInstanceOf(Types.StringType.class);
        assertThat(schema.findField("lv").type()).isInstanceOf(Types.StringType.class);
    }

    @Test
    void repeatedMapAndNestedFieldsGetTheirContainerTypes() {
        Schema schema = IcebergSchemas.fromDescriptor(shapes.findMessageTypeByName("Container"));
        assertThat(schema.findField("tags").type().isListType()).isTrue();
        assertThat(schema.findField("tags").type().asListType().elementType())
                .isInstanceOf(Types.StringType.class);
        assertThat(schema.findField("attrs").type().isMapType()).isTrue();
        assertThat(schema.findField("attrs").type().asMapType().keyType())
                .isInstanceOf(Types.StringType.class);
        assertThat(schema.findField("attrs").type().asMapType().valueType())
                .isInstanceOf(Types.LongType.class);
        assertThat(schema.findField("inner").type().isStructType()).isTrue();
        assertThat(schema.findField("inner.name").type()).isInstanceOf(Types.StringType.class);
    }

    @Test
    void directlyRecursiveMessagesAreRejectedNotStackOverflowed() {
        assertThatThrownBy(() ->
                IcebergSchemas.fromDescriptor(recursive.findMessageTypeByName("Node")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive")
                .hasMessageContaining("Node");
    }

    @Test
    void indirectlyRecursiveMessagesAreRejected() {
        assertThatThrownBy(() ->
                IcebergSchemas.fromDescriptor(recursive.findMessageTypeByName("A")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Recursive");
    }

    // ---------------------------------------------------------------- iceberg -> proto corners

    @Test
    void illegalColumnNamesAreSanitizedIntoCompilableProto() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "1st-field", Types.StringType.get()),
                Types.NestedField.optional(2, "has spaces!", Types.LongType.get()));
        String source = IcebergSchemas.toProtoSource(schema, "gen.v1", "Row");

        // A leading digit gets an 'f' prefix; every non-identifier char becomes '_'.
        assertThat(source).contains("f1st_field").contains("has_spaces_");
        // Proof it is legal proto: it recompiles.
        assertThat(recompile(source, "Row").findFieldByName("f1st_field")).isNotNull();
    }

    @Test
    void mapKeysThatCannotBeProtoMapKeysDegradeToEntryMessages() throws Exception {
        // Proto map keys must be integral or string; a float-keyed Iceberg map cannot be one, so
        // it must become a repeated entry message rather than silently dropping the key type.
        Schema schema = new Schema(Types.NestedField.optional(1, "prices",
                Types.MapType.ofOptional(2, 3, Types.FloatType.get(), Types.DoubleType.get())));
        String source = IcebergSchemas.toProtoSource(schema, "gen.v1", "Row");

        assertThat(source).contains("repeated PricesEntry prices");
        assertThat(source).contains("message PricesEntry");
        assertThat(source).doesNotContain("map<");
        Descriptor row = recompile(source, "Row");
        assertThat(row.findFieldByName("prices").isRepeated()).isTrue();
    }

    @Test
    void integralAndStringMapKeysStayAsProtoMaps() {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "byName",
                        Types.MapType.ofOptional(2, 3, Types.StringType.get(),
                                Types.LongType.get())),
                Types.NestedField.optional(4, "byId",
                        Types.MapType.ofOptional(5, 6, Types.IntegerType.get(),
                                Types.StringType.get())));
        String source = IcebergSchemas.toProtoSource(schema, "gen.v1", "Row");
        assertThat(source).contains("map<string, int64> byName");
        assertThat(source).contains("map<int32, string> byId");
    }

    @Test
    void aStructFieldNamedLikeItsParentGetsASuffixToAvoidCollision() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "row",
                Types.StructType.of(Types.NestedField.optional(2, "x", Types.LongType.get()))));
        String source = IcebergSchemas.toProtoSource(schema, "gen.v1", "Row");

        // The generated nested message would be "Row", colliding with the top message, so "Struct".
        assertThat(source).contains("message RowStruct").contains("RowStruct row");
        assertThat(recompile(source, "Row").findFieldByName("row").getMessageType().getName())
                .isEqualTo("RowStruct");
    }

    private static Descriptor recompile(String source, String message) throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("gen/v1/row.proto", source, "gen").build());
        return compiled.descriptorFor("gen/v1/row.proto").orElseThrow()
                .findMessageTypeByName(message);
    }
}

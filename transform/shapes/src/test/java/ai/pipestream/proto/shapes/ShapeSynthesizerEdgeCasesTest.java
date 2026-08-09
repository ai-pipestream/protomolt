package ai.pipestream.proto.shapes;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Shape-synthesis edges the end-to-end tests do not reach: identifier validation, empty
 * shapes, path resolution failures (whole sources, maps), enum and message fields carrying
 * their types and dependencies into the shape, and the emitted file naming.
 */
class ShapeSynthesizerEdgeCasesTest {

    private static final String PROTO = """
            syntax = "proto3";
            package edge.v1;
            message Order {
              string id = 1;
              Status status = 2;
              Address ship_to = 3;
              repeated string tags = 4;
              map<string, string> attrs = 5;
            }
            message Address {
              string city = 1;
            }
            enum Status {
              STATUS_UNSPECIFIED = 0;
              OPEN = 1;
            }
            """;

    private static Descriptor order;
    private static Descriptor address;

    private final ShapeSynthesizer synthesizer = new ShapeSynthesizer();

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("edge/v1/edge.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("edge/v1/edge.proto").orElseThrow();
        order = file.findMessageTypeByName("Order");
        address = file.findMessageTypeByName("Address");
    }

    private static List<ShapeSynthesizer.NamedType> sources() {
        return List.of(new ShapeSynthesizer.NamedType("order", order));
    }

    @Test
    void recordComponentsAreValidated() {
        assertThatThrownBy(() -> new ShapeSynthesizer.NamedType("not valid", order))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a valid source name");
        assertThatThrownBy(() -> new ShapeSynthesizer.NamedType("order", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ShapeSynthesizer.ProjectedField("9bad", "order.id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a valid field name");
        assertThatThrownBy(() -> new ShapeSynthesizer.ProjectedField("ok", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ShapeSynthesizer.NamedField("bad name",
                        order.findFieldByName("id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a valid field name");
    }

    @Test
    void shapesNeedAtLeastOneSourceAndOneField() {
        assertThatThrownBy(() -> synthesizer.envelope("derived.v1.Empty", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one source");
        assertThatThrownBy(() -> synthesizer.taggedUnion("derived.v1.Empty", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one source");
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Empty", sources(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one field");
        assertThatThrownBy(() -> synthesizer.fromFields("derived.v1.Empty", List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one field");
    }

    @Test
    void messageNamesMustBeIdentifiers() {
        assertThatThrownBy(() -> synthesizer.envelope("derived.v1.Not Valid", sources()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a valid message name");
    }

    @Test
    void duplicateProjectedFieldsAreRejected() {
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Dup", sources(),
                List.of(new ShapeSynthesizer.ProjectedField("x", "order.id"),
                        new ShapeSynthesizer.ProjectedField("x", "order.id"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate projected field: x");
    }

    @Test
    void projectingAWholeSourceOrAMapFailsClearly() {
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Bad", sources(),
                List.of(new ShapeSynthesizer.ProjectedField("x", "order"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names a whole source");
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Bad", sources(),
                List.of(new ShapeSynthesizer.ProjectedField("x", "order.attrs"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be projected");
        // The same rejection fires one level down, when the field is carried into a shape.
        assertThatThrownBy(() -> synthesizer.fromFields("derived.v1.Bad",
                List.of(new ShapeSynthesizer.NamedField("attrs", order.findFieldByName("attrs"))),
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be carried into a synthesized shape yet");
    }

    @Test
    void descendingThroughAScalarFailsClearly() {
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Bad", sources(),
                List.of(new ShapeSynthesizer.ProjectedField("x", "order.id.deeper"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a singular message");
    }

    @Test
    void enumFieldsCarryTheirTypeAndDependencyIntoTheShape() {
        var shape = synthesizer.projection("derived.v1.EnumCarrier", sources(),
                List.of(new ShapeSynthesizer.ProjectedField("status", "order.status")));
        FieldDescriptor status = shape.type().findFieldByName("status");
        assertThat(status.getJavaType()).isEqualTo(FieldDescriptor.JavaType.ENUM);
        assertThat(status.getEnumType().getFullName()).isEqualTo("edge.v1.Status");
        assertThat(shape.protoSource())
                .contains("import \"edge/v1/edge.proto\";")
                .contains("edge.v1.Status status = 1;");
        assertThat(shape.descriptorSet().getFileList())
                .extracting(FileDescriptorProto::getName)
                .containsExactly("edge/v1/edge.proto", "derived/v1/enum_carrier.proto");
    }

    @Test
    void messageAndRepeatedFieldsKeepTheirShape() {
        var shape = synthesizer.projection("derived.v1.Carrier", sources(),
                List.of(new ShapeSynthesizer.ProjectedField("ship_to", "order.ship_to"),
                        new ShapeSynthesizer.ProjectedField("tags", "order.tags")));
        FieldDescriptor shipTo = shape.type().findFieldByName("ship_to");
        assertThat(shipTo.getJavaType()).isEqualTo(FieldDescriptor.JavaType.MESSAGE);
        assertThat(shipTo.getMessageType().getFullName()).isEqualTo("edge.v1.Address");
        assertThat(shape.type().findFieldByName("tags").isRepeated()).isTrue();
        // The implied rules make the projection joinable with no ruleset at all.
        assertThat(shape.impliedRules())
                .containsExactly("ship_to = order.ship_to", "tags = order.tags");
    }

    @Test
    void fileNamesAreThePackagePathPlusSnakeCasedSimpleName() {
        var packaged = synthesizer.envelope("derived.v1.OrderSummary",
                List.of(new ShapeSynthesizer.NamedType("order", order)));
        assertThat(packaged.file().getName()).isEqualTo("derived/v1/order_summary.proto");
        assertThat(packaged.protoSource()).startsWith("syntax = \"proto3\";\n\npackage derived.v1;");

        var bare = synthesizer.envelope("Pair",
                List.of(new ShapeSynthesizer.NamedType("order", order)));
        assertThat(bare.file().getName()).isEqualTo("pair.proto");
        assertThat(bare.protoSource()).doesNotContain("package ");
    }

    @Test
    void twoSourcesFromOneFileImportItOnce() {
        var shape = synthesizer.envelope("derived.v1.DoubleWrapped", List.of(
                new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("address", address)));
        String importLine = "import \"edge/v1/edge.proto\";";
        String source = shape.protoSource();
        assertThat(source.indexOf(importLine)).isGreaterThanOrEqualTo(0);
        assertThat(source.indexOf(importLine, source.indexOf(importLine) + 1)).isEqualTo(-1);
    }

    @Test
    void taggedUnionSourceDeclaresTheOneofAndEveryCase() {
        var shape = synthesizer.taggedUnion("derived.v1.Either", List.of(
                new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("address", address)));
        assertThat(shape.protoSource())
                .contains("oneof value {")
                .contains("edge.v1.Order order = 1;")
                .contains("edge.v1.Address address = 2;");
        // A union carries no implied rules: wrapping is done by case, not by mapping.
        assertThat(shape.impliedRules()).isEmpty();
        assertThat(shape.type().getOneofs()).hasSize(1);
    }
}

package ai.pipestream.proto.shapes;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Inference edges beyond the happy path: the depth guard, degenerate arrays and objects,
 * the int64-versus-double boundary at 2^53, key sanitization (collisions, leading digits),
 * and the widening rules across samples (absent keys never narrow; mixed kinds fall back
 * to {@code Value}).
 */
class SchemaInferrerEdgeCasesTest {

    private static Struct struct(String json) throws Exception {
        Struct.Builder builder = Struct.newBuilder();
        JsonFormat.parser().merge(json, builder);
        return builder.build();
    }

    @Test
    void documentsNestingBeyondTheDepthGuardAreRejected() {
        Value current = Value.newBuilder().setStringValue("x").build();
        for (int i = 0; i < 40; i++) {
            Value inner = current;
            current = Value.newBuilder()
                    .setStructValue(Struct.newBuilder().putFields("a", inner))
                    .build();
        }
        Struct sample = current.getStructValue();
        assertThatThrownBy(() -> new SchemaInferrer().infer("x.Y", List.of(sample)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deeper than 32");
    }

    @Test
    void emptyAndNestedArraysFallBackToValue() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(struct("""
                {"empty": [], "nested": [[1, 2]], "mixed": [1, "two"]}
                """)));
        Descriptor type = shape.type();
        // No signal, no proto shape, mixed kinds: all three degrade to Value.
        assertThat(type.findFieldByName("empty").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Value");
        assertThat(type.findFieldByName("nested").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Value");
        assertThat(type.findFieldByName("mixed").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Value");
    }

    @Test
    void arrayElementsInferAndWidenLikeScalars() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(struct("""
                {"ints": [1, 2], "widened": [1, 2.5], "objects": [{"sku": "x"}]}
                """)));
        Descriptor type = shape.type();
        FieldDescriptor ints = type.findFieldByName("ints");
        assertThat(ints.isRepeated()).isTrue();
        assertThat(ints.getType()).isEqualTo(FieldDescriptor.Type.INT64);
        // One fractional element widens the whole array to double.
        FieldDescriptor widened = type.findFieldByName("widened");
        assertThat(widened.isRepeated()).isTrue();
        assertThat(widened.getType()).isEqualTo(FieldDescriptor.Type.DOUBLE);
        FieldDescriptor objects = type.findFieldByName("objects");
        assertThat(objects.isRepeated()).isTrue();
        assertThat(objects.getMessageType().findFieldByName("sku")).isNotNull();
    }

    @Test
    void integralNumbersAreInt64OnlyUpToTwoToThe53() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(struct("""
                {"exact": 9007199254740992, "huge": 10000000000000000, "negative": -5}
                """)));
        Descriptor type = shape.type();
        // 2^53 is still exact in a JSON double; past it the honest type is double.
        assertThat(type.findFieldByName("exact").getType())
                .isEqualTo(FieldDescriptor.Type.INT64);
        assertThat(type.findFieldByName("huge").getType())
                .isEqualTo(FieldDescriptor.Type.DOUBLE);
        assertThat(type.findFieldByName("negative").getType())
                .isEqualTo(FieldDescriptor.Type.INT64);
    }

    @Test
    void absentKeysNeverNarrowTheKind() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(
                struct("{\"v\": 3}"),
                struct("{\"other\": \"x\"}")));
        Descriptor type = shape.type();
        // v is absent from the second sample; absence is not a null observation.
        assertThat(type.findFieldByName("v").getType()).isEqualTo(FieldDescriptor.Type.INT64);
        assertThat(type.findFieldByName("other").getType())
                .isEqualTo(FieldDescriptor.Type.STRING);
    }

    @Test
    void nonNumericKindsDoNotWidenIntoEachOther() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(
                struct("{\"flag\": true}"),
                struct("{\"flag\": \"yes\"}")));
        assertThat(shape.type().findFieldByName("flag").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Value");
    }

    @Test
    void sanitizedKeyCollisionsGetNumericSuffixes() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(struct("""
                {"a-b": 1, "a_b": 2}
                """)));
        Descriptor type = shape.type();
        // Both keys sanitize to a_b; the second keeps the original via json_name.
        FieldDescriptor first = type.findFieldByName("a_b");
        FieldDescriptor second = type.findFieldByName("a_b_2");
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first.getJsonName()).isEqualTo("a-b");
        assertThat(second.getJsonName()).isEqualTo("a_b");
        assertThat(shape.protoSource())
                .contains("json_name = \"a-b\"")
                .contains("json_name = \"a_b\"");
    }

    @Test
    void digitLeadingKeysGetAnUnderscorePrefix() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(struct("""
                {"9lives": "x"}
                """)));
        FieldDescriptor field = shape.type().findFieldByName("_9lives");
        assertThat(field).isNotNull();
        assertThat(field.getJsonName()).isEqualTo("9lives");
    }

    @Test
    void messageNamesComeFromTheFullNameAndLink() throws Exception {
        var shape = new SchemaInferrer().infer("inferred.v1.Event", List.of(struct("""
                {"id": "e-1"}
                """)));
        assertThat(shape.type().getFullName()).isEqualTo("inferred.v1.Event");
        assertThat(shape.file().getName()).isEqualTo("inferred/v1/event.proto");
        // A bare name (no package) is legal too.
        var bare = new SchemaInferrer().infer("Event", List.of(struct("{\"id\": \"e-1\"}")));
        assertThat(bare.type().getFullName()).isEqualTo("Event");
        assertThat(bare.file().getName()).isEqualTo("event.proto");
    }
}

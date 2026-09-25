package ai.protomolt.proto.http.openapi;

import ai.protomolt.proto.http.openapi.gaps.Child;
import ai.protomolt.proto.http.openapi.gaps.Input;
import ai.protomolt.proto.http.openapi.gaps.Skipped;
import ai.protomolt.proto.http.rest.ProtoRestMethod;
import ai.protomolt.proto.http.rest.ProtoRestMethodRegistry;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class OpenApiRuntimeGapTest {
    private static final Map<String, Object> SCHEMAS = schemas();
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void skippedNestedValidationDoesNotRetainAConstrainedChildReference() throws Exception {
        Map<String, Object> input = schema("Input");
        assertThat(property(input, "inspected"))
                .containsEntry("type", "object").doesNotContainKey("$ref");
        assertThat(child(property(input, "inspectedMany"), "items"))
                .containsEntry("type", "object").doesNotContainKey("$ref");
        assertThat(child(property(input, "inspectedMap"), "additionalProperties"))
                .containsEntry("type", "object").doesNotContainKey("$ref");
        assertThat(property(schema("Skipped"), "child"))
                .containsEntry("type", "object").doesNotContainKey("$ref");
        assertThat(accepts(property(input, "inspected"), "{}")).isTrue();
        assertThat(accepts(property(input, "inspectedMany"), "[{}]")).isTrue();
        assertThat(accepts(property(input, "inspectedMap"), "{\"x\":{}}")).isTrue();
        assertThat(accepts(property(schema("Skipped"), "child"), "{}")).isTrue();

        Child invalid = Child.getDefaultInstance();
        assertThat(ValidationResult.validate(Input.newBuilder().setMustBeNonzero(1)
                .setOutside(2).setWideOutside(2).setFloatOutside(2)
                .setInspected(invalid).addInspectedMany(invalid)
                .putInspectedMap("x", invalid).build()).valid()).isTrue();
        assertThat(ValidationResult.validate(Skipped.newBuilder().setIncomplete(true)
                .setChild(invalid).build()).valid()).isTrue();
        assertThat(ValidationResult.validate(Skipped.newBuilder().setChild(invalid).build())
                .valid()).isFalse();
    }

    @Test
    void reversedNumericBoundsAreAlternativesWithMembershipStillConjoined() {
        Map<String, Object> input = schema("Input");
        Map<String, Object> outside = property(input, "outside");
        assertThat(outside.get("enum")).isEqualTo(List.of(2L, 11L));
        assertThat((List<?>) outside.get("anyOf")).hasSize(2);
        assertThat((List<?>) property(input, "wideOutside").get("anyOf")).hasSize(4);
        assertThat((List<?>) property(input, "unsignedOutside").get("anyOf")).hasSize(4);
        assertThat((List<?>) property(input, "floatOutside").get("anyOf")).hasSize(2);

        for (int value : List.of(2, 11)) {
            assertThat(ValidationResult.validate(Input.newBuilder().setMustBeNonzero(1)
                    .setOutside(value).setWideOutside(value).setFloatOutside(value).build())
                    .valid()).isTrue();
        }
        assertThat(ValidationResult.validate(Input.newBuilder().setMustBeNonzero(1)
                .setOutside(5).setWideOutside(5).setFloatOutside(5).build()).valid()).isFalse();
        long highUnsigned = Long.parseUnsignedLong("18446744073709551611");
        assertThat(ValidationResult.validate(Input.newBuilder().setMustBeNonzero(1)
                .setOutside(2).setUnsignedOutside(highUnsigned).build()).valid()).isTrue();
        assertThat(ValidationResult.validate(Input.newBuilder().setMustBeNonzero(1)
                .setOutside(2).setUnsignedOutside(5).build()).valid()).isFalse();
    }

    @Test
    void reversedBoundsAcceptTheSameCanonicalValuesAsRuntime() throws Exception {
        Map<String, Object> input = schema("Input");
        assertThat(accepts(property(input, "outside"), "2")).isTrue();
        assertThat(accepts(property(input, "outside"), "11")).isTrue();
        assertThat(accepts(property(input, "outside"), "5")).isFalse();
        assertThat(accepts(property(input, "wideOutside"), "\"2\"")).isTrue();
        assertThat(accepts(property(input, "wideOutside"), "\"11\"")).isTrue();
        assertThat(accepts(property(input, "wideOutside"), "\"5\"")).isFalse();
        assertThat(accepts(property(input, "floatOutside"), "2.0")).isTrue();
        assertThat(accepts(property(input, "floatOutside"), "11.0")).isTrue();
        assertThat(accepts(property(input, "floatOutside"), "5.0")).isFalse();
        assertThat(accepts(property(input, "unsignedOutside"),
                "\"18446744073709551611\"")).isTrue();
        assertThat(accepts(property(input, "unsignedOutside"), "\"5\"")).isFalse();
    }

    @Test
    void implicitZeroRequiredIsDisclosedAsRuntimeOnly() {
        Map<String, Object> input = schema("Input");
        assertThat((List<String>) input.get("required")).contains("mustBeNonzero");
        assertThat(property(input, "mustBeNonzero").get("x-protomolt-runtime-rules"))
                .isEqualTo(List.of("required-implicit-zero"));
        assertThat(ValidationResult.validate(Input.newBuilder().setMustBeNonzero(0).build())
                .valid()).isFalse();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> schemas() {
        ProtoRestMethodRegistry registry = new ProtoRestMethodRegistry();
        registry.register(ProtoRestMethod.builder("GapService", "Check", request -> request)
                .methodDescriptor(Input.getDescriptor().getFile().findServiceByName("GapService")
                        .findMethodByName("Check")).build());
        Map<String, Object> document = new ProtoOpenApiGenerator().generate(registry);
        return (Map<String, Object>) ((Map<String, Object>) document.get("components")).get("schemas");
    }

    private static Map<String, Object> schema(String name) {
        return child(SCHEMAS, "ai_protomolt_proto_http_openapi_gaps_v1_" + name);
    }

    private static Map<String, Object> property(Map<String, Object> schema, String name) {
        return child(child(schema, "properties"), name);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> child(Map<String, Object> parent, String name) {
        return (Map<String, Object>) parent.get(name);
    }

    private static boolean accepts(Map<String, Object> source, String json) throws Exception {
        ObjectNode schema = JSON.valueToTree(source);
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        adaptExclusive(schema);
        return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(JSON.writeValueAsString(schema))
                .validate(json, InputFormat.JSON).isEmpty();
    }

    private static void adaptExclusive(JsonNode node) {
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            for (String kind : List.of("Minimum", "Maximum")) {
                String bound = kind.equals("Minimum") ? "minimum" : "maximum";
                String exclusive = "exclusive" + kind;
                if (object.path(exclusive).asBoolean(false) && object.has(bound)) {
                    object.set(exclusive, object.get(bound).deepCopy());
                    object.remove(bound);
                } else object.remove(exclusive);
            }
            object.elements().forEachRemaining(OpenApiRuntimeGapTest::adaptExclusive);
        } else if (node.isArray()) node.elements().forEachRemaining(OpenApiRuntimeGapTest::adaptExclusive);
    }
}

package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.protomolt.proto.actions.TestFixtures.MAPPER;
import static ai.protomolt.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Envelope validation: every helper fails as {@code invalid-input} with the JSON pointer. */
class InputsTest {

    @Test
    void invalidInputCarriesCodeMessageAndPointer() {
        ActionException exception = Inputs.invalidInput("Missing required field 'x'", "/x");
        assertThat(exception.code()).isEqualTo("invalid-input");
        assertThat(exception.getMessage()).contains("/x");
        assertThat(exception.details().orElseThrow().get("pointer").asText()).isEqualTo("/x");
    }

    @Test
    void requireEnvelopeRejectsNullAndPassesObjectsThrough() throws Exception {
        assertThatThrownBy(() -> Inputs.requireEnvelope(null))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
        ObjectNode envelope = obj("{}");
        assertThat(Inputs.requireEnvelope(envelope)).isSameAs(envelope);
    }

    @Test
    void requireObjectCoversMissingNullNonObjectAndValid() throws Exception {
        ObjectNode input = obj("{\"present\": {}, \"explicitNull\": null, \"string\": \"x\"}");
        assertThatThrownBy(() -> Inputs.requireObject(input, "absent"))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/absent");
                });
        assertThatThrownBy(() -> Inputs.requireObject(input, "explicitNull"))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
        assertThatThrownBy(() -> Inputs.requireObject(input, "string"))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.getMessage()).contains("must be a JSON object"));
        assertThat(Inputs.requireObject(input, "present")).isEqualTo(obj("{}"));
    }

    @Test
    void optionalObjectReturnsNullWhenAbsent() throws Exception {
        ObjectNode input = obj("{\"present\": {\"a\": 1}, \"explicitNull\": null}");
        assertThat(Inputs.optionalObject(input, "absent")).isNull();
        assertThat(Inputs.optionalObject(input, "explicitNull")).isNull();
        assertThat(Inputs.optionalObject(input, "present")).isEqualTo(obj("{\"a\": 1}"));
        assertThatThrownBy(() -> Inputs.optionalObject(obj("{\"s\": \"x\"}"), "s"))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void requireStringRejectsMissingAndNonTextual() throws Exception {
        ObjectNode input = obj("{\"name\": \"x\", \"count\": 3}");
        assertThat(Inputs.requireString(input, "name")).isEqualTo("x");
        assertThatThrownBy(() -> Inputs.requireString(input, "absent"))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/absent");
                });
        assertThatThrownBy(() -> Inputs.requireString(input, "count"))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.getMessage()).contains("must be a string"));
    }

    @Test
    void optionalStringRejectsOnlyPresentNonStrings() throws Exception {
        ObjectNode input = obj("{\"name\": \"x\", \"count\": 3}");
        assertThat(Inputs.optionalString(input, "absent")).isNull();
        assertThat(Inputs.optionalString(input, "name")).isEqualTo("x");
        assertThatThrownBy(() -> Inputs.optionalString(input, "count"))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void optionalBooleanDefaultsAndRejectsNonBooleans() throws Exception {
        ObjectNode input = obj("{\"flag\": true, \"count\": 3}");
        assertThat(Inputs.optionalBoolean(input, "absent", true)).isTrue();
        assertThat(Inputs.optionalBoolean(input, "absent", false)).isFalse();
        assertThat(Inputs.optionalBoolean(input, "flag", false)).isTrue();
        assertThatThrownBy(() -> Inputs.optionalBoolean(input, "count", false))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.getMessage()).contains("must be a boolean"));
    }

    @Test
    void optionalArrayReturnsNullWhenAbsentAndRejectsNonArrays() throws Exception {
        ObjectNode input = obj("{\"items\": [1, 2], \"count\": 3}");
        assertThat(Inputs.optionalArray(input, "absent")).isNull();
        assertThat(Inputs.optionalArray(input, "items")).hasSize(2);
        assertThatThrownBy(() -> Inputs.optionalArray(input, "count"))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.getMessage()).contains("must be an array"));
    }

    @Test
    void stringElementsCollectsStringsAndPointsAtTheBadElement() throws Exception {
        ArrayNode strings = MAPPER.createArrayNode().add("a").add("b");
        assertThat(Inputs.stringElements(strings, "/rules")).isEqualTo(List.of("a", "b"));

        ArrayNode mixed = MAPPER.createArrayNode().add("a").add(42);
        assertThatThrownBy(() -> Inputs.stringElements(mixed, "/rules"))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/rules/1");
                });
    }
}

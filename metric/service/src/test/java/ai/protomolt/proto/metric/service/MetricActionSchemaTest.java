package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The input schemas the metric verbs publish.
 *
 * <p>These schemas are derived from the request messages rather than written by hand, so a
 * caller reading the tool manifest sees the same contract the gate enforces. The assertions
 * below check that the derivation actually carries the declared rules through, because a
 * schema that merely says "object" would still serve, still parse, and still pass every
 * behavioural test while telling every caller nothing.
 */
class MetricActionSchemaTest {

    private static final Map<String, Integer> VERB_INDEX =
            Map.of("describe-mapping", 0, "query-metrics", 1, "rebuild-rollup", 2);

    private static ObjectNode schemaOf(String verb) {
        List<ProtoAction> verbs = MetricActions.over(Map.of());
        ProtoAction action = verbs.get(VERB_INDEX.get(verb));
        assertThat(action.name()).isEqualTo(verb);
        return action.inputSchema();
    }

    /**
     * The request message's own definition, resolved through the document's root reference.
     * A verb manifest describes its root message in place, because a tool-calling client
     * reads the root's properties directly and cannot follow a reference to find them.
     * Nested types stay in the definitions block, where a message reaching the same type
     * twice is still described once.
     */
    private static JsonNode root(ObjectNode schema) {
        assertThat(schema.path("$ref").isMissingNode())
                .as("a manifest root is described in place, not referenced").isTrue();
        assertThat(schema.path("type").asText()).isEqualTo("object");
        return schema;
    }

    private static JsonNode property(ObjectNode schema, String field) {
        JsonNode property = root(schema).path("properties").path(field);
        assertThat(property.isMissingNode()).as("property %s", field).isFalse();
        return property;
    }

    @Test
    void everyVerbDescribesItsFieldsRatherThanAcceptingAnyObject() {
        for (String verb : VERB_INDEX.keySet()) {
            JsonNode definition = root(schemaOf(verb));
            assertThat(definition.path("properties").isObject())
                    .as("%s properties", verb).isTrue();
            assertThat(definition.path("properties")).as("%s field count", verb).isNotEmpty();
        }
    }

    /**
     * The bound on result rows exists so one query cannot ask for the whole table. It is
     * declared on the request message, and a caller has to be able to see it before sending.
     */
    @Test
    void theRowLimitBoundReachesCallersThroughTheSchema() {
        JsonNode limit = property(schemaOf("query-metrics"), "limit");
        assertThat(limit.path("exclusiveMinimum").asInt()).isEqualTo(0);
        assertThat(limit.path("maximum").asInt()).isEqualTo(1000);
    }

    /** A query with no measure computes nothing, so at least one is declared as required. */
    @Test
    void theMeasureRequirementReachesCallersThroughTheSchema() {
        JsonNode measures = property(schemaOf("query-metrics"), "measures");
        assertThat(measures.path("type").asText()).isEqualTo("array");
        assertThat(measures.path("minItems").asInt()).isEqualTo(1);
        assertThat(measures.path("maxItems").asInt()).isEqualTo(100);
    }

    /** Member names are bounded wherever they appear, including inside the measure list. */
    @Test
    void measureNamesCarryTheSameBoundAsEveryOtherMemberReference() {
        JsonNode items = property(schemaOf("query-metrics"), "measures").path("items");
        assertThat(items.path("minLength").asInt()).isEqualTo(1);
        assertThat(items.path("maxLength").asInt()).isEqualTo(200);
    }

    /** A rollup replaces a named table, so the name is required rather than defaulted. */
    @Test
    void theRollupTableIsDeclaredRequired() {
        ObjectNode schema = schemaOf("rebuild-rollup");
        property(schema, "table");
        JsonNode required = root(schema).path("required");
        assertThat(required).isNotEmpty();
        assertThat(required.toString()).contains("table");
    }

    /** The engine selector is an enum, so its legal values are visible rather than guessed. */
    @Test
    void theBackendSelectorPublishesItsLegalValues() {
        JsonNode backend = property(schemaOf("query-metrics"), "backend");
        assertThat(backend.toString())
                .contains("METRIC_BACKEND_LUCENE")
                .contains("METRIC_BACKEND_ICEBERG");
    }
}

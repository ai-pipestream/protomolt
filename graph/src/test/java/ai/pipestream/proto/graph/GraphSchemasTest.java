package ai.pipestream.proto.graph;

import ai.pipestream.proto.index.spi.IndexingPlanFactory;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Indexing hints to a Graph external connection schema: the declare-once contract drives a
 * fourth engine. Types map (TEXT searchable string, KEYWORD exact-match, DATE dateTime,
 * repeated to collections), refinable never coexists with searchable, unrepresentable
 * fields are skipped with reasons, and property names are Graph-legal.
 */
class GraphSchemasTest {

    private static final String PROTO = """
            syntax = "proto3";
            package gs.test;
            message Doc {
              string body = 1;
              string category = 2;
              int64 count = 3;
              double score = 4;
              bool active = 5;
              repeated string tags = 6;
              repeated bool flags = 7;
              Meta meta = 8;
            }
            message Meta { string author_name = 1; }
            """;

    private static Descriptor compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("gs/test/gs.proto", PROTO, "test").build());
        return compiled.descriptorFor("gs/test/gs.proto").orElseThrow()
                .findMessageTypeByName("Doc");
    }

    @Test
    void rendersHintsAsAConnectionSchema() throws Exception {
        Descriptor doc = compile();
        var plan = IndexingPlanFactory.inferringOnly().create(doc);
        GraphSchemas.Rendered rendered = GraphSchemas.connectionSchema(doc, plan);

        assertThat(rendered.schema().path("baseType").asText())
                .isEqualTo("microsoft.graph.externalItem");
        Map<String, JsonNode> byName = new java.util.LinkedHashMap<>();
        rendered.schema().path("properties").forEach(p ->
                byName.put(p.path("name").asText(), p));

        // Repeated strings become a collection.
        assertThat(byName.get("tags").path("type").asText()).isEqualTo("stringCollection");
        // Numeric and boolean types map; nothing repeated is refinable.
        assertThat(byName.get("count").path("type").asText()).isEqualTo("int64");
        assertThat(byName.get("score").path("type").asText()).isEqualTo("double");
        assertThat(byName.get("active").path("type").asText()).isEqualTo("boolean");

        // Graph forbids searchable+refinable on one property.
        rendered.schema().path("properties").forEach(p -> assertThat(
                p.path("isSearchable").asBoolean() && p.path("isRefinable").asBoolean())
                .as("%s must not be searchable and refinable", p.path("name").asText())
                .isFalse());

        // Repeated bools have no Graph collection type, and nested messages stay
        // sub-documents in the plan: both skip with a reason, never silently drop.
        assertThat(rendered.skipped()).anySatisfy(reason ->
                assertThat(reason).contains("flags").contains("no Graph property type"));
        assertThat(rendered.skipped()).anySatisfy(reason ->
                assertThat(reason).contains("meta").contains("flat"));
    }

    @Test
    void propertyNamesAreLegalUniqueAndBounded() {
        Set<String> used = new LinkedHashSet<>();
        assertThat(GraphSchemas.propertyName("location.lat", used)).isEqualTo("locationLat");
        // A collision numbers itself instead of silently overwriting.
        assertThat(GraphSchemas.propertyName("location_lat", used)).isEqualTo("locationLat2");
        // Illegal leading characters and over-long paths are repaired.
        assertThat(GraphSchemas.propertyName("2fast", used)).isEqualTo("p2fast");
        String long1 = GraphSchemas.propertyName(
                "a.very.long.nested.path.that.keeps.going.and.going.forever", used);
        assertThat(long1.length()).isLessThanOrEqualTo(32);
        String long2 = GraphSchemas.propertyName(
                "a.very.long.nested.path.that.keeps.going.and.going.forever", used);
        assertThat(long2.length()).isLessThanOrEqualTo(32);
        assertThat(long2).isNotEqualTo(long1);
    }
}

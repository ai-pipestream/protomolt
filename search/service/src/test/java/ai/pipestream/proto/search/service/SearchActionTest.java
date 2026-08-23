package ai.pipestream.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.StoredValue;
import ai.pipestream.proto.search.v1.SubjectInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The query surface an agent sees. Two things are worth holding still here: the action has
 * to refuse exactly what the gRPC door refuses, since it does not sit behind that door's
 * validating interceptor, and a refusal has to tell the caller enough to fix the call
 * without a second round trip.
 */
class SearchActionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A served index that records the request and answers from a list. */
    private static final class FakeIndex implements SubjectSearch {
        SearchRequest lastRequest;
        String lastSubject;
        List<SearchHit> hits = new ArrayList<>();
        RuntimeException failure;

        @Override
        public List<SearchHit> search(String subjectName, SearchRequest request) {
            lastSubject = subjectName;
            lastRequest = request;
            if (failure != null) {
                throw failure;
            }
            return hits;
        }

        @Override
        public List<SubjectInfo> describeSubjects() {
            return List.of(SubjectInfo.newBuilder()
                    .setSubject("docs")
                    .setHasVectorLane(true)
                    .addTextFields("title")
                    .addTextFields("body")
                    .build());
        }
    }

    private final FakeIndex index = new FakeIndex();
    private final SearchAction action = new SearchAction(index);
    private final ActionContext context = ActionContext.create();

    private static ObjectNode input(String subject, String query, Integer k, String lane) {
        ObjectNode input = MAPPER.createObjectNode();
        if (subject != null) {
            input.put("mappingSubject", subject);
        }
        if (query != null) {
            input.put("query", query);
        }
        if (k != null) {
            input.put("k", k);
        }
        if (lane != null) {
            input.put("lane", lane);
        }
        return input;
    }

    private static ObjectNode valid() {
        return input("docs", "hello", 5, "lexical");
    }

    // --- the contract the catalog sees ------------------------------------------

    @Test
    void theActionClaimsTheSearchQueryScope() {
        assertThat(action.name()).isEqualTo("search");
        assertThat(action.requiredScope()).isEqualTo(Scopes.SEARCH_QUERY);
    }

    @Test
    void theInputSchemaNamesTheThreeLanesAndTheHitCap() {
        ObjectNode schema = action.inputSchema();
        ObjectNode properties = (ObjectNode) schema.get("properties");

        assertThat(properties.get("lane").get("enum").toString())
                .isEqualTo("[\"lexical\",\"vector\",\"hybrid\"]");
        assertThat(properties.get("k").get("maximum").asInt()).isEqualTo(10_000);
        assertThat(schema.get("required").toString())
                .isEqualTo("[\"mappingSubject\",\"query\",\"k\",\"lane\"]");
        assertThat(schema.get("additionalProperties").asBoolean()).isFalse();
    }

    // --- the request that reaches the index -------------------------------------

    @Test
    void aQueryReachesTheIndexAsTheGrpcDoorWouldHaveBuiltIt() throws Exception {
        ObjectNode in = valid();
        in.putArray("fields").add("title");

        action.execute(in, context);

        assertThat(index.lastSubject).isEqualTo("docs");
        assertThat(index.lastRequest.getMappingSubject()).isEqualTo("docs");
        assertThat(index.lastRequest.getQuery()).isEqualTo("hello");
        assertThat(index.lastRequest.getK()).isEqualTo(5);
        assertThat(index.lastRequest.getLane()).isEqualTo(SearchLane.SEARCH_LANE_LEXICAL);
        assertThat(index.lastRequest.getFieldsList()).containsExactly("title");
    }

    @Test
    void everyLaneNameMapsToItsEnum() throws Exception {
        action.execute(input("docs", "q", 1, "vector"), context);
        assertThat(index.lastRequest.getLane()).isEqualTo(SearchLane.SEARCH_LANE_VECTOR);

        action.execute(input("docs", "q", 1, "hybrid"), context);
        assertThat(index.lastRequest.getLane()).isEqualTo(SearchLane.SEARCH_LANE_HYBRID);

        action.execute(input("docs", "q", 1, "LEXICAL"), context);
        assertThat(index.lastRequest.getLane()).isEqualTo(SearchLane.SEARCH_LANE_LEXICAL);
    }

    @Test
    void anUnknownLaneIsRefusedByNameWithTheLegalSet() {
        assertThatThrownBy(() -> action.execute(input("docs", "q", 1, "fuzzy"), context))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("fuzzy")
                .hasMessageContaining("lexical, vector, hybrid");
    }

    // --- the same rules the gRPC door enforces ----------------------------------

    /**
     * The gRPC door runs the search proto's declared rules through an interceptor. This
     * action does not sit behind that interceptor, so if it did not re-run them it would be
     * the looser of the two doors into the same index.
     */
    @Test
    void theDeclaredRulesAreEnforcedHereToo() {
        assertThatThrownBy(() -> action.execute(input("docs", "q", 0, "lexical"), context))
                .as("k must be positive")
                .isInstanceOf(ActionException.class);
        assertThatThrownBy(() -> action.execute(input("docs", "q", 10_001, "lexical"), context))
                .as("k is capped, and over-cap is refused rather than clamped")
                .isInstanceOf(ActionException.class);
        assertThatThrownBy(() -> action.execute(input("docs", "", 5, "lexical"), context))
                .as("the query text is required")
                .isInstanceOf(ActionException.class);
        assertThatThrownBy(() -> action.execute(input("", "q", 5, "lexical"), context))
                .as("the subject is required")
                .isInstanceOf(ActionException.class);
    }

    @Test
    void anOverCapRequestNeverReachesTheIndex() {
        assertThatThrownBy(() -> action.execute(input("docs", "q", 10_001, "lexical"), context))
                .isInstanceOf(ActionException.class);
        assertThat(index.lastRequest).as("refused before the index was touched").isNull();
    }

    /**
     * An absent lane is left unspecified rather than defaulted. Picking one here would
     * answer a different question than the caller asked and never say so.
     */
    @Test
    void anAbsentLaneIsRefusedRatherThanChosen() {
        assertThatThrownBy(() -> action.execute(input("docs", "q", 5, null), context))
                .isInstanceOf(ActionException.class);
        assertThat(index.lastRequest).isNull();
    }

    // --- what comes back --------------------------------------------------------

    @Test
    void hitsComeBackWithTypedStoredValues() throws Exception {
        index.hits = List.of(SearchHit.newBuilder()
                .setDocId("doc-1")
                .setChunkId("doc-1#abc#0")
                .setScore(1.5f)
                .putStored("title", StoredValue.newBuilder().setStringValue("Hello").build())
                .putStored("count", StoredValue.newBuilder().setInt64Value(42).build())
                .putStored("ratio", StoredValue.newBuilder().setDoubleValue(0.25).build())
                .putStored("live", StoredValue.newBuilder().setBoolValue(true).build())
                .putStored("at", StoredValue.newBuilder()
                        .setTimestampValue(Timestamp.newBuilder().setSeconds(1_700_000_000))
                        .build())
                .build());

        ObjectNode out = action.execute(valid(), context);

        assertThat(out.get("hits")).hasSize(1);
        ObjectNode hit = (ObjectNode) out.get("hits").get(0);
        assertThat(hit.get("docId").asText()).isEqualTo("doc-1");
        assertThat(hit.get("chunkId").asText()).isEqualTo("doc-1#abc#0");
        assertThat(hit.get("score").asDouble()).isEqualTo(1.5);

        ObjectNode stored = (ObjectNode) hit.get("stored");
        // Typed arms, not rendered strings: the caller never re-parses to recover a value.
        assertThat(stored.get("title").get("stringValue").asText()).isEqualTo("Hello");
        assertThat(stored.get("count").get("int64Value").asLong()).isEqualTo(42);
        assertThat(stored.get("ratio").get("doubleValue").asDouble()).isEqualTo(0.25);
        assertThat(stored.get("live").get("boolValue").asBoolean()).isTrue();
        assertThat(stored.get("at").has("timestampValue")).isTrue();
    }

    @Test
    void noHitsIsAnEmptyListNotAnError() throws Exception {
        assertThat(action.execute(valid(), context).get("hits")).isEmpty();
    }

    // --- refusals carry the way forward -----------------------------------------

    /**
     * An unknown subject is the most likely mistake an agent makes, and the answer it needs
     * is which subjects exist. Sending that with the refusal saves a round trip it would
     * otherwise have to guess its way to.
     */
    @Test
    void anUnknownSubjectComesBackWithTheSubjectsThatDoExist() {
        index.failure = new IllegalArgumentException("no subject 'nope'");

        assertThatThrownBy(() -> action.execute(input("nope", "q", 5, "lexical"), context))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.getMessage()).contains("nope");
                    ObjectNode details = e.details().orElseThrow();
                    ObjectNode served = (ObjectNode) details.get("servedSubjects").get(0);
                    assertThat(served.get("subject").asText()).isEqualTo("docs");
                    assertThat(served.get("hasVectorLane").asBoolean()).isTrue();
                    assertThat(served.get("textFields").toString())
                            .isEqualTo("[\"title\",\"body\"]");
                });
    }

    /** A subject with no chunk lane cannot answer a vector query, and says which it is. */
    @Test
    void aLaneTheSubjectCannotRunIsItsOwnRefusal() {
        index.failure = new IllegalStateException("subject 'docs' has no chunk lane");

        assertThatThrownBy(() -> action.execute(input("docs", "q", 5, "vector"), context))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("lane-unavailable");
                    assertThat(e.details().orElseThrow().has("servedSubjects")).isTrue();
                });
    }

    @Test
    void theIndexIsRequired() {
        assertThatThrownBy(() -> new SearchAction(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index");
    }
}

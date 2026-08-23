package ai.pipestream.proto.search.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SubjectInfo;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Querying a mapping subject, as an agent-operable action.
 *
 * <p>The indexing half of this service has been agent-operable for a while: an agent can
 * index a document and replay a subject. It could not ask a question of what it had just
 * written, because the query surface was gRPC only. A retrieval product whose retrieval is
 * the one thing an agent cannot reach has the shape backwards, so this closes it.
 *
 * <p>It runs the same code path the gRPC service does, down to the same declared rules.
 * That matters more than it looks: the gRPC door enforces the {@code validate.v1} rules on
 * {@link SearchRequest} through an interceptor, and an action does not sit behind that
 * interceptor. Re-checking them here is what keeps the two doors from disagreeing about
 * what a legal query is, rather than this one quietly accepting a {@code k} the other
 * refuses.
 */
public final class SearchAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "search";

    /** Lane names a caller writes, and the enum each one means. */
    private static final Map<String, SearchLane> LANES = Map.of(
            "lexical", SearchLane.SEARCH_LANE_LEXICAL,
            "vector", SearchLane.SEARCH_LANE_VECTOR,
            "hybrid", SearchLane.SEARCH_LANE_HYBRID);

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private final SubjectSearch index;
    private final ProtoValidator validator;

    /**
     * Creates the action.
     *
     * @param index the served index this action queries
     */
    public SearchAction(SubjectSearch index) {
        this(index, ProtoValidator.create());
    }

    SearchAction(SubjectSearch index, ProtoValidator validator) {
        if (index == null) {
            throw new IllegalArgumentException("index must not be null");
        }
        if (validator == null) {
            throw new IllegalArgumentException("validator must not be null");
        }
        this.index = index;
        this.validator = validator;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String requiredScope() {
        return Scopes.SEARCH_QUERY;
    }

    @Override
    public String description() {
        return "Searches one mapping subject and returns the best hits. Lanes: 'lexical' is"
                + " analyzed term matching over the subject's text fields, 'vector' is KNN over"
                + " its chunk vectors and needs a chunking policy, 'hybrid' runs both and fuses"
                + " them by reciprocal rank. Hits carry the document id, the chunk id for"
                + " vector-lane chunk hits, the lane score, and the mapping's stored fields as"
                + " typed values (each is an object with one of stringValue, int64Value,"
                + " doubleValue, boolValue, timestampValue or bytesValue, so a caller never has"
                + " to re-parse a rendered string). Use list-subjects, or an unknown subject's"
                + " refusal, to discover what can be searched.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("mappingSubject")
                .put("type", "string")
                .put("description", "The mapping subject to search.");
        properties.putObject("query")
                .put("type", "string")
                .put("description", "The query text.");
        ObjectNode k = properties.putObject("k");
        k.put("type", "integer");
        k.put("minimum", 1);
        k.put("maximum", 10000);
        k.put("description", "Maximum hits to return. An over-cap value is refused by name"
                + " rather than clamped, so one query cannot ask for the whole index.");
        ObjectNode lane = properties.putObject("lane");
        lane.put("type", "string");
        ArrayNode laneValues = lane.putArray("enum");
        laneValues.add("lexical");
        laneValues.add("vector");
        laneValues.add("hybrid");
        lane.put("description", "Which lane to run.");
        ObjectNode fields = properties.putObject("fields");
        fields.put("type", "array");
        fields.putObject("items").put("type", "string");
        fields.put("description", "Text fields to match in the lexical lane, by index field"
                + " name. Omit for every text field in the subject's mapping; a field outside"
                + " the mapping is refused by name.");
        ArrayNode required = schema.putArray("required");
        required.add("mappingSubject");
        required.add("query");
        required.add("k");
        required.add("lane");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SearchRequest request = requestFrom(input, context);
        List<ai.pipestream.proto.search.v1.SearchHit> hits;
        try {
            hits = index.search(request.getMappingSubject(), request);
        } catch (IllegalArgumentException e) {
            // An unknown subject or an unmapped field: the store names what went wrong and
            // lists what it serves, which is the answer the caller needs to fix the call.
            throw new ActionException("invalid-query", e.getMessage(), subjects(context));
        } catch (IllegalStateException e) {
            // A lane the subject is not wired for, most often vector without a chunk lane.
            throw new ActionException("lane-unavailable", e.getMessage(), subjects(context));
        }
        String json = context.transcoder().toJson(
                SearchResponse.newBuilder().addAllHits(hits).build());
        try {
            return (ObjectNode) context.objectMapper().readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new ActionException("internal", "the search response did not render as JSON: "
                    + e.getMessage(), null);
        }
    }

    /** Reads the input into a request and holds it to the rules the gRPC door holds it to. */
    private SearchRequest requestFrom(ObjectNode input, ActionContext context)
            throws ActionException {
        SearchRequest.Builder request = SearchRequest.newBuilder();
        request.setMappingSubject(text(input, "mappingSubject"));
        request.setQuery(text(input, "query"));
        JsonNode k = input.get("k");
        if (k != null && k.isNumber()) {
            request.setK(k.asInt());
        }
        request.setLane(lane(input));
        JsonNode fields = input.get("fields");
        if (fields != null && fields.isArray()) {
            for (JsonNode field : fields) {
                request.addFields(field.asText());
            }
        }
        SearchRequest built = request.build();
        ValidationResult result = validator.validate(built);
        if (!result.valid()) {
            ObjectNode details = context.objectMapper().createObjectNode();
            ArrayNode violations = details.putArray("violations");
            for (ValidationResult.Violation violation : result.violations()) {
                ObjectNode node = violations.addObject();
                node.put("field", violation.path());
                node.put("ruleId", violation.ruleId());
                node.put("message", violation.message());
            }
            throw new ActionException("invalid-query",
                    "The query does not satisfy the search contract: " + describe(result),
                    details);
        }
        return built;
    }

    private static String text(ObjectNode input, String field) {
        JsonNode node = input.get(field);
        return node == null || node.isNull() ? "" : node.asText();
    }

    /**
     * The lane, refused by name when it is not one of the three. Left unspecified when
     * absent, so the declared rules produce the missing-field violation rather than this
     * inventing a default lane, which would silently answer a different question.
     */
    private SearchLane lane(ObjectNode input) throws ActionException {
        JsonNode node = input.get("lane");
        if (node == null || node.isNull() || node.asText().isBlank()) {
            return SearchLane.SEARCH_LANE_UNSPECIFIED;
        }
        String name = node.asText().trim().toLowerCase(Locale.ROOT);
        SearchLane lane = LANES.get(name);
        if (lane == null) {
            throw new ActionException("invalid-query", "lane '" + node.asText()
                    + "' is not one of: lexical, vector, hybrid", null);
        }
        return lane;
    }

    /**
     * What this index serves, so a refused query can say what could be asked instead. The
     * text fields and the vector lane are the two things a caller needs to pick a legal
     * next query, so they travel with the refusal rather than needing a second round trip.
     */
    private ObjectNode subjects(ActionContext context) {
        ObjectNode details = context.objectMapper().createObjectNode();
        ArrayNode served = details.putArray("servedSubjects");
        for (SubjectInfo info : index.describeSubjects()) {
            ObjectNode node = served.addObject();
            node.put("subject", info.getSubject());
            node.put("hasVectorLane", info.getHasVectorLane());
            ArrayNode text = node.putArray("textFields");
            info.getTextFieldsList().forEach(text::add);
        }
        return details;
    }

    private static String describe(ValidationResult result) {
        StringBuilder out = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            if (out.length() > 0) {
                out.append("; ");
            }
            out.append(violation.path()).append(' ').append(violation.message());
        }
        return out.toString();
    }
}

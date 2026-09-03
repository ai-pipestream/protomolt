package ai.protomolt.proto.search.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SubjectInfo;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;

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

    private final SubjectSearch index;

    /**
     * Creates the action.
     *
     * @param index the served index this action queries
     */
    public SearchAction(SubjectSearch index) {
        if (index == null) {
            throw new IllegalArgumentException("index must not be null");
        }
        this.index = index;
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
        return "Searches one mapping subject and returns the best hits."
                + " SEARCH_LANE_LEXICAL is analyzed term matching over the subject's text"
                + " fields, SEARCH_LANE_VECTOR is nearest-neighbour search over its chunk"
                + " vectors and requires a chunking policy, and SEARCH_LANE_HYBRID runs both"
                + " and fuses them by reciprocal rank. Hits carry the document id, the chunk"
                + " id for vector-lane chunk hits, the lane score, and the mapping's stored"
                + " fields as typed values, each an object carrying one of stringValue,"
                + " int64Value, doubleValue, boolValue, timestampValue or bytesValue, so a"
                + " caller never re-parses a rendered string. An unknown subject is refused"
                + " with the list of subjects this index serves.";
    }

    @Override
    public Descriptor requestType() {
        return SearchRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return SearchResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SearchRequest request = CatalogContract.as(
                input, SearchRequest.getDefaultInstance(), name());
        List<ai.protomolt.proto.search.v1.SearchHit> hits;
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
        return SearchResponse.newBuilder().addAllHits(hits).build();
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
}

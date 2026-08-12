package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/** Applies an external review decision to the open completion candidate. */
final class DelegationReviewAction extends DelegationAction {

    DelegationReviewAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-review";
    }

    @Override
    public String description() {
        return "Applies the review verdict for a task's open completion candidate: "
                + "'accept' with a verdict line, or 'revise' with feedback and the failed "
                + "checks. The candidate must be under review (submitted, not yet decided).";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "taskId", "The task uuid with an open candidate.");
        properties.putObject("decision")
                .put("type", "string")
                .put("description", "'accept' or 'revise'.")
                .putArray("enum").add("accept").add("revise");
        putString(properties, "verdict",
                "The one-line acceptance verdict; required when decision is 'accept'.");
        putString(properties, "feedback",
                "What must change; required when decision is 'revise'.");
        properties.putObject("failedChecks")
                .put("type", "array")
                .put("description", "The required checks whose evidence did not convince "
                        + "the reviewer; only with 'revise'.")
                .putObject("items").put("type", "string");
        require(schema, "taskId", "decision");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String taskId = DelegationActionJson.uuid(input, "taskId");
        String decision = DelegationActionJson.text(input, "decision");
        CandidateReviewer.ReviewDecision review;
        switch (decision) {
            case "accept" -> {
                String verdict = DelegationActionJson.text(input, "verdict");
                review = CandidateReviewer.ReviewDecision.accept(verdict);
            }
            case "revise" -> {
                String feedback = DelegationActionJson.text(input, "feedback");
                List<String> failedChecks = new ArrayList<>();
                JsonNode checks = input.get("failedChecks");
                if (checks != null && !checks.isNull()) {
                    if (!checks.isArray()) {
                        throw DelegationActionJson.invalid("'failedChecks' must be an array",
                                "/failedChecks");
                    }
                    for (int i = 0; i < checks.size(); i++) {
                        if (!checks.get(i).isTextual()) {
                            throw DelegationActionJson.invalid(
                                    "'failedChecks' entries must be strings",
                                    "/failedChecks/" + i);
                        }
                        failedChecks.add(checks.get(i).asText());
                    }
                }
                review = CandidateReviewer.ReviewDecision.revise(feedback, failedChecks);
            }
            default -> throw DelegationActionJson.invalid(
                    "'decision' must be 'accept' or 'revise'", "/decision");
        }
        try {
            bridge.review(taskId, review);
        } catch (RuntimeException e) {
            throw failure(e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("taskId", taskId);
        output.put("decision", decision);
        return output;
    }
}

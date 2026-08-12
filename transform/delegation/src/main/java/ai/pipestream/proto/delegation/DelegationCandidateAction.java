package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Submits one revision of completion evidence for coordinator review. */
final class DelegationCandidateAction extends DelegationAction {

    DelegationCandidateAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-candidate";
    }

    @Override
    public String description() {
        return "Submits a completion candidate for review: the CompletionCandidate as proto3 "
                + "JSON with the attempt, the expected revision, a summary, passing evidence "
                + "for every required check of the offer's spec, and at least one commit or "
                + "artifact reference. The coordinator answers with acceptance or a revision "
                + "request on the event feed; a worker can never mark its own task done.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "workerId", "The registered worker submitting the candidate.");
        putString(properties, "taskId", "The task uuid.");
        properties.putObject("candidate")
                .put("type", "object")
                .put("description", "The CompletionCandidate as canonical proto3 JSON: "
                        + "attempt, revision, summary, evidence (one passing entry per "
                        + "required check), commits and/or artifacts.");
        require(schema, "workerId", "taskId", "candidate");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String workerId = DelegationActionJson.identity(input, "workerId");
        String taskId = DelegationActionJson.uuid(input, "taskId");
        CompletionCandidate candidate = (CompletionCandidate) DelegationActionJson.parse(
                DelegationActionJson.object(input, "candidate"),
                CompletionCandidate.newBuilder(), "/candidate");
        try {
            bridge.submitCandidate(workerId, taskId, candidate);
        } catch (RuntimeException e) {
            throw failure(workerId, e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("taskId", taskId);
        output.put("attempt", candidate.getAttempt());
        output.put("revision", candidate.getRevision());
        return output;
    }
}

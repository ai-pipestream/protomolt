package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Records one resumable checkpoint of worker state on the leased attempt. */
final class DelegationCheckpointAction extends DelegationAction {

    DelegationCheckpointAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-checkpoint";
    }

    @Override
    public String description() {
        return "Records one resumable checkpoint on the worker's leased attempt: a resume "
                + "token, an optional note, and an optional state artifact reference. "
                + "Checkpoint sequences are assigned by the bridge and never regress; a "
                + "later offer's resumeFrom echoes the token back.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "workerId", "The registered worker recording the checkpoint.");
        putString(properties, "taskId", "The task uuid.");
        putInteger(properties, "attempt", "The leased attempt.", 1, 1_024);
        putString(properties, "resumeToken",
                "The opaque token a later offer's resumeFrom echoes back.");
        putString(properties, "note", "Optional bounded note about what the checkpoint covers.");
        properties.putObject("state")
                .put("type", "object")
                .put("description", "Optional ArtifactReference (proto3 JSON) holding the "
                        + "checkpointed state when it is too large for the token.");
        require(schema, "workerId", "taskId", "attempt", "resumeToken");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String workerId = DelegationActionJson.identity(input, "workerId");
        String taskId = DelegationActionJson.uuid(input, "taskId");
        int attempt = DelegationActionJson.boundedInt(input, "attempt", -1, 1, 1_024);
        if (attempt < 0) {
            throw DelegationActionJson.invalid("'attempt' is required", "/attempt");
        }
        String resumeToken = DelegationActionJson.text(input, "resumeToken");
        String note = DelegationActionJson.optionalText(input, "note");
        ArtifactReference state = null;
        if (input.has("state") && !input.get("state").isNull()) {
            state = (ArtifactReference) DelegationActionJson.parse(
                    DelegationActionJson.object(input, "state"),
                    ArtifactReference.newBuilder(), "/state");
        }
        int checkpointSeq;
        try {
            checkpointSeq = bridge.checkpoint(workerId, taskId, attempt, resumeToken, note,
                    state);
        } catch (RuntimeException e) {
            throw failure(workerId, e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("checkpointSeq", checkpointSeq);
        output.put("resumeToken", resumeToken);
        return output;
    }
}

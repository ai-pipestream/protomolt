package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reports one monotonic progress note from the worker on its leased attempt. */
final class DelegationProgressAction extends DelegationAction {

    DelegationProgressAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-progress";
    }

    @Override
    public String description() {
        return "Reports one bounded progress note on the worker's leased attempt. Progress "
                + "sequences are assigned by the bridge and strictly increase inside the "
                + "attempt; the returned progressSeq is the assigned sequence.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "workerId", "The registered worker reporting progress.");
        putString(properties, "taskId", "The task uuid.");
        putInteger(properties, "attempt", "The leased attempt.", 1, 1_024);
        putString(properties, "message", "What advanced, in bounded prose.");
        require(schema, "workerId", "taskId", "attempt", "message");
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
        String message = DelegationActionJson.text(input, "message");
        int progressSeq;
        try {
            progressSeq = bridge.progress(workerId, taskId, attempt, message);
        } catch (RuntimeException e) {
            throw failure(workerId, e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("progressSeq", progressSeq);
        return output;
    }
}

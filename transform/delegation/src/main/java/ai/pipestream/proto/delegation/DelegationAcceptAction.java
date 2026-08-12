package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** The worker takes the open offer for a task's current attempt. */
final class DelegationAcceptAction extends DelegationAction {

    DelegationAcceptAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-accept";
    }

    @Override
    public String description() {
        return "Accepts the open offer for a task: the worker takes the attempt's lease. "
                + "Watch the event feed (delegation-watch) for the offer first; the attempt "
                + "must match the open offer's attempt.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "workerId", "The registered worker taking the lease.");
        putString(properties, "taskId", "The offered task uuid.");
        putInteger(properties, "attempt", "The open offer's attempt number.", 1, 1_024);
        require(schema, "workerId", "taskId", "attempt");
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
        try {
            bridge.accept(workerId, taskId, attempt);
        } catch (RuntimeException e) {
            throw failure(workerId, e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("taskId", taskId);
        output.put("attempt", attempt);
        return output;
    }
}

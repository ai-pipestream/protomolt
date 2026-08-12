package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Cancels a task's open offer, lease, or candidate review. */
final class DelegationCancelAction extends DelegationAction {

    DelegationCancelAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-cancel";
    }

    @Override
    public String description() {
        return "Cancels a task's open attempt with a bounded reason. Cancellation is "
                + "terminal the moment the coordinator emits it; a completion candidate "
                + "that arrives afterwards races the cancellation and loses.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "taskId", "The task uuid whose open attempt is cancelled.");
        putString(properties, "reason", "Why the work is called off.");
        require(schema, "taskId", "reason");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String taskId = DelegationActionJson.uuid(input, "taskId");
        String reason = DelegationActionJson.text(input, "reason");
        try {
            bridge.cancel(taskId, reason);
        } catch (RuntimeException e) {
            throw failure(e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("taskId", taskId);
        return output;
    }
}

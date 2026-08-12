package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.TaskOffer;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.UUID;

/** Offers one bounded task attempt to an admitted worker, with its lease. */
final class DelegationOfferAction extends DelegationAction {

    DelegationOfferAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-offer";
    }

    @Override
    public String description() {
        return "Offers a bounded task to an admitted worker: the task spec (objective, scope, "
                + "constraints, required acceptance checks, context artifacts) as proto3 JSON, "
                + "plus the lease in seconds and an optional resume checkpoint. Returns the "
                + "emitted offer. The coordinator owns the lifecycle; this only adapts.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "workerId", "The admitted worker the offer addresses.");
        putString(properties, "taskId",
                "The task uuid; generated when absent. Re-offering a finished attempt uses "
                        + "the same task id and increments the attempt.");
        properties.putObject("spec")
                .put("type", "object")
                .put("description", "The TaskSpec as canonical proto3 JSON: objective, "
                        + "allowedScope, constraints, requiredChecks (at least one), context, "
                        + "deadline.");
        putInteger(properties, "leaseSeconds",
                "How long the lease runs without renewal, in seconds.", 1, 86_400);
        properties.putObject("resumeFrom")
                .put("type", "object")
                .put("description", "Optional CheckpointReference (attempt, checkpointSeq, "
                        + "resumeToken) into a prior attempt's recorded checkpoints.");
        require(schema, "workerId", "spec");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String workerId = DelegationActionJson.identity(input, "workerId");
        String taskId = DelegationActionJson.optionalUuid(input, "taskId");
        if (taskId == null) {
            taskId = UUID.randomUUID().toString();
        }
        TaskSpec spec = (TaskSpec) DelegationActionJson.parse(
                DelegationActionJson.object(input, "spec"), TaskSpec.newBuilder(), "/spec");
        int leaseSeconds = DelegationActionJson.boundedInt(input, "leaseSeconds", 300, 1, 86_400);
        CheckpointReference resumeFrom = null;
        if (input.has("resumeFrom") && !input.get("resumeFrom").isNull()) {
            resumeFrom = (CheckpointReference) DelegationActionJson.parse(
                    DelegationActionJson.object(input, "resumeFrom"),
                    CheckpointReference.newBuilder(), "/resumeFrom");
        }
        TaskOffer offer;
        try {
            offer = bridge.offer(workerId, taskId, spec, Duration.ofSeconds(leaseSeconds),
                    resumeFrom);
        } catch (RuntimeException e) {
            throw failure(workerId, e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.put("taskId", taskId);
        output.set("offer", DelegationActionJson.render(offer, context));
        return output;
    }
}

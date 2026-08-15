package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.TaskMessage;
import ai.pipestream.proto.delegation.v1.TaskMessageKind;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/** Sends one non-transitioning structured task message in either direction. */
final class DelegationMessageAction extends DelegationAction {

    DelegationMessageAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-message";
    }

    @Override
    public String description() {
        return "Sends a non-transitioning task message: a worker's question or note to the "
                + "coordinator, or the coordinator's answer or guidance to a worker. The "
                + "message is recorded and sequenced like any frame but never moves the "
                + "lifecycle. 'sender' is 'coordinator' or a registered worker id; kind is a "
                + "TaskMessageKind name (QUESTION, ANSWER, GUIDANCE, NOTE with the "
                + "TASK_MESSAGE_KIND_ prefix).";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        putString(properties, "taskId", "The task uuid the message concerns.");
        putString(properties, "sender",
                "'coordinator' or a registered worker id.");
        putString(properties, "recipient",
                "The worker id when the sender is the coordinator; defaults to "
                        + "'coordinator' for worker senders.");
        ObjectNode kind = properties.putObject("kind");
        kind.put("type", "string")
                .put("description", "The TaskMessageKind enum name.");
        kind.putArray("enum")
                .add("TASK_MESSAGE_KIND_QUESTION")
                .add("TASK_MESSAGE_KIND_ANSWER")
                .add("TASK_MESSAGE_KIND_GUIDANCE")
                .add("TASK_MESSAGE_KIND_NOTE");
        putString(properties, "text", "The bounded message body; long content belongs "
                + "in artifacts.");
        putString(properties, "replyTo", "The message uuid this answers, when it "
                + "continues a thread.");
        properties.putObject("artifacts")
                .put("type", "array")
                .put("maxItems", 32)
                .put("description", "ArtifactReference objects (proto3 JSON) the message "
                        + "refers to.")
                .putObject("items").put("type", "object");
        require(schema, "taskId", "sender", "kind", "text");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        String taskId = DelegationActionJson.uuid(input, "taskId");
        String sender = DelegationActionJson.identity(input, "sender");
        TaskMessageKind kind = kind(input);
        String text = DelegationActionJson.text(input, "text");
        String replyTo = DelegationActionJson.optionalUuid(input, "replyTo");
        List<ArtifactReference> artifacts = artifacts(input);
        TaskMessage message;
        try {
            if (DelegationValidation.COORDINATOR.equals(sender)) {
                String recipient = DelegationActionJson.optionalText(input, "recipient");
                if (recipient == null) {
                    throw DelegationActionJson.invalid(
                            "'recipient' must name the worker when the sender is the "
                                    + "coordinator", "/recipient");
                }
                message = bridge.sendCoordinatorMessage(recipient, taskId, kind, text,
                        replyTo, artifacts);
            } else {
                String recipient = DelegationActionJson.optionalText(input, "recipient");
                if (recipient != null
                        && !DelegationValidation.COORDINATOR.equals(recipient)) {
                    throw DelegationActionJson.invalid(
                            "a worker message is addressed to 'coordinator'", "/recipient");
                }
                message = bridge.sendWorkerMessage(sender, taskId, kind, text, replyTo,
                        artifacts);
            }
        } catch (RuntimeException e) {
            throw failure(sender, e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.set("message", DelegationActionJson.render(message, context));
        return output;
    }

    private static TaskMessageKind kind(ObjectNode input) throws ActionException {
        String value = DelegationActionJson.text(input, "kind");
        try {
            return TaskMessageKind.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw DelegationActionJson.invalid("'kind' must be a TaskMessageKind name",
                    "/kind");
        }
    }

    private static List<ArtifactReference> artifacts(ObjectNode input) throws ActionException {
        List<ArtifactReference> artifacts = new ArrayList<>();
        JsonNode node = input.get("artifacts");
        if (node == null || node.isNull()) {
            return artifacts;
        }
        if (!node.isArray()) {
            throw DelegationActionJson.invalid("'artifacts' must be an array", "/artifacts");
        }
        for (int i = 0; i < node.size(); i++) {
            if (!node.get(i).isObject()) {
                throw DelegationActionJson.invalid("'artifacts' entries must be objects",
                        "/artifacts/" + i);
            }
            artifacts.add((ArtifactReference) DelegationActionJson.parse(
                    (ObjectNode) node.get(i), ArtifactReference.newBuilder(),
                    "/artifacts/" + i));
        }
        return artifacts;
    }
}

package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.SendTaskMessageRequest;
import ai.pipestream.proto.delegation.v1.SendTaskMessageResponse;
import ai.pipestream.proto.delegation.v1.TaskMessage;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

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
    public Descriptor requestType() {
        return SendTaskMessageRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return SendTaskMessageResponse.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        // The two directions differ in who the recipient may be. Both rules are declared as
        // message-level CEL on the request, so by this point a coordinator message names its
        // worker and a worker message is addressed to the coordinator or to nobody.
        SendTaskMessageRequest request = DelegationActionJson
                .parse(input, SendTaskMessageRequest.newBuilder(), name()).build();
        String sender = request.getSender();
        TaskMessage message;
        try {
            if (DelegationValidation.COORDINATOR.equals(sender)) {
                message = bridge.sendCoordinatorMessage(request.getRecipient(),
                        request.getTaskId(), request.getKind(), request.getText(),
                        request.getReplyTo(), request.getArtifactsList());
            } else {
                message = bridge.sendWorkerMessage(sender, request.getTaskId(),
                        request.getKind(), request.getText(), request.getReplyTo(),
                        request.getArtifactsList());
            }
        } catch (RuntimeException e) {
            throw failure(sender, e);
        }
        return DelegationActionJson.render(SendTaskMessageResponse.newBuilder()
                .setOk(true)
                .setMessage(message)
                .build(), context);
    }
}

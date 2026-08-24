package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.CancelTaskRequest;
import ai.pipestream.proto.delegation.v1.CancelTaskResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

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
    public Descriptor requestType() {
        return CancelTaskRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return CancelTaskResponse.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CancelTaskRequest request =
                DelegationActionJson.parse(input, CancelTaskRequest.newBuilder(), name()).build();
        try {
            bridge.cancel(request.getTaskId(), request.getReason());
        } catch (RuntimeException e) {
            throw failure(e);
        }
        return DelegationActionJson.render(CancelTaskResponse.newBuilder()
                .setOk(true)
                .setTaskId(request.getTaskId())
                .build(), context);
    }
}

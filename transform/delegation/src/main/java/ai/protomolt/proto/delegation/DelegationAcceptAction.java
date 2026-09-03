package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.AcceptTaskResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor requestType() {
        return AcceptTaskRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return AcceptTaskResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        AcceptTaskRequest request = CatalogContract.as(
                input, AcceptTaskRequest.getDefaultInstance(), name());
        try {
            bridge.accept(request.getWorkerId(), request.getTaskId(), request.getAttempt());
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return AcceptTaskResponse.newBuilder()
                .setOk(true)
                .setTaskId(request.getTaskId())
                .setAttempt(request.getAttempt())
                .build();
    }
}

package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.AcceptTaskRequest;
import ai.pipestream.proto.delegation.v1.AcceptTaskResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

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
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        AcceptTaskRequest request =
                DelegationActionJson.parse(input, AcceptTaskRequest.newBuilder(), name()).build();
        try {
            bridge.accept(request.getWorkerId(), request.getTaskId(), request.getAttempt());
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return DelegationActionJson.render(AcceptTaskResponse.newBuilder()
                .setOk(true)
                .setTaskId(request.getTaskId())
                .setAttempt(request.getAttempt())
                .build(), context);
    }
}

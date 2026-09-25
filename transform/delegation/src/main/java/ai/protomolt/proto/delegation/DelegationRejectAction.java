package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.RejectTaskRequest;
import ai.protomolt.proto.delegation.v1.RejectTaskResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/** The addressed worker declines an open offer through the existing rejection frame. */
final class DelegationRejectAction extends DelegationAction {

    DelegationRejectAction(DelegationBridge bridge) { super(bridge); }

    @Override public String name() { return "delegation-reject"; }

    @Override public String description() {
        return "Declines the addressed worker's current open offer. The attempt ends; "
                + "retryable is advisory and does not create another offer.";
    }

    @Override public Descriptor requestType() { return RejectTaskRequest.getDescriptor(); }

    @Override public Descriptor responseType() { return RejectTaskResponse.getDescriptor(); }

    @Override public Message execute(Message input, ActionContext context) throws ActionException {
        RejectTaskRequest request = CatalogContract.as(
                input, RejectTaskRequest.getDefaultInstance(), name());
        try {
            bridge.reject(request.getWorkerId(), request.getTaskId(), request.getAttempt(),
                    request.getReason(), request.getRetryable());
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return RejectTaskResponse.newBuilder().setOk(true)
                .setTaskId(request.getTaskId()).setAttempt(request.getAttempt()).build();
    }
}

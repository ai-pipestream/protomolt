package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.delegation.v1.ReportProgressRequest;
import ai.pipestream.proto.delegation.v1.ReportProgressResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor requestType() {
        return ReportProgressRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return ReportProgressResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ReportProgressRequest request = CatalogContract.as(
                input, ReportProgressRequest.getDefaultInstance(), name());
        int progressSeq;
        try {
            progressSeq = bridge.progress(request.getWorkerId(), request.getTaskId(),
                    request.getAttempt(), request.getMessage());
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return ReportProgressResponse.newBuilder()
                .setOk(true)
                .setProgressSeq(progressSeq)
                .build();
    }
}

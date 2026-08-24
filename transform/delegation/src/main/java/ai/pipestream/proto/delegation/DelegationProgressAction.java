package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.ReportProgressRequest;
import ai.pipestream.proto.delegation.v1.ReportProgressResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

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
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ReportProgressRequest request = DelegationActionJson
                .parse(input, ReportProgressRequest.newBuilder(), name()).build();
        int progressSeq;
        try {
            progressSeq = bridge.progress(request.getWorkerId(), request.getTaskId(),
                    request.getAttempt(), request.getMessage());
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return DelegationActionJson.render(ReportProgressResponse.newBuilder()
                .setOk(true)
                .setProgressSeq(progressSeq)
                .build(), context);
    }
}

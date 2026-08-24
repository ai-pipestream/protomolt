package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.RecordCheckpointRequest;
import ai.pipestream.proto.delegation.v1.RecordCheckpointResponse;
import ai.pipestream.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/** Records one resumable checkpoint of worker state on the leased attempt. */
final class DelegationCheckpointAction extends DelegationAction {

    DelegationCheckpointAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-checkpoint";
    }

    @Override
    public String description() {
        return "Records one resumable checkpoint on the worker's leased attempt: a resume "
                + "token, an optional note, and an optional state artifact reference. "
                + "Checkpoint sequences are assigned by the bridge and never regress; a "
                + "later offer's resumeFrom echoes the token back.";
    }

    @Override
    public Descriptor requestType() {
        return RecordCheckpointRequest.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        RecordCheckpointRequest request = DelegationActionJson
                .parse(input, RecordCheckpointRequest.newBuilder(), name()).build();
        ArtifactReference state = request.hasState() ? request.getState() : null;
        int checkpointSeq;
        try {
            checkpointSeq = bridge.checkpoint(request.getWorkerId(), request.getTaskId(),
                    request.getAttempt(), request.getResumeToken(), request.getNote(), state);
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return DelegationActionJson.render(RecordCheckpointResponse.newBuilder()
                .setOk(true)
                .setCheckpointSeq(checkpointSeq)
                .setResumeToken(request.getResumeToken())
                .build(), context);
    }
}

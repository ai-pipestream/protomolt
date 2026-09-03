package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.RecordCheckpointRequest;
import ai.protomolt.proto.delegation.v1.RecordCheckpointResponse;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor responseType() {
        return RecordCheckpointResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        RecordCheckpointRequest request = CatalogContract.as(
                input, RecordCheckpointRequest.getDefaultInstance(), name());
        ArtifactReference state = request.hasState() ? request.getState() : null;
        int checkpointSeq;
        try {
            checkpointSeq = bridge.checkpoint(request.getWorkerId(), request.getTaskId(),
                    request.getAttempt(), request.getResumeToken(), request.getNote(), state);
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return RecordCheckpointResponse.newBuilder()
                .setOk(true)
                .setCheckpointSeq(checkpointSeq)
                .setResumeToken(request.getResumeToken())
                .build();
    }
}

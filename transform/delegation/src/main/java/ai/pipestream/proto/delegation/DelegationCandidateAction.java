package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.SubmitCandidateRequest;
import ai.pipestream.proto.delegation.v1.SubmitCandidateResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/** Submits one revision of completion evidence for coordinator review. */
final class DelegationCandidateAction extends DelegationAction {

    DelegationCandidateAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-candidate";
    }

    @Override
    public String description() {
        return "Submits a completion candidate for review: the CompletionCandidate as proto3 "
                + "JSON with the attempt, the expected revision, a summary, passing evidence "
                + "for every required check of the offer's spec, and at least one commit or "
                + "artifact reference. The coordinator answers with acceptance or a revision "
                + "request on the event feed; a worker can never mark its own task done.";
    }

    @Override
    public Descriptor requestType() {
        return SubmitCandidateRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return SubmitCandidateResponse.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SubmitCandidateRequest request = DelegationActionJson
                .parse(input, SubmitCandidateRequest.newBuilder(), name()).build();
        CompletionCandidate candidate = request.getCandidate();
        try {
            bridge.submitCandidate(request.getWorkerId(), request.getTaskId(), candidate);
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return DelegationActionJson.render(SubmitCandidateResponse.newBuilder()
                .setOk(true)
                .setTaskId(request.getTaskId())
                .setAttempt(candidate.getAttempt())
                .setRevision(candidate.getRevision())
                .build(), context);
    }
}

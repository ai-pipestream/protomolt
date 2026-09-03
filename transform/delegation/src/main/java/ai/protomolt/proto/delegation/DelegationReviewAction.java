package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.ReviewCandidateRequest;
import ai.protomolt.proto.delegation.v1.ReviewCandidateResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/** Applies an external review decision to the open completion candidate. */
final class DelegationReviewAction extends DelegationAction {

    DelegationReviewAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-review";
    }

    @Override
    public String description() {
        return "Applies the review verdict for a task's open completion candidate: "
                + "REVIEW_DECISION_ACCEPT with a verdict line, or REVIEW_DECISION_REVISE "
                + "with feedback and the failed checks. The candidate must be under review "
                + "(submitted, not yet decided).";
    }

    @Override
    public Descriptor requestType() {
        return ReviewCandidateRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return ReviewCandidateResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // Which fields each decision needs is declared as message-level CEL on the request:
        // an acceptance carries its verdict, a revision request carries its feedback, and
        // failed checks belong only to a revision request.
        ReviewCandidateRequest request = CatalogContract.as(
                input, ReviewCandidateRequest.getDefaultInstance(), name());
        CandidateReviewer.ReviewDecision review = switch (request.getDecision()) {
            case REVIEW_DECISION_ACCEPT ->
                    CandidateReviewer.ReviewDecision.accept(request.getVerdict());
            case REVIEW_DECISION_REVISE ->
                    CandidateReviewer.ReviewDecision.revise(request.getFeedback(),
                            request.getFailedChecksList());
            default -> throw DelegationFailures.rejected(
                    "delegation-review needs a decision the contract defines");
        };
        try {
            bridge.review(request.getTaskId(), review);
        } catch (RuntimeException e) {
            throw failure(e);
        }
        return ReviewCandidateResponse.newBuilder()
                .setOk(true)
                .setTaskId(request.getTaskId())
                .setDecision(request.getDecision())
                .build();
    }
}

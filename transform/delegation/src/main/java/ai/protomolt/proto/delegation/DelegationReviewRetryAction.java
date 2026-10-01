package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewRequest;
import ai.protomolt.proto.delegation.v1.RetryCandidateReviewResponse;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/** Restarts a failed review through the coordinator's durable retry boundary. */
final class DelegationReviewRetryAction extends DelegationAction {
    DelegationReviewRetryAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-review-retry";
    }

    @Override
    public String description() {
        return "Retries the identified failed review of the current candidate. "
                + "Reuse the same retry UUID and request after a lost reply. "
                + "A committed retry returns its recorded invocation without dispatching again. "
                + "Deferred manual reviews cannot retry.";
    }

    @Override
    public Descriptor requestType() {
        return RetryCandidateReviewRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return RetryCandidateReviewResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        RetryCandidateReviewRequest request = CatalogContract.as(
                input, RetryCandidateReviewRequest.getDefaultInstance(), name());
        try {
            return bridge.retryCandidateReview(request);
        } catch (RuntimeException failure) {
            throw failure(failure);
        }
    }
}

package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.v1.DelegateResponse;

/**
 * The coordinator frame-emission surface used by the extracted review-runtime and
 * lease-schedule helpers. The {@link InProcessDelegationCoordinator} alone implements
 * this: every emission still sequences the frame on the worker's session, validates it,
 * appends it to the durable transcript under the coordinator's monitor, runs the
 * projection mutation, and only then attempts delivery. Extracting the callers does not
 * move that authority.
 *
 * <p>Implementations must be invoked while holding the coordinator's monitor, exactly as
 * the private emission methods they delegate to.
 */
interface CoordinatorFrames {

    /** Emits one coordinator frame with no follow-up projection mutation. */
    void emit(DelegationCoordinatorState.Session session, String taskId, int attempt,
              DelegateResponse.Builder payload);

    /** Emits one coordinator frame, running {@code afterPersist} once the frame is recorded. */
    void emit(DelegationCoordinatorState.Session session, String taskId, int attempt,
              DelegateResponse.Builder payload, Runnable afterPersist);

    /** Applies an external review decision to its identified completion candidate. */
    void applyReview(DelegationCoordinatorState.TaskRuntime task, ReviewDecision decision,
                     String invocationId);
}

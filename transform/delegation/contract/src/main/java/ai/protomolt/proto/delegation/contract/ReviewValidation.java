package ai.protomolt.proto.delegation.contract;

import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.ReviewDeferred;
import ai.protomolt.proto.delegation.v1.ReviewFailed;
import ai.protomolt.proto.delegation.v1.ReviewFailureCode;
import ai.protomolt.proto.delegation.v1.ReviewStarted;

/**
 * The review-identity binding checks of the delegation contract: the candidate review
 * identity that ties a review to its offer entry, candidate entry, attempt, and revision,
 * and the review-started / review-failed / review-deferred payloads that carry it.
 *
 * <p>These are the checks {@link DelegationValidation}'s review entry points delegate to;
 * moving them here keeps the candidate/attempt/revision/review identity rules in one unit
 * without changing which admission point runs them or in what order.
 */
final class ReviewValidation {

    private ReviewValidation() {
    }

    /** Validates a candidate review identity: task, worker, attempt, revision, invocation, digests. */
    static void validate(CandidateReviewIdentity identity) {
        ValidationChecks.require(identity != null, "review.identity must not be null");
        ValidationChecks.requireNoUnknown(identity, "review.identity");
        ValidationChecks.require(Formats.isUuid(identity.getTaskId()),
                "review.identity.task_id must be a uuid");
        ValidationChecks.validateName(identity.getWorkerId(), "review.identity.worker_id");
        ValidationChecks.validateAttempt(identity.getAttempt(), "review.identity.attempt");
        ValidationChecks.validateRevision(identity.getRevision(), "review.identity.revision");
        ValidationChecks.validateCanonicalUuid(identity.getInvocationId(),
                "review.identity.invocation_id");
        ValidationChecks.validateDigest(identity.getOfferEntrySha256(),
                "review.identity.offer_entry_sha256");
        ValidationChecks.validateDigest(identity.getCandidateEntrySha256(),
                "review.identity.candidate_entry_sha256");
    }

    /** Validates a review start: identity, timestamps, window bound, and retry linkage. */
    static void validate(ReviewStarted started) {
        ValidationChecks.require(started != null, "review_started must not be null");
        ValidationChecks.requireNoUnknown(started, "review_started");
        ValidationChecks.require(started.hasIdentity(), "review_started.identity is required");
        validate(started.getIdentity());
        ValidationChecks.require(started.hasStartedAt() && started.hasDeadline(),
                "review_started timestamps are required");
        ValidationChecks.validateTimestamp(started.getStartedAt(), "review_started.started_at");
        ValidationChecks.validateTimestamp(started.getDeadline(), "review_started.deadline");
        long seconds = started.getDeadline().getSeconds() - started.getStartedAt().getSeconds();
        int nanos = started.getDeadline().getNanos() - started.getStartedAt().getNanos();
        ValidationChecks.require(seconds > 0 || (seconds == 0 && nanos > 0),
                "review_started.deadline must follow started_at");
        ValidationChecks.require(seconds < 3600 || (seconds == 3600 && nanos <= 0),
                "review_started deadline window exceeds one hour");
        ValidationChecks.require(
                started.getRetryId().isEmpty() == started.getPreviousInvocationId().isEmpty(),
                "review_started retry fields must occur together");
        ValidationChecks.validateOptionalCanonicalUuid(started.getRetryId(),
                "review_started.retry_id");
        ValidationChecks.validateOptionalCanonicalUuid(started.getPreviousInvocationId(),
                "review_started.previous_invocation_id");
        ValidationChecks.require(
                !started.getPreviousInvocationId().equals(started.getIdentity().getInvocationId()),
                "review_started must use a new invocation_id");
    }

    /** Validates a review failure: identity plus a defined failure code. */
    static void validate(ReviewFailed failed) {
        ValidationChecks.require(failed != null && failed.hasIdentity(),
                "review_failed.identity is required");
        ValidationChecks.requireNoUnknown(failed, "review_failed");
        validate(failed.getIdentity());
        ValidationChecks.require(failed.getCode() == ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE
                        || failed.getCode() == ReviewFailureCode.REVIEW_FAILURE_CODE_DEADLINE
                        || failed.getCode() == ReviewFailureCode.REVIEW_FAILURE_CODE_INTERRUPTED,
                "review_failed.code must be a defined failure code");
    }

    /** Validates a review deferral: the identity of the invocation left open. */
    static void validate(ReviewDeferred deferred) {
        ValidationChecks.require(deferred != null && deferred.hasIdentity(),
                "review_deferred.identity is required");
        ValidationChecks.requireNoUnknown(deferred, "review_deferred");
        validate(deferred.getIdentity());
    }
}

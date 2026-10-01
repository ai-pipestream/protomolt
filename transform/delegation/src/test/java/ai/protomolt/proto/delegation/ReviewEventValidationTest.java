package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.CompletionAccepted;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.ReviewDeferred;
import ai.protomolt.proto.delegation.v1.ReviewFailed;
import ai.protomolt.proto.delegation.v1.ReviewFailureCode;
import ai.protomolt.proto.delegation.v1.ReviewStarted;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReviewEventValidationTest {
    private static final String TASK = "49e553aa-df4c-4f19-8c27-51655ecc0b53";
    private static final String INVOCATION = "11111111-1111-4111-8111-111111111111";
    private static final Timestamp START = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();

    private static CandidateReviewIdentity identity() {
        return CandidateReviewIdentity.newBuilder()
                .setTaskId(TASK).setWorkerId("worker-review")
                .setAttempt(1).setRevision(1).setInvocationId(INVOCATION)
                .setOfferEntrySha256("a".repeat(64)).setCandidateEntrySha256("b".repeat(64))
                .build();
    }

    private static DelegateResponse.Builder envelope() {
        return DelegateResponse.newBuilder()
                .setFrameId("22222222-2222-4222-8222-222222222222")
                .setTaskId(TASK).setSeq(5).setSentAt(START);
    }

    private static ValidationResult validate(Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
    }

    private static ValidationResult validateDynamic(Message message) throws Exception {
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        return ProtoValidator.forMessageType(dynamic.getDescriptorForType()).validate(dynamic);
    }

    private static void validBoth(Message message) throws Exception {
        ValidationResult generated = validate(message);
        ValidationResult dynamic = validateDynamic(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isTrue();
        assertThat(dynamic.valid()).as("dynamic violations: %s", dynamic.violations()).isTrue();
    }

    private static void invalidBoth(Message message) throws Exception {
        ValidationResult generated = validate(message);
        ValidationResult dynamic = validateDynamic(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isFalse();
        assertThat(dynamic.valid()).as("dynamic violations: %s", dynamic.violations()).isFalse();
    }

    @Test
    void startEnvelopeBindsReviewIdentityTaskAndRecordedStartTime() throws Exception {
        ReviewStarted started = ReviewStarted.newBuilder().setIdentity(identity()).setStartedAt(START)
                .setDeadline(Timestamp.newBuilder().setSeconds(1_700_000_300L)).build();
        validBoth(envelope().setReviewStarted(started).build());

        DelegateResponse wrongTask = envelope().setReviewStarted(started.toBuilder()
                .setIdentity(identity().toBuilder().setTaskId("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"))).build();
        invalidBoth(wrongTask);
        assertRule(wrongTask, "review-event-matches-envelope");

        DelegateResponse wrongStartTime = envelope().setSentAt(START.toBuilder().setNanos(1))
                .setReviewStarted(started).build();
        invalidBoth(wrongStartTime);
        assertRule(wrongStartTime, "review-event-matches-envelope");
    }

    @Test
    void failedAndDeferredEventsRequireIdentityAndMatchEnvelopeTask() throws Exception {
        validBoth(envelope().setReviewFailed(ReviewFailed.newBuilder().setIdentity(identity())
                .setCode(ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE)).build());
        validBoth(envelope().setReviewDeferred(ReviewDeferred.newBuilder().setIdentity(identity())).build());

        DelegateResponse missingFailureIdentity = envelope().setReviewFailed(ReviewFailed.newBuilder()
                .setCode(ReviewFailureCode.REVIEW_FAILURE_CODE_DEADLINE)).build();
        invalidBoth(missingFailureIdentity);
        DelegateResponse wrongDeferredTask = envelope().setReviewDeferred(ReviewDeferred.newBuilder()
                .setIdentity(identity().toBuilder().setTaskId("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"))).build();
        invalidBoth(wrongDeferredTask);
        assertRule(wrongDeferredTask, "review-event-matches-envelope");
    }

    @Test
    void verdictInvocationIdsAreCanonicalUuidOrOmittedForManualAndLegacyVerdicts() throws Exception {
        CompletionAccepted accepted = CompletionAccepted.newBuilder()
                .setAttempt(1).setRevision(1).setVerdict("accepted").build();
        validBoth(accepted);
        validBoth(accepted.toBuilder().setReviewInvocationId(INVOCATION).build());
        invalidBoth(accepted.toBuilder().setReviewInvocationId("not-a-uuid").build());
        invalidBoth(accepted.toBuilder().setReviewInvocationId("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA").build());

        RevisionRequested requested = RevisionRequested.newBuilder().setAttempt(1)
                .setRevision(1).setFeedback("please fix the missing check").build();
        validBoth(requested);
        validBoth(requested.toBuilder().setReviewInvocationId(INVOCATION).build());
        invalidBoth(requested.toBuilder().setReviewInvocationId("bad").build());
    }

    private static void assertRule(Message message, String ruleId) {
        assertThat(validate(message).violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo(ruleId));
    }
}

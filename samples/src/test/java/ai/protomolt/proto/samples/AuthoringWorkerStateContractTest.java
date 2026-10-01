package ai.protomolt.proto.samples;

import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CommitReference;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerPending;
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerState;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Runtime shape checks for the sample worker's private restart state. */
class AuthoringWorkerStateContractTest {
    private static final String TASK = "00000000-0000-4000-8000-0000000000a1";
    private static final String PREPARATION = "00000000-0000-4000-8000-0000000000b2";
    private static final String HASH = "a".repeat(64);

    @Test
    void stateRequiresSupportedVersionConfiguredTargetsAndWorkerIdentity() {
        var validator = ProtoValidator.forMessageType(AuthoringWorkerState.getDescriptor());
        AuthoringWorkerState valid = state().build();
        assertValidBoth(validator, valid);
        assertInvalidBoth(validator, valid.toBuilder().setFormatVersion(0).build());
        assertInvalidBoth(validator, valid.toBuilder().setFormatVersion(2).build());
        assertInvalidBoth(validator, valid.toBuilder().clearCoordinator().build());
        assertInvalidBoth(validator, valid.toBuilder().clearFixture().build());
        assertInvalidBoth(validator, valid.toBuilder().clearWorkerId().build());
        assertInvalidBoth(validator, valid.toBuilder().setCoordinator("x".repeat(2049)).build());
        assertInvalidBoth(validator, valid.toBuilder().setFixture("x".repeat(2049)).build());
        assertInvalidBoth(validator, valid.toBuilder().setWorkerId("not a worker").build());
        assertInvalidBoth(validator, valid.toBuilder().setDiscoveryCursor(-1).build());
    }

    @Test
    void pendingQueueIsBoundedAndItsAssignmentsCannotBeAheadOfTheSavedWatermark() {
        var validator = ProtoValidator.forMessageType(AuthoringWorkerState.getDescriptor());
        var maximum = state().setDiscoveryCursor(64);
        for (int i = 1; i <= 64; i++) {
            maximum.addPending(pending(i).build());
        }
        assertValidBoth(validator, maximum.build());
        assertInvalidBoth(validator, maximum.addPending(pending(65).build()).build());

        AuthoringWorkerState behind = state().setDiscoveryCursor(4)
                .addPending(pending(5).build()).build();
        assertRule(behind, "worker-state-assignment-cursor");
        assertValidBoth(validator, state().setDiscoveryCursor(Long.MAX_VALUE)
                .addPending(pending(Long.MAX_VALUE).build()).build());
    }

    @Test
    void assignmentIdentityMustBeWellFormed() {
        var validator = ProtoValidator.forMessageType(WorkflowAuthorAssignment.getDescriptor());
        WorkflowAuthorAssignment assignment = assignment(1).build();
        assertValidBoth(validator, assignment);
        assertInvalidBoth(validator, WorkflowAuthorAssignment.getDefaultInstance());
        assertInvalidBoth(validator, assignment.toBuilder().setCursor(0).build());
        assertInvalidBoth(validator, assignment.toBuilder().setTaskId("not-a-uuid").build());
        assertInvalidBoth(validator, assignment.toBuilder().setAttempt(0).build());
        assertInvalidBoth(validator, assignment.toBuilder().setAttempt(1025).build());
        assertInvalidBoth(validator, assignment.toBuilder().setOfferEntrySha256("bad").build());
    }

    @Test
    void preparationAndSubmissionRequireTheirPredecessorsAndMatchingAssignmentIdentity() {
        var validator = ProtoValidator.forMessageType(AuthoringWorkerPending.getDescriptor());
        AuthoringWorkerPending base = pending(1).build();
        assertValidBoth(validator, base);

        AuthoringWorkerPending withProbe = base.toBuilder().setProbe(probe()).build();
        assertValidBoth(validator, withProbe);
        AuthoringWorkerPending prepared = withProbe.toBuilder().setPreparation(preparation()).build();
        assertValidBoth(validator, prepared);
        AuthoringWorkerPending submitted = prepared.toBuilder().setSubmission(submission())
                .setReviewCursor(4).setSubmissionCursor(3).build();
        assertValidBoth(validator, submitted);

        assertRule(base.toBuilder().setPreparation(preparation()).build(), "worker-pending-intent-order");
        assertRule(withProbe.toBuilder().setSubmission(submission()).build(), "worker-pending-intent-order");

        assertRule(withProbe.toBuilder().setPreparation(preparation().toBuilder().setTaskId(
                "00000000-0000-4000-8000-0000000000c3")).build(), "worker-pending-preparation-identity");
        assertRule(withProbe.toBuilder().setPreparation(preparation().toBuilder().setAttempt(2)).build(),
                "worker-pending-preparation-identity");
        assertRule(withProbe.toBuilder().setPreparation(preparation().toBuilder().setRevision(2)).build(),
                "worker-pending-preparation-identity");

        assertRule(prepared.toBuilder().setSubmission(submission().toBuilder().setTaskId(
                "00000000-0000-4000-8000-0000000000c3")).build(), "worker-pending-submission-identity");
        assertRule(prepared.toBuilder().setSubmission(submission().toBuilder().setCandidate(
                candidate().toBuilder().setAttempt(2)).build()).build(), "worker-pending-submission-identity");
        assertRule(prepared.toBuilder().setSubmission(submission().toBuilder().setCandidate(
                candidate().toBuilder().setRevision(2)).build()).build(), "worker-pending-submission-identity");

        assertRule(prepared.toBuilder().setSubmissionCursor(1).build(), "worker-pending-observed-submission");
        assertRule(prepared.toBuilder().setSubmission(submission()).setSubmissionCursor(2)
                .setReviewCursor(1).build(), "worker-pending-observed-submission");
    }

    @Test
    void submittedStateMustBelongToTheConfiguredWorker() {
        var validator = ProtoValidator.forMessageType(AuthoringWorkerState.getDescriptor());
        AuthoringWorkerPending pending = pending(1).setProbe(probe()).setPreparation(preparation())
                .setSubmission(submission()).build();
        assertValidBoth(validator, state().setDiscoveryCursor(1).addPending(pending).build());
        assertRule(state().setDiscoveryCursor(1).setWorkerId("another-worker").addPending(pending).build(),
                "worker-state-submission-owner");
    }

    private static AuthoringWorkerState.Builder state() {
        return AuthoringWorkerState.newBuilder().setFormatVersion(1)
                .setCoordinator("grpc://coordinator.example:8443")
                .setFixture("grpc://fixture.example:9443").setWorkerId("workflow-author");
    }

    private static AuthoringWorkerPending.Builder pending(long cursor) {
        return AuthoringWorkerPending.newBuilder().setAssignment(assignment(cursor).build());
    }

    private static WorkflowAuthorAssignment.Builder assignment(long cursor) {
        return WorkflowAuthorAssignment.newBuilder().setCursor(cursor).setTaskId(TASK)
                .setAttempt(1).setOfferEntrySha256(HASH);
    }

    private static WriteRecordRequest probe() {
        return WriteRecordRequest.newBuilder().setOperationId(PREPARATION).setContent("probe").build();
    }

    private static PrepareWorkflowCandidateRequest preparation() {
        return PrepareWorkflowCandidateRequest.newBuilder().setTaskId(TASK).setAttempt(1).setRevision(1)
                .setPreparationId(PREPARATION).setExecutableSourceJson(com.google.protobuf.ByteString.copyFromUtf8("{}"))
                .build();
    }

    private static SubmitCandidateRequest submission() {
        return SubmitCandidateRequest.newBuilder().setWorkerId("workflow-author").setTaskId(TASK)
                .setCandidate(candidate()).build();
    }

    private static CompletionCandidate candidate() {
        return CompletionCandidate.newBuilder().setAttempt(1).setRevision(1).setSummary("Completed")
                .addEvidence(CheckEvidence.newBuilder().setCheckName("tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)))
                .addCommits(CommitReference.newBuilder().setRepository("example/repo")
                        .setCommit("b".repeat(40)).setSubject("Complete sample task"))
                .build();
    }

    private static void assertRule(Message message, String ruleId) {
        ProtoValidator validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
        DynamicMessage dynamic = DynamicMessage.newBuilder(message.getDescriptorForType()).mergeFrom(message).build();
        assertThat(validator.validate(dynamic).violations()).anySatisfy(
                violation -> assertThat(violation.ruleId()).isEqualTo(ruleId));
    }

    private static void assertValidBoth(ProtoValidator validator, Message message) {
        var generated = validator.validate(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isTrue();
        DynamicMessage dynamic = DynamicMessage.newBuilder(message.getDescriptorForType()).mergeFrom(message).build();
        var dynamicResult = validator.validate(dynamic);
        assertThat(dynamicResult.valid()).as("dynamic violations: %s", dynamicResult.violations()).isTrue();
    }

    private static void assertInvalidBoth(ProtoValidator validator, Message message) {
        assertThat(validator.validate(message).valid()).isFalse();
        DynamicMessage dynamic = DynamicMessage.newBuilder(message.getDescriptorForType()).mergeFrom(message).build();
        assertThat(validator.validate(dynamic).valid()).isFalse();
    }
}

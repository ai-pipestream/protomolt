package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.DelegationReducer.ReviewStatus;
import ai.protomolt.proto.delegation.v1.CandidateReviewIdentity;
import ai.protomolt.proto.delegation.v1.CompletionAccepted;
import ai.protomolt.proto.delegation.v1.DelegateResponse;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.ReviewDeferred;
import ai.protomolt.proto.delegation.v1.ReviewFailed;
import ai.protomolt.proto.delegation.v1.ReviewFailureCode;
import ai.protomolt.proto.delegation.v1.ReviewStarted;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.protomolt.proto.delegation.DelegationFixtures.SECOND_WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static ai.protomolt.proto.delegation.DelegationFixtures.uuid;
import static org.assertj.core.api.Assertions.assertThat;

/** Adversarial lifecycle coverage for durable, transcript-bound reviewer invocations. */
class DelegationReviewReducerTest {
    private static final String INVOCATION_1 = "11111111-1111-4111-8111-111111111111";
    private static final String INVOCATION_2 = "22222222-2222-4222-8222-222222222222";
    private static final String INVOCATION_3 = "33333333-3333-4333-8333-333333333333";
    private static final String RETRY_1 = "44444444-4444-4444-8444-444444444444";
    private static final String RETRY_2 = "55555555-5555-4555-8555-555555555555";
    private static final Timestamp REVIEW_AT = Timestamp.newBuilder().setSeconds(1_700_001_000L).build();

    private final DelegationReducer reducer = new DelegationReducer();

    private static Transcript candidateTranscript(String task, String worker) {
        var taskSpec = spec("compile");
        return new DelegationFixtures.TranscriptBuilder()
                .hello(worker).admit(worker)
                .offer(task, worker, 1, taskSpec)
                .accept(task, worker, 1)
                .candidate(task, worker, 1, taskSpec)
                .build();
    }

    private static Transcript twoCandidateTasks() {
        var taskSpec = spec("compile");
        String secondTask = uuid("second-review-task");
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, taskSpec).accept(TASK, WORKER, 1)
                .candidate(TASK, WORKER, 1, taskSpec)
                .offer(secondTask, WORKER, 1, taskSpec).accept(secondTask, WORKER, 1)
                .candidate(secondTask, WORKER, 1, taskSpec)
                .build();
    }

    private static TranscriptEntry offerEntry(Transcript transcript, String task) {
        return transcript.getEntriesList().stream()
                .filter(entry -> entry.hasCoordinatorFrame()
                        && entry.getCoordinatorFrame().getTaskId().equals(task)
                        && entry.getCoordinatorFrame().hasOffer())
                .findFirst().orElseThrow();
    }

    private static TranscriptEntry candidateEntry(Transcript transcript, String task) {
        return transcript.getEntriesList().stream()
                .filter(entry -> entry.hasWorkerFrame()
                        && entry.getWorkerFrame().getTaskId().equals(task)
                        && entry.getWorkerFrame().hasCompletion())
                .findFirst().orElseThrow();
    }

    private static CandidateReviewIdentity identity(Transcript transcript, String task, String invocation) {
        return DelegationReviewBindings.identity(offerEntry(transcript, task),
                candidateEntry(transcript, task), invocation);
    }

    private static Transcript start(Transcript transcript, CandidateReviewIdentity identity,
                                    String previousInvocation, String retryId, String tag) {
        return startInTask(transcript, identity.getTaskId(), identity, previousInvocation, retryId, tag);
    }

    private static Transcript startInTask(Transcript transcript, String envelopeTask,
            CandidateReviewIdentity identity, String previousInvocation, String retryId, String tag) {
        ReviewStarted.Builder started = ReviewStarted.newBuilder().setIdentity(identity)
                .setStartedAt(REVIEW_AT)
                .setDeadline(Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() + 300));
        if (previousInvocation != null) started.setPreviousInvocationId(previousInvocation);
        if (retryId != null) started.setRetryId(retryId);
        return appendCoordinator(transcript, envelopeTask, identity.getAttempt(), tag,
                DelegateResponse.newBuilder().setReviewStarted(started));
    }

    private static Transcript fail(Transcript transcript, CandidateReviewIdentity identity,
                                   ReviewFailureCode code, String tag) {
        return appendCoordinator(transcript, identity.getTaskId(), identity.getAttempt(), tag,
                DelegateResponse.newBuilder().setReviewFailed(ReviewFailed.newBuilder()
                        .setIdentity(identity).setCode(code)));
    }

    private static Transcript defer(Transcript transcript, CandidateReviewIdentity identity, String tag) {
        return appendCoordinator(transcript, identity.getTaskId(), identity.getAttempt(), tag,
                DelegateResponse.newBuilder().setReviewDeferred(ReviewDeferred.newBuilder().setIdentity(identity)));
    }

    private static Transcript accept(Transcript transcript, CandidateReviewIdentity identity,
                                     String invocationId, String tag) {
        CompletionAccepted.Builder accepted = CompletionAccepted.newBuilder()
                .setAttempt(identity.getAttempt()).setRevision(identity.getRevision()).setVerdict("accepted");
        if (invocationId != null) accepted.setReviewInvocationId(invocationId);
        return appendCoordinator(transcript, identity.getTaskId(), identity.getAttempt(), tag,
                DelegateResponse.newBuilder().setAccepted(accepted));
    }

    private static Transcript requestRevision(Transcript transcript, CandidateReviewIdentity identity,
                                              String invocationId, String tag) {
        RevisionRequested.Builder request = RevisionRequested.newBuilder()
                .setAttempt(identity.getAttempt()).setRevision(identity.getRevision())
                .setFeedback("please correct the reviewed issue");
        if (invocationId != null) request.setReviewInvocationId(invocationId);
        return appendCoordinator(transcript, identity.getTaskId(), identity.getAttempt(), tag,
                DelegateResponse.newBuilder().setRevisionRequested(request));
    }

    private static Transcript cancel(Transcript transcript, String task, int attempt, String tag) {
        return appendCoordinator(transcript, task, attempt, tag,
                DelegateResponse.newBuilder().setCancellation(
                        ai.protomolt.proto.delegation.v1.Cancellation.newBuilder()
                                .setAttempt(attempt).setReason("cancelled by operator")));
    }

    private static Transcript appendCoordinator(Transcript transcript, String task, int attempt,
                                                String tag, DelegateResponse.Builder payload) {
        long nextSeq = transcript.getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_COORDINATOR
                        && entry.getWorkerId().equals(WORKER)
                        && entry.getCoordinatorFrame().getTaskId().equals(task)
                        && responseAttempt(entry.getCoordinatorFrame()) == attempt)
                .mapToLong(entry -> entry.getCoordinatorFrame().getSeq()).max().orElse(0) + 1;
        DelegateResponse frame = payload.setFrameId(uuid("review-frame-" + tag))
                .setTaskId(task).setSeq(nextSeq).setSentAt(REVIEW_AT).build();
        TranscriptEntry entry = TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR)
                .setWorkerId(WORKER).setCoordinatorFrame(frame).build();
        return transcript.toBuilder().addEntries(entry).build();
    }

    private static int responseAttempt(DelegateResponse response) {
        return switch (response.getPayloadCase()) {
            case OFFER -> response.getOffer().getAttempt();
            case RENEWAL -> response.getRenewal().getAttempt();
            case EXPIRED -> response.getExpired().getAttempt();
            case CANCELLATION -> response.getCancellation().getAttempt();
            case REVISION_REQUESTED -> response.getRevisionRequested().getAttempt();
            case ACCEPTED -> response.getAccepted().getAttempt();
            case REVIEW_STARTED -> response.getReviewStarted().getIdentity().getAttempt();
            case REVIEW_FAILED -> response.getReviewFailed().getIdentity().getAttempt();
            case REVIEW_DEFERRED -> response.getReviewDeferred().getIdentity().getAttempt();
            default -> 0;
        };
    }

    private static DelegationReducer.TaskState state(DelegationReducer.Result result, String task) {
        return result.tasks().get(task);
    }

    @Test
    void validStartTracksRunningReviewAndIdentityUsesFullEntryDigests() {
        Transcript base = candidateTranscript(TASK, WORKER);
        TranscriptEntry offer = offerEntry(base, TASK);
        TranscriptEntry candidate = candidateEntry(base, TASK);
        CandidateReviewIdentity identity = identity(base, TASK, INVOCATION_1);
        assertThat(identity.getOfferEntrySha256()).isEqualTo(DelegationReviewBindings.digest(offer));
        assertThat(identity.getCandidateEntrySha256()).isEqualTo(DelegationReviewBindings.digest(candidate));

        DelegationReducer.Result result = reducer.reduce(start(base, identity, null, null, "valid-start"));
        assertThat(result.clean()).as("findings: %s", result.findings()).isTrue();
        assertThat(state(result, TASK).review().status()).isEqualTo(ReviewStatus.RUNNING);
        assertThat(state(result, TASK).review().started().getIdentity()).isEqualTo(identity);

        TranscriptEntry changedCandidate = candidate.toBuilder().setWorkerFrame(candidate.getWorkerFrame()
                .toBuilder().setCompletion(candidate.getWorkerFrame().getCompletion().toBuilder()
                        .setSummary("same candidate identity, changed complete frame bytes"))).build();
        assertThat(DelegationReviewBindings.digest(changedCandidate)).isNotEqualTo(identity.getCandidateEntrySha256());
        CandidateReviewIdentity staleDigest = identity.toBuilder()
                .setCandidateEntrySha256(DelegationReviewBindings.digest(changedCandidate)).build();
        DelegationReducer.Result tampered = reducer.reduce(start(base, staleDigest, null, null, "stale-digest"));
        assertThat(tampered.clean()).isFalse();
    }

    @Test
    void startIdentityMustMatchSelectedTaskWorkerAttemptRevisionAndEntryDigests() {
        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity valid = identity(base, TASK, INVOCATION_1);
        List<CandidateReviewIdentity> mismatches = List.of(
                valid.toBuilder().setTaskId(uuid("other-task")).build(),
                valid.toBuilder().setWorkerId(SECOND_WORKER).build(),
                valid.toBuilder().setAttempt(2).build(),
                valid.toBuilder().setRevision(2).build(),
                valid.toBuilder().setOfferEntrySha256("c".repeat(64)).build(),
                valid.toBuilder().setCandidateEntrySha256("d".repeat(64)).build());
        int index = 0;
        for (CandidateReviewIdentity mismatch : mismatches) {
            Transcript resultTranscript = startInTask(base, TASK, mismatch, null, null, "mismatch-" + index++);
            DelegationReducer.Result result = reducer.reduce(resultTranscript);
            assertThat(result.clean()).as("mismatch %s should be rejected", mismatch).isFalse();
        }
    }

    @Test
    void globalInvocationAndRetryIdsCannotBeReused() {
        Transcript two = twoCandidateTasks();
        String secondTask = candidateEntry(two, uuid("second-review-task")).getWorkerFrame().getTaskId();
        int firstCandidateIndex = two.getEntriesList().indexOf(candidateEntry(two, TASK));
        Transcript firstCandidate = Transcript.newBuilder().addAllEntries(
                two.getEntriesList().subList(0, firstCandidateIndex + 1)).build();
        Transcript firstStarted = start(firstCandidate, identity(two, TASK, INVOCATION_1),
                null, null, "global-first");
        assertThat(reducer.reduce(firstStarted).clean()).isTrue();
        Transcript bothCandidates = firstStarted.toBuilder().addAllEntries(
                two.getEntriesList().subList(firstCandidateIndex + 1, two.getEntriesCount())).build();
        assertThat(reducer.reduce(bothCandidates).clean()).isTrue();
        Transcript bothStarted = start(bothCandidates,
                identity(two, secondTask, INVOCATION_1), null, null, "global-second");
        assertThat(reducer.reduce(bothStarted).findings())
                .anySatisfy(finding -> assertThat(finding.kind()).isEqualTo("duplicate"));

        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity first = identity(base, TASK, INVOCATION_1);
        Transcript initial = start(base, first, null, null, "retry-start-1");
        assertThat(reducer.reduce(initial).clean()).isTrue();
        Transcript failed = fail(initial, first,
                ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE, "retry-fail-1");
        assertThat(reducer.reduce(failed).clean()).isTrue();
        CandidateReviewIdentity second = identity(base, TASK, INVOCATION_2);
        Transcript retryStarted = start(failed, second, INVOCATION_1, RETRY_1, "retry-start-2");
        assertThat(reducer.reduce(retryStarted).clean()).isTrue();
        Transcript retryFailed = fail(retryStarted, second,
                ReviewFailureCode.REVIEW_FAILURE_CODE_INTERRUPTED, "retry-fail-2");
        assertThat(reducer.reduce(retryFailed).clean()).isTrue();
        CandidateReviewIdentity third = identity(base, TASK, INVOCATION_3);
        Transcript duplicateRetry = start(retryFailed, third, INVOCATION_2, RETRY_1, "retry-start-3");
        assertThat(reducer.reduce(duplicateRetry).findings())
                .anySatisfy(finding -> assertThat(finding.kind()).isEqualTo("duplicate"));
    }

    @Test
    void oneInvocationHasOneOutcomeAndASecondStartCannotOverlapAnActiveReview() {
        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity first = identity(base, TASK, INVOCATION_1);
        Transcript running = start(base, first, null, null, "overlap-first");
        Transcript overlap = start(running, identity(base, TASK, INVOCATION_2), null, null, "overlap-second");
        assertThat(reducer.reduce(overlap).clean()).isFalse();

        Transcript terminal = fail(running, first,
                ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE, "only-outcome-fail");
        Transcript secondOutcome = defer(terminal, first, "second-outcome-defer");
        assertThat(reducer.reduce(secondOutcome).clean()).isFalse();
    }

    @Test
    void onlyFailedOrInterruptedReviewsCanRetryAndDeferredReviewsCannot() {
        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity first = identity(base, TASK, INVOCATION_1);
        Transcript failed = fail(start(base, first, null, null, "failed-retry-start"), first,
                ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE, "failed-retry-outcome");
        CandidateReviewIdentity retry = identity(base, TASK, INVOCATION_2);
        DelegationReducer.Result allowed = reducer.reduce(start(failed, retry, INVOCATION_1,
                RETRY_1, "failed-retry-allowed"));
        assertThat(allowed.clean()).as("findings: %s", allowed.findings()).isTrue();
        assertThat(state(allowed, TASK).review().status()).isEqualTo(ReviewStatus.RUNNING);

        Transcript deferred = defer(start(base, first, null, null, "deferred-retry-start"), first,
                "deferred-retry-outcome");
        DelegationReducer.Result forbidden = reducer.reduce(start(deferred, retry, INVOCATION_1,
                RETRY_2, "deferred-retry-forbidden"));
        assertThat(forbidden.clean()).isFalse();
        assertThat(state(reducer.reduce(deferred), TASK).review().status()).isEqualTo(ReviewStatus.DEFERRED);
    }

    @Test
    void manualVerdictsMustCloseRunningInvocationAndMustOmitClosedInvocationId() {
        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity identity = identity(base, TASK, INVOCATION_1);
        Transcript running = start(base, identity, null, null, "manual-running-start");
        assertThat(reducer.reduce(accept(running, identity, null, "manual-running-missing-id")).clean()).isFalse();
        DelegationReducer.Result bound = reducer.reduce(accept(running, identity, INVOCATION_1,
                "manual-running-bound"));
        assertThat(bound.clean()).as("findings: %s", bound.findings()).isTrue();
        assertThat(state(bound, TASK).review().status()).isEqualTo(ReviewStatus.ACCEPTED);

        Transcript failed = fail(running, identity,
                ReviewFailureCode.REVIEW_FAILURE_CODE_INFRASTRUCTURE, "manual-failed-outcome");
        DelegationReducer.Result omittedAfterFailure = reducer.reduce(accept(failed, identity, null,
                "manual-after-failure"));
        assertThat(omittedAfterFailure.clean()).as("findings: %s", omittedAfterFailure.findings()).isTrue();
        assertThat(reducer.reduce(accept(failed, identity, INVOCATION_1,
                "manual-after-failure-id-forbidden")).clean()).isFalse();

        Transcript deferred = defer(running, identity, "manual-deferred-outcome");
        assertThat(reducer.reduce(requestRevision(deferred, identity, null,
                "manual-after-deferred")).clean()).isTrue();
        assertThat(reducer.reduce(requestRevision(deferred, identity, INVOCATION_1,
                "manual-after-deferred-id-forbidden")).clean()).isFalse();

        // Old transcripts with no explicit review events retain their manual-review path.
        DelegationReducer.Result legacy = reducer.reduce(accept(base, identity, null, "legacy-manual-accept"));
        assertThat(legacy.clean()).as("findings: %s", legacy.findings()).isTrue();
        assertThat(state(legacy, TASK).review().status()).isEqualTo(ReviewStatus.ACCEPTED);
    }

    @Test
    void runningVerdictMustArriveWithinItsRecordedReviewWindow() {
        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity identity = identity(base, TASK, INVOCATION_1);
        Transcript running = start(base, identity, null, null, "window-start");
        Transcript bound = accept(running, identity, INVOCATION_1, "window-verdict");
        assertThat(reducer.reduce(bound).clean()).isTrue();
        assertThat(reducer.reduce(withLastFrameTime(bound,
                Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() - 1).build())).clean())
                .isFalse();
        assertThat(reducer.reduce(withLastFrameTime(bound,
                Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() + 300).build())).clean())
                .isFalse();
        Transcript deferred = defer(running, identity, "window-deferred");
        assertThat(reducer.reduce(withLastFrameTime(deferred,
                Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() + 300).build())).clean())
                .isFalse();

        Transcript timedOut = fail(running, identity,
                ReviewFailureCode.REVIEW_FAILURE_CODE_DEADLINE, "window-timeout");
        timedOut = withLastFrameTime(timedOut,
                Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() + 300).build());
        assertThat(reducer.reduce(timedOut).clean()).isTrue();
        Transcript manual = accept(timedOut, identity, null, "window-manual");
        manual = withLastFrameTime(manual,
                Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() + 301).build());
        assertThat(reducer.reduce(manual).clean()).isTrue();
    }

    private static Transcript withLastFrameTime(Transcript transcript, Timestamp time) {
        int last = transcript.getEntriesCount() - 1;
        TranscriptEntry entry = transcript.getEntries(last);
        return transcript.toBuilder().setEntries(last, entry.toBuilder().setCoordinatorFrame(
                entry.getCoordinatorFrame().toBuilder().setSentAt(time))).build();
    }

    @Test
    void cancellationAndReassignmentSupersedeTheRunningReview() {
        Transcript base = candidateTranscript(TASK, WORKER);
        CandidateReviewIdentity identity = identity(base, TASK, INVOCATION_1);
        Transcript running = start(base, identity, null, null, "superseded-start");
        DelegationReducer.Result cancelled = reducer.reduce(cancel(running, TASK, 1, "superseded-cancel"));
        assertThat(cancelled.clean()).as("findings: %s", cancelled.findings()).isTrue();
        assertThat(state(cancelled, TASK).review().status()).isEqualTo(ReviewStatus.SUPERSEDED);

        var taskSpec = spec("compile");
        Transcript reassigned = cancelledTranscript(running, taskSpec);
        DelegationReducer.Result reoffered = reducer.reduce(reassigned);
        assertThat(reoffered.clean()).as("findings: %s", reoffered.findings()).isTrue();
        assertThat(state(reoffered, TASK).holder()).isEqualTo(SECOND_WORKER);
        assertThat(state(reoffered, TASK).review().status()).isEqualTo(ReviewStatus.SUPERSEDED);
        assertThat(reducer.reduce(fail(cancel(running, TASK, 1, "late-cancel"), identity,
                ReviewFailureCode.REVIEW_FAILURE_CODE_DEADLINE, "late-outcome")).clean()).isFalse();
    }

    private static Transcript cancelledTranscript(Transcript running, ai.protomolt.proto.delegation.v1.TaskSpec taskSpec) {
        Transcript startAndCancel = cancel(running, TASK, 1, "reassign-cancel-frame");
        var hello = ai.protomolt.proto.delegation.v1.DelegateRequest.newBuilder()
                .setFrameId(uuid("second-review-hello")).setSeq(1).setSentAt(REVIEW_AT)
                .setHello(ai.protomolt.proto.delegation.v1.WorkerHello.newBuilder()
                        .setWorkerId(SECOND_WORKER).setProtocolVersion(1).setProvider("sol")
                        .addCapabilities(ai.protomolt.proto.delegation.v1.WorkerCapability.newBuilder()
                                .setName("java-build"))).build();
        TranscriptEntry helloEntry = TranscriptEntry.newBuilder().setLane(Lane.LANE_WORKER)
                .setWorkerId(SECOND_WORKER).setWorkerFrame(hello).build();
        DelegateResponse admission = DelegateResponse.newBuilder().setFrameId(uuid("second-review-admission"))
                .setSeq(1).setSentAt(REVIEW_AT)
                .setAdmission(ai.protomolt.proto.delegation.v1.AdmissionDecision.newBuilder()
                        .setAdmitted(true).setSessionId(uuid("second-review-session"))).build();
        TranscriptEntry admissionEntry = TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR)
                .setWorkerId(SECOND_WORKER).setCoordinatorFrame(admission).build();
        Transcript withNewSession = startAndCancel.toBuilder()
                .addEntries(helloEntry).addEntries(admissionEntry).build();
        return appendReassignment(withNewSession, taskSpec);
    }

    private static Transcript appendReassignment(Transcript transcript,
            ai.protomolt.proto.delegation.v1.TaskSpec taskSpec) {
        DelegateResponse offer = DelegateResponse.newBuilder().setFrameId(uuid("reassignment-offer"))
                .setTaskId(TASK).setSeq(1).setSentAt(REVIEW_AT)
                .setOffer(TaskOffer.newBuilder().setAttempt(2).setSpec(taskSpec)
                        .setLeaseDuration(com.google.protobuf.Duration.newBuilder().setSeconds(300))
                        .setExpiresAt(Timestamp.newBuilder().setSeconds(REVIEW_AT.getSeconds() + 300)))
                .build();
        TranscriptEntry offerEntry = TranscriptEntry.newBuilder().setLane(Lane.LANE_COORDINATOR)
                .setWorkerId(SECOND_WORKER).setCoordinatorFrame(offer).build();
        var workerAccept = ai.protomolt.proto.delegation.v1.DelegateRequest.newBuilder()
                .setFrameId(uuid("reassignment-accept")).setTaskId(TASK).setSeq(1).setSentAt(REVIEW_AT)
                .setAccept(ai.protomolt.proto.delegation.v1.TaskAccept.newBuilder().setAttempt(2)).build();
        TranscriptEntry acceptEntry = TranscriptEntry.newBuilder().setLane(Lane.LANE_WORKER)
                .setWorkerId(SECOND_WORKER).setWorkerFrame(workerAccept).build();
        return transcript.toBuilder().addEntries(offerEntry).addEntries(acceptEntry).build();
    }
}

package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.CheckEvidence;
import ai.pipestream.proto.delegation.v1.CheckVerdict;
import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.DelegateRequest;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.Transcript;
import ai.pipestream.proto.delegation.v1.TranscriptEntry;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.delegation.DelegationFixtures.SECOND_WORKER;
import static ai.pipestream.proto.delegation.DelegationFixtures.TASK;
import static ai.pipestream.proto.delegation.DelegationFixtures.WORKER;
import static ai.pipestream.proto.delegation.DelegationFixtures.commit;
import static ai.pipestream.proto.delegation.DelegationFixtures.evidence;
import static ai.pipestream.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integrity scenarios of the offline delegation reducer: idempotency, sequencing,
 * staleness, evidence completeness, and regression rejection. The reducer reports;
 * it never repairs.
 */
class DelegationReducerIntegrityTest {

    private final DelegationReducer reducer = new DelegationReducer();

    private static DelegationFixtures.TranscriptBuilder leasedAttempt() {
        return new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile", "tests"))
                .accept(TASK, WORKER, 1);
    }

    /** A redelivered frame with identical bytes replays silently. */
    @Test
    void duplicateFramesWithIdenticalPayloadsAreIdempotent() {
        DelegationFixtures.TranscriptBuilder builder = leasedAttempt()
                .progress(TASK, WORKER, 1, 1, "half way");
        TranscriptEntry delivered = builder.lastEntry();
        builder.append(delivered)
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .accepted(TASK, WORKER, 1, "proven");
        assertThat(reducer.reduce(builder.build()).findings()).isEmpty();
    }

    /** The same frame id carrying changed bytes is a conflicting duplicate. */
    @Test
    void aDuplicateFrameIdWithAChangedPayloadIsRejected() {
        DelegationFixtures.TranscriptBuilder builder = leasedAttempt()
                .progress(TASK, WORKER, 1, 1, "half way");
        TranscriptEntry delivered = builder.lastEntry();
        DelegateRequest altered = delivered.getWorkerFrame().toBuilder()
                .setProgress(delivered.getWorkerFrame().getProgress().toBuilder()
                        .setMessage("a different story under the same id"))
                .build();
        builder.append(delivered.toBuilder().setWorkerFrame(altered).build())
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .accepted(TASK, WORKER, 1, "proven");
        DelegationReducer.Result result = reducer.reduce(builder.build());
        assertThat(result.findings()).hasSize(1);
        assertThat(result.findings().get(0).kind()).isEqualTo("duplicate");
        assertThat(result.findings().get(0).error()).contains("conflicting duplicate");
    }

    /** A missing sequence inside one scope is a finding, not a silent skip. */
    @Test
    void aSequenceGapIsReported() {
        DelegationFixtures.TranscriptBuilder builder = leasedAttempt()
                .progress(TASK, WORKER, 1, 1, "one")
                .progress(TASK, WORKER, 1, 2, "two");
        TranscriptEntry gapped = builder.lastEntry().toBuilder()
                .setWorkerFrame(builder.lastEntry().getWorkerFrame().toBuilder()
                        .setFrameId(DelegationFixtures.uuid("gapped-frame"))
                        .setSeq(builder.lastEntry().getWorkerFrame().getSeq() + 3))
                .build();
        builder.append(gapped)
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .accepted(TASK, WORKER, 1, "proven");
        DelegationReducer.Result result = reducer.reduce(builder.build());
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("sequence");
            assertThat(f.error()).contains("skips expected seq");
        });
    }

    /** A completion candidate must prove every required check of the offer's spec. */
    @Test
    void missingRequiredCheckEvidenceIsRejected() {
        CompletionCandidate thin = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("compile only")
                .addEvidence(evidence("compile"))
                .addCommits(commit("thin"))
                .build();
        Transcript transcript = leasedAttempt()
                .candidateWith(TASK, WORKER, 1, thin)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("evidence");
            assertThat(f.error()).contains("tests");
        });
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.LEASED);
    }

    /** Invalid evidence cannot open a candidate that the coordinator then accepts. */
    @Test
    void invalidEvidenceCannotDriveTheTaskToAccepted() {
        CompletionCandidate thin = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("compile only")
                .addEvidence(evidence("compile"))
                .addCommits(commit("thin-accepted"))
                .build();
        Transcript transcript = leasedAttempt()
                .candidateWith(TASK, WORKER, 1, thin)
                .accepted(TASK, WORKER, 1, "accepted without all evidence")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).extracting(DelegationReducer.Finding::kind)
                .contains("evidence", "revision");
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.LEASED);
    }

    /** A delayed completion from an older lease cannot cross an attempt boundary. */
    @Test
    void staleCompletionCannotBeInterpretedAsTheCurrentAttempt() {
        TaskSpec checks = spec("compile", "tests");
        Transcript transcript = leasedAttempt()
                .expire(TASK, WORKER, 1)
                .offer(TASK, WORKER, 2, checks)
                .accept(TASK, WORKER, 2)
                .candidateForAttempt(TASK, WORKER, 1, 1, checks)
                .accepted(TASK, WORKER, 1, "must not accept the stale candidate")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("lease");
            assertThat(f.error()).contains("stale attempt 1")
                    .contains("current attempt is 2");
        });
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.LEASED);
        assertThat(result.tasks().get(TASK).attempt()).isEqualTo(2);
    }

    /** Evidence for a check the spec never required is a finding. */
    @Test
    void evidenceForAnUnrequiredCheckIsRejected() {
        CompletionCandidate extra = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("with a bonus check")
                .addEvidence(evidence("compile"))
                .addEvidence(evidence("tests"))
                .addEvidence(evidence("lint"))
                .addCommits(commit("extra"))
                .build();
        Transcript transcript = leasedAttempt()
                .candidateWith(TASK, WORKER, 1, extra)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("evidence");
            assertThat(f.error()).contains("lint");
        });
    }

    /** A candidate reporting a failed check is not acceptable evidence. */
    @Test
    void aCandidateWithFailedCheckEvidenceIsRejected() {
        CheckEvidence failed = CheckEvidence.newBuilder()
                .setCheckName("tests")
                .setVerdict(CheckVerdict.CHECK_VERDICT_FAILED)
                .setRanAt(Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setDetail("3 failures")
                .build();
        CompletionCandidate candidate = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("tests red")
                .addEvidence(evidence("compile"))
                .addEvidence(failed)
                .addCommits(commit("red"))
                .build();
        Transcript transcript = leasedAttempt()
                .candidateWith(TASK, WORKER, 1, candidate)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("evidence");
            assertThat(f.error()).contains("FAILED");
        });
    }

    /** Frames from a worker that does not hold the lease are stale. */
    @Test
    void aStaleWorkerCannotTouchTheLease() {
        Transcript transcript = leasedAttempt()
                .hello(SECOND_WORKER).admit(SECOND_WORKER)
                .heartbeat(TASK, SECOND_WORKER, 1)
                .progress(TASK, SECOND_WORKER, 1, 1, "intruding")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).hasSize(2)
                .allSatisfy(f -> {
                    assertThat(f.kind()).isEqualTo("lease");
                    assertThat(f.error()).contains(WORKER);
                });
    }

    /** A candidate that is not the awaited revision is stale and unreviewable. */
    @Test
    void aStaleRevisionIsRejected() {
        Transcript transcript = leasedAttempt()
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .revisionRequested(TASK, WORKER, 1, "redo the tests evidence")
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("revision");
            assertThat(f.error()).contains("awaits revision 2");
        });
    }

    /** A checkpoint never advances backwards. */
    @Test
    void aCheckpointRegressionIsRejected() {
        Transcript transcript = leasedAttempt()
                .checkpoint(TASK, WORKER, 1, 1, "tok-a")
                .checkpoint(TASK, WORKER, 1, 2, "tok-b")
                .checkpoint(TASK, WORKER, 1, 1, "tok-c")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("checkpoint");
            assertThat(f.error()).contains("regresses");
        });
    }

    /** A rejected checkpoint cannot replace the valid token used by resume checks. */
    @Test
    void aRegressedCheckpointCannotOverwriteTheResumeToken() {
        CheckpointReference poisoned = DelegationFixtures.TranscriptBuilder
                .resumeFrom(1, 1, "tok-evil");
        Transcript transcript = leasedAttempt()
                .checkpoint(TASK, WORKER, 1, 1, "tok-good")
                .checkpoint(TASK, WORKER, 1, 1, "tok-evil")
                .expire(TASK, WORKER, 1)
                .offerResuming(TASK, WORKER, 2, spec("compile", "tests"), poisoned)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("checkpoint");
            assertThat(f.error()).contains("token does not match");
        });
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.EXPIRED);
    }

    /** Idempotency includes the stream identity, not only the wire-frame bytes. */
    @Test
    void aFrameIdReusedOnAnotherWorkerStreamConflicts() {
        DelegationFixtures.TranscriptBuilder builder = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .hello(SECOND_WORKER).admit(SECOND_WORKER)
                .offer(TASK, WORKER, 1, spec("compile"));
        TranscriptEntry offer = builder.lastEntry();
        builder.append(offer.toBuilder().setWorkerId(SECOND_WORKER).build());
        DelegationReducer.Result result = reducer.reduce(builder.build());
        assertThat(result.findings()).singleElement().satisfies(f -> {
            assertThat(f.kind()).isEqualTo("duplicate");
            assertThat(f.error()).contains("conflicting duplicate");
        });
    }

    /** A resume pointer must name a checkpoint the task actually recorded. */
    @Test
    void resumingFromAnUnknownCheckpointIsRejected() {
        CheckpointReference unknown = DelegationFixtures.TranscriptBuilder
                .resumeFrom(1, 7, "tok-never-recorded");
        Transcript transcript = leasedAttempt()
                .checkpoint(TASK, WORKER, 1, 1, "tok-a")
                .expire(TASK, WORKER, 1)
                .offerResuming(TASK, WORKER, 2, spec("compile", "tests"), unknown)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("checkpoint");
            assertThat(f.error()).contains("never recorded");
        });
    }

    /** A renewal must move the declared expiry forward. */
    @Test
    void aRenewalThatDoesNotAdvanceTheExpiryIsRejected() {
        DelegationFixtures.TranscriptBuilder builder = leasedAttempt()
                .renew(TASK, WORKER, 1);
        TranscriptEntry renewal = builder.lastEntry();
        // Replay the same renewal under a new frame id: the expiry no longer advances.
        builder.append(renewal.toBuilder()
                .setCoordinatorFrame(renewal.getCoordinatorFrame().toBuilder()
                        .setFrameId(DelegationFixtures.uuid("renewal-replay")))
                .build());
        DelegationReducer.Result result = reducer.reduce(builder.build());
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("lease");
            assertThat(f.error()).contains("does not advance");
        });
    }

    /** A task frame from a worker that was never admitted is a session violation. */
    @Test
    void taskFramesBeforeAdmissionAreRejected() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("session");
            assertThat(f.error()).contains("never admitted");
        });
    }

    /** Progress is monotonic inside an attempt. */
    @Test
    void aProgressRegressionIsRejected() {
        Transcript transcript = leasedAttempt()
                .progress(TASK, WORKER, 1, 1, "one")
                .progress(TASK, WORKER, 1, 3, "three")
                .progress(TASK, WORKER, 1, 2, "two")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("progress");
            assertThat(f.error()).contains("does not advance");
        });
        assertThat(result.findings()).anySatisfy(f -> {
            assertThat(f.kind()).isEqualTo("progress");
            assertThat(f.error()).contains("skips");
        });
    }
}

package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.CheckpointReference;
import ai.pipestream.proto.delegation.v1.Transcript;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.delegation.DelegationFixtures.SECOND_WORKER;
import static ai.pipestream.proto.delegation.DelegationFixtures.TASK;
import static ai.pipestream.proto.delegation.DelegationFixtures.WORKER;
import static ai.pipestream.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lifecycle scenarios of the offline delegation reducer: the honest paths a task can
 * take from offer to acceptance, and the terminal boundaries around them.
 */
class DelegationReducerLifecycleTest {

    private final DelegationReducer reducer = new DelegationReducer();

    @Test
    void happyPathCompletesClean() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile", "tests"))
                .accept(TASK, WORKER, 1)
                .heartbeat(TASK, WORKER, 1)
                .renew(TASK, WORKER, 1)
                .progress(TASK, WORKER, 1, 1, "scaffolding in place")
                .checkpoint(TASK, WORKER, 1, 1, "tok-a")
                .progress(TASK, WORKER, 1, 2, "implementation done")
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .accepted(TASK, WORKER, 1, "both checks proven")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).isEmpty();
        DelegationReducer.TaskState state = result.tasks().get(TASK);
        assertThat(state.phase()).isEqualTo(DelegationReducer.Phase.ACCEPTED);
        assertThat(state.attempt()).isEqualTo(1);
        assertThat(state.holder()).isEqualTo(WORKER);
        assertThat(state.candidateRevision()).isEqualTo(1);
    }

    @Test
    void rejectThenReofferCompletes() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .reject(TASK, WORKER, 1, "lease too short for this task")
                .offer(TASK, WORKER, 2, spec("compile"))
                .accept(TASK, WORKER, 2)
                .candidate(TASK, WORKER, 1, spec("compile"))
                .accepted(TASK, WORKER, 1, "proven")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).isEmpty();
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.ACCEPTED);
        assertThat(result.tasks().get(TASK).attempt()).isEqualTo(2);
    }

    @Test
    void reconnectResumesFromARecordedCheckpoint() {
        CheckpointReference resume = DelegationFixtures.TranscriptBuilder
                .resumeFrom(1, 1, "tok-a");
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .accept(TASK, WORKER, 1)
                .checkpoint(TASK, WORKER, 1, 1, "tok-a")
                // The stream drops; the worker reconnects and is re-admitted.
                .hello(WORKER).admit(WORKER)
                .expire(TASK, WORKER, 1)
                .offerResuming(TASK, WORKER, 2, spec("compile"), resume)
                .accept(TASK, WORKER, 2)
                .progress(TASK, WORKER, 2, 1, "resumed from checkpoint tok-a")
                .candidate(TASK, WORKER, 1, spec("compile"))
                .accepted(TASK, WORKER, 1, "proven after resume")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).isEmpty();
        DelegationReducer.TaskState state = result.tasks().get(TASK);
        assertThat(state.phase()).isEqualTo(DelegationReducer.Phase.ACCEPTED);
        assertThat(state.attempt()).isEqualTo(2);
    }

    @Test
    void leaseExpiryAllowsReassignmentToAnotherWorker() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .accept(TASK, WORKER, 1)
                .expire(TASK, WORKER, 1)
                .hello(SECOND_WORKER).admit(SECOND_WORKER)
                .offer(TASK, SECOND_WORKER, 2, spec("compile"))
                .accept(TASK, SECOND_WORKER, 2)
                .candidate(TASK, SECOND_WORKER, 1, spec("compile"))
                .accepted(TASK, SECOND_WORKER, 1, "proven by the second worker")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).isEmpty();
        DelegationReducer.TaskState state = result.tasks().get(TASK);
        assertThat(state.phase()).isEqualTo(DelegationReducer.Phase.ACCEPTED);
        assertThat(state.holder()).isEqualTo(SECOND_WORKER);
    }

    @Test
    void aCandidateRacingACancellationLoses() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .accept(TASK, WORKER, 1)
                .cancel(TASK, WORKER, 1, "priorities changed")
                // The worker's candidate was already in flight when the cancel landed.
                .candidate(TASK, WORKER, 1, spec("compile"))
                .cancelledNotice(TASK, WORKER, 1)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).hasSize(1);
        assertThat(result.findings().get(0).kind()).isEqualTo("terminal");
        assertThat(result.findings().get(0).error()).contains("races the cancellation");
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.CANCELLED);
    }

    @Test
    void revisionRequestFollowedByCorrectedEvidenceAccepts() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile", "tests"))
                .accept(TASK, WORKER, 1)
                .candidate(TASK, WORKER, 1, spec("compile", "tests"))
                .revisionRequested(TASK, WORKER, 1,
                        "the tests evidence does not convince; rerun with coverage")
                .candidate(TASK, WORKER, 2, spec("compile", "tests"))
                .accepted(TASK, WORKER, 2, "revision 2 proves both checks")
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).isEmpty();
        assertThat(result.tasks().get(TASK).candidateRevision()).isEqualTo(2);
    }

    @Test
    void blockedEndsTheAttemptAndACorrectedSpecReoffers() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .accept(TASK, WORKER, 1)
                .blocked(TASK, WORKER, 1, "scope excludes the module that must change")
                .offer(TASK, WORKER, 2, spec("compile"))
                .accept(TASK, WORKER, 2)
                .candidate(TASK, WORKER, 1, spec("compile"))
                .accepted(TASK, WORKER, 1, "proven")
                .build();
        assertThat(reducer.reduce(transcript).findings()).isEmpty();
    }

    @Test
    void terminalImmutabilityRejectsFramesAfterAcceptance() {
        DelegationFixtures.TranscriptBuilder builder =
                new DelegationFixtures.TranscriptBuilder()
                        .hello(WORKER).admit(WORKER)
                        .offer(TASK, WORKER, 1, spec("compile"))
                        .accept(TASK, WORKER, 1)
                        .candidate(TASK, WORKER, 1, spec("compile"))
                        .accepted(TASK, WORKER, 1, "proven")
                        .heartbeat(TASK, WORKER, 1)
                        .offer(TASK, WORKER, 2, spec("compile"));
        DelegationReducer.Result result = reducer.reduce(builder.build());
        assertThat(result.findings())
                .allSatisfy(f -> assertThat(f.kind()).isEqualTo("terminal"));
        assertThat(result.findings()).hasSize(2);
        assertThat(result.tasks().get(TASK).phase())
                .isEqualTo(DelegationReducer.Phase.ACCEPTED);
    }

    @Test
    void aCancelledNoticeWithoutACancellationIsAFinding() {
        Transcript transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, spec("compile"))
                .accept(TASK, WORKER, 1)
                .cancelledNotice(TASK, WORKER, 1)
                .build();
        DelegationReducer.Result result = reducer.reduce(transcript);
        assertThat(result.findings()).hasSize(1);
        assertThat(result.findings().get(0).kind()).isEqualTo("transition");
        assertThat(result.findings().get(0).error()).contains("no cancellation");
    }
}

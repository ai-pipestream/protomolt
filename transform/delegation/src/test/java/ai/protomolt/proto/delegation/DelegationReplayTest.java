package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskAccept;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regressions for the extracted replay driver and projection state ownership. Replaying a
 * recorded transcript into a fresh {@link DelegationCoordinatorState} must rebuild exactly
 * the projection the live coordinator held — sessions, tasks, sequences, frame index,
 * cursors — and an unclean stored transcript must fail before any state is mutated.
 */
class DelegationReplayTest {

    private static final Instant START = Instant.parse("2026-10-01T12:00:00Z");

    @Test
    void replayRebuildsTheLiveProjectionFromTheTranscriptAlone() throws Exception {
        Transcript transcript;
        InProcessDelegationCoordinator.WorkerResumption liveResumption;
        try (var coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(),
                Clock.fixed(START, ZoneOffset.UTC), new InMemoryTranscriptRepository(),
                Duration.ofSeconds(30), false, false);
             var bridge = new DelegationBridge(coordinator)) {
            register(bridge);
            bridge.offer(WORKER, TASK, DelegationFixtures.spec("tests"),
                    Duration.ofMinutes(5), null);
            bridge.accept(WORKER, TASK, 1);
            bridge.progress(WORKER, TASK, 1, "half done");
            bridge.checkpoint(WORKER, TASK, 1, "tok-1", "midway", null);
            long cursorBeforeCandidate = coordinator.eventsAfter(null, 0).size();
            bridge.submitCandidate(WORKER, TASK, CompletionCandidate.newBuilder()
                    .setAttempt(1).setRevision(1)
                    .setSummary("implemented and verified")
                    .addEvidence(DelegationFixtures.evidence("tests"))
                    .addCommits(DelegationFixtures.commit("replay-restore"))
                    .build());
            // The candidate and its review start commit synchronously; the manual
            // reviewer's deferral lands asynchronously from the review runtime task.
            assertThat(coordinator.waitForEvent(TASK, cursorBeforeCandidate + 2,
                    Duration.ofSeconds(30))).isPresent();
            transcript = coordinator.transcript();
            liveResumption = coordinator.workerResumption(WORKER);
        }

        DelegationCoordinatorState restored = new DelegationCoordinatorState();
        DelegationReplay.restore(new DelegationReducer(), restored, transcript);

        assertThat(restored.entries).containsExactlyElementsOf(transcript.getEntriesList());
        assertThat(restored.events).hasSize(transcript.getEntriesCount());
        for (int i = 0; i < restored.events.size(); i++) {
            assertThat(restored.events.get(i).cursor()).isEqualTo(i + 1L);
            assertThat(restored.events.get(i).entry()).isEqualTo(transcript.getEntries(i));
        }
        assertThat(restored.cursor).isEqualTo(transcript.getEntriesCount());

        var session = restored.sessions.get(WORKER);
        assertThat(session).isNotNull();
        assertThat(session.admitted).isTrue();
        assertThat(session.connected).isFalse();

        var task = restored.tasks.get(TASK);
        assertThat(task).isNotNull();
        assertThat(task.workerId).isEqualTo(WORKER);
        assertThat(task.attempt).isEqualTo(1);
        assertThat(task.phase).isEqualTo(DelegationReducer.Phase.CANDIDATE);
        assertThat(task.candidate.getRevision()).isEqualTo(1);
        assertThat(task.offer).isNotNull();

        long workerFrameCount = transcript.getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_WORKER)
                .count();
        assertThat(restored.workerFrames).hasSize((int) workerFrameCount);

        // The rebuilt session continues each coordinator scope where the transcript left it.
        long coordinatorScopeFrames = transcript.getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_COORDINATOR)
                .filter(entry -> entry.getCoordinatorFrame().getTaskId().equals(TASK))
                .filter(entry -> DelegationReplay.attemptOf(entry.getCoordinatorFrame()) == 1)
                .count();
        assertThat(session.peekNextCoordinatorSeq(TASK, 1))
                .isEqualTo(coordinatorScopeFrames + 1);

        // The resumption rebuilt from the replayed entries equals the live coordinator's.
        assertThat(DelegationReplay.workerResumption(restored.entries, WORKER))
                .isEqualTo(liveResumption);
    }

    @Test
    void replayAcrossAttemptsPreservesTheOpenAttemptAndLeaseGeneration() {
        Transcript transcript;
        try (var coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(),
                Clock.fixed(START, ZoneOffset.UTC), new InMemoryTranscriptRepository(),
                Duration.ofSeconds(30), false, false);
             var bridge = new DelegationBridge(coordinator)) {
            register(bridge);
            bridge.offer(WORKER, TASK, DelegationFixtures.spec("tests"),
                    Duration.ofMinutes(5), null);
            bridge.accept(WORKER, TASK, 1);
            bridge.coordinator().cancel(TASK, "superseded by a second attempt");
            bridge.offer(WORKER, TASK, DelegationFixtures.spec("tests"),
                    Duration.ofMinutes(5), null);
            bridge.accept(WORKER, TASK, 2);
            transcript = coordinator.transcript();
        }

        DelegationCoordinatorState restored = new DelegationCoordinatorState();
        DelegationReplay.restore(new DelegationReducer(), restored, transcript);

        var task = restored.tasks.get(TASK);
        assertThat(task).isNotNull();
        assertThat(task.attempt).isEqualTo(2);
        assertThat(task.phase).isEqualTo(DelegationReducer.Phase.LEASED);
        assertThat(task.offer.getAttempt()).isEqualTo(2);
        // The generation counter only ever advances: offer, cancellation, re-offer.
        assertThat(task.leaseGeneration).isEqualTo(3);
        assertThat(task.attemptTerminal()).isFalse();
    }

    @Test
    void restoreRejectsAnUncleanTranscriptBeforeMutatingState() {
        Transcript honest;
        try (var coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(),
                Clock.fixed(START, ZoneOffset.UTC), new InMemoryTranscriptRepository(),
                Duration.ofSeconds(30), false, false);
             var bridge = new DelegationBridge(coordinator)) {
            register(bridge);
            bridge.offer(WORKER, TASK, DelegationFixtures.spec("tests"),
                    Duration.ofMinutes(5), null);
            honest = coordinator.transcript();
        }
        // Well-formed enough to carry (validation passes) but lifecycle-impossible:
        // an accept for a task that was never offered.
        TranscriptEntry acceptWithoutOffer = TranscriptEntry.newBuilder()
                .setWorkerId(WORKER)
                .setLane(Lane.LANE_WORKER)
                .setWorkerFrame(DelegateRequest.newBuilder()
                        .setFrameId(DelegationFixtures.uuid("accept-without-offer"))
                        .setTaskId(DelegationFixtures.uuid("never-offered"))
                        .setSeq(1)
                        .setSentAt(Timestamp.newBuilder().setSeconds(1_700_000_100L))
                        .setAccept(TaskAccept.newBuilder().setAttempt(1)))
                .build();
        Transcript dirty = Transcript.newBuilder()
                .addAllEntries(honest.getEntriesList())
                .addEntries(acceptWithoutOffer)
                .build();

        DelegationCoordinatorState fresh = new DelegationCoordinatorState();
        assertThatThrownBy(() -> DelegationReplay.restore(new DelegationReducer(), fresh, dirty))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stored transcript is invalid");
        assertThat(fresh.entries).isEmpty();
        assertThat(fresh.events).isEmpty();
        assertThat(fresh.sessions).isEmpty();
        assertThat(fresh.tasks).isEmpty();
        assertThat(fresh.workerFrames).isEmpty();
        assertThat(fresh.cursor).isZero();
    }

    private static void register(DelegationBridge bridge) {
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER).setProtocolVersion(1).setProvider("fixture")
                .addCapabilities(WorkerCapability.newBuilder().setName("structured-delegation"))
                .build());
    }
}

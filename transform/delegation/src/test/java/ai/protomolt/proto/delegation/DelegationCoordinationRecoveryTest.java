package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static ai.protomolt.proto.delegation.DelegationFixtures.TASK;
import static ai.protomolt.proto.delegation.DelegationFixtures.WORKER;
import static ai.protomolt.proto.delegation.DelegationFixtures.spec;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises lease and checkpoint recovery through the live in-process coordinator. */
class DelegationCoordinationRecoveryTest {
    private static final Instant START = Instant.parse("2026-09-25T12:00:00Z");

    @Test
    void anAcceptedAttemptWithoutFollowupExpiresUnacceptedAndCanBeReoffered() {
        MutableClock clock = new MutableClock(START);
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                     AdmissionPolicy.allowAll(), CandidateReviewer.manual(), clock);
             DelegationBridge bridge = new DelegationBridge(coordinator)) {
            register(bridge);
            bridge.offer(WORKER, TASK, spec("tests"), Duration.ofSeconds(30), null);
            bridge.accept(WORKER, TASK, 1);

            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);
            assertThat(coordinator.expireLeases(START.plusSeconds(31))).isEqualTo(1);
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.EXPIRED);
            assertThat(coordinator.transcript().getEntriesList())
                    .noneSatisfy(entry -> assertThat(entry.getCoordinatorFrame()
                            .hasAccepted()).isTrue());

            bridge.offer(WORKER, TASK, spec("tests"), Duration.ofSeconds(30), null);
            assertThat(coordinator.state().tasks().get(TASK).attempt()).isEqualTo(2);
            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(DelegationReducer.Phase.OFFERED);
        }
    }

    @Test
    void reofferCarriesTheRecordedCheckpointFromThePreviousAttempt() {
        MutableClock clock = new MutableClock(START);
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                     AdmissionPolicy.allowAll(), CandidateReviewer.manual(), clock);
             DelegationBridge bridge = new DelegationBridge(coordinator)) {
            register(bridge);
            bridge.offer(WORKER, TASK, spec("tests"), Duration.ofSeconds(30), null);
            bridge.accept(WORKER, TASK, 1);
            assertThat(bridge.checkpoint(WORKER, TASK, 1, "resume-token-1",
                    "saved after the first check", null)).isEqualTo(1);
            coordinator.expireLeases(START.plusSeconds(31));

            CheckpointReference resume = CheckpointReference.newBuilder()
                    .setAttempt(1).setCheckpointSeq(1).setResumeToken("resume-token-1")
                    .build();
            var reoffer = bridge.offer(WORKER, TASK, spec("tests"),
                    Duration.ofSeconds(30), resume);

            assertThat(reoffer.getAttempt()).isEqualTo(2);
            assertThat(reoffer.hasResumeFrom()).isTrue();
            assertThat(reoffer.getResumeFrom().getAttempt()).isEqualTo(1);
            assertThat(reoffer.getResumeFrom().getCheckpointSeq()).isEqualTo(1);
            assertThat(reoffer.getResumeFrom().getResumeToken()).isEqualTo("resume-token-1");
        }
    }

    @Test
    void aNewAttemptMustSatisfyItsChangedScopeAndAnAcceptedTaskCannotBeReoffered() {
        var firstScope = spec("old-tests").toBuilder()
                .clearAllowedScope().addAllowedScope("old/**").build();
        var changedScope = spec("new-tests").toBuilder()
                .clearAllowedScope().addAllowedScope("new/**").build();
        var transcript = new DelegationFixtures.TranscriptBuilder()
                .hello(WORKER).admit(WORKER)
                .offer(TASK, WORKER, 1, firstScope)
                .accept(TASK, WORKER, 1)
                .progress(TASK, WORKER, 1, 1, "worked only in old scope")
                .expire(TASK, WORKER, 1)
                .offer(TASK, WORKER, 2, changedScope)
                .accept(TASK, WORKER, 2)
                // Even a well-formed completion for attempt 1 is stale after the scope change.
                .candidateForAttempt(TASK, WORKER, 1, 1, firstScope)
                .candidateForAttempt(TASK, WORKER, 2, 1, changedScope)
                .accepted(TASK, WORKER, 1, "new-scope check passed")
                .offer(TASK, WORKER, 3, firstScope)
                .build();

        DelegationReducer.Result result = new DelegationReducer().reduce(transcript);
        assertThat(result.findings()).anySatisfy(finding -> {
            assertThat(finding.kind()).isEqualTo("lease");
            assertThat(finding.error()).contains("attempt 1").contains("current attempt is 2");
        });
        assertThat(result.findings()).anySatisfy(finding ->
                assertThat(finding.kind()).isEqualTo("terminal"));
        assertThat(result.tasks().get(TASK).phase()).isEqualTo(DelegationReducer.Phase.ACCEPTED);
        assertThat(result.tasks().get(TASK).attempt()).isEqualTo(2);

        TranscriptEntry accepted = transcript.getEntriesList().stream()
                .filter(entry -> entry.getCoordinatorFrame().hasAccepted())
                .findFirst().orElseThrow();
        assertThat(accepted.getCoordinatorFrame().getAccepted().getAttempt()).isEqualTo(2);
    }

    private static void register(DelegationBridge bridge) {
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER).setProtocolVersion(1).setProvider("fixture")
                .addCapabilities(WorkerCapability.newBuilder().setName("structured-delegation"))
                .build());
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}

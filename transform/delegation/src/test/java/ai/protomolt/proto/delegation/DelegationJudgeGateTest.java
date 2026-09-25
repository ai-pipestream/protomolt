package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.Lane;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Any;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The real delegation path admits only contract-valid results to candidate review. */
class DelegationJudgeGateTest {

    private static final String WORKER = "judge-gate-worker";
    private static final String CHECK = "unit-tests";
    private static final Duration LEASE = Duration.ofMinutes(1);

    private InProcessDelegationCoordinator coordinator;
    private DelegationBridge bridge;
    private String taskId;

    @AfterEach
    void closeRuntime() {
        if (bridge != null) {
            bridge.close();
        }
        if (coordinator != null) {
            coordinator.close();
        }
    }

    @Test
    void missingResultNeverReachesReviewer() {
        assertRejectedBeforeReview(null, "carries no result");
    }

    @Test
    void wrongResultTypeNeverReachesReviewer() {
        assertRejectedBeforeReview(DeliverableFixtures.resultOfAnotherType(),
                DeliverableFixtures.TYPE_NAME);
    }

    @Test
    void fieldInvalidResultNeverReachesReviewer() {
        assertRejectedBeforeReview(DeliverableFixtures.result("short", 3),
                DeliverableFixtures.HEADLINE_RULE);
    }

    @Test
    void celInvalidResultNeverReachesReviewer() {
        assertRejectedBeforeReview(
                DeliverableFixtures.result("a headline long enough", 0),
                DeliverableFixtures.CEL_MESSAGE);
    }

    @Test
    void validResultReachesReviewerButCanStillBeRevised() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch reviewed = new CountDownLatch(1);
        start(context -> {
            calls.incrementAndGet();
            reviewed.countDown();
            return CandidateReviewer.ReviewDecision.revise(
                    "the report needs a human-confirmed finding", List.of(CHECK));
        });

        bridge.submitCandidate(WORKER, taskId,
                candidate(DeliverableFixtures.result("a headline long enough", 4)));

        assertThat(reviewed.await(10, TimeUnit.SECONDS)).isTrue();
        awaitRevisionRequested();
        assertThat(calls).hasValue(1);
        assertThat(coordinator.transcript().getEntriesList().stream()
                .filter(entry -> entry.getLane() == Lane.LANE_WORKER)
                .filter(entry -> entry.getWorkerFrame().hasCompletion()))
                .hasSize(1);
        assertThat(coordinator.state().clean()).isTrue();
        assertThat(coordinator.state().tasks().get(taskId).phase())
                .isEqualTo(DelegationReducer.Phase.LEASED);
    }

    private void assertRejectedBeforeReview(Any result, String expectedReason) {
        AtomicInteger calls = new AtomicInteger();
        start(context -> {
            calls.incrementAndGet();
            return CandidateReviewer.ReviewDecision.accept("reviewed");
        });
        Transcript before = coordinator.transcript();

        assertThatThrownBy(() -> bridge.submitCandidate(WORKER, taskId, candidate(result)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("contract")
                .hasMessageContaining(expectedReason);

        assertThat(coordinator.transcript()).isEqualTo(before);
        assertThat(coordinator.state().clean()).isTrue();
        assertThat(coordinator.state().tasks().get(taskId).phase())
                .isEqualTo(DelegationReducer.Phase.LEASED);
        assertThat(calls).hasValue(0);
    }

    private void start(CandidateReviewer reviewer) {
        coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), reviewer);
        bridge = new DelegationBridge(coordinator);
        assertThat(bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("fixture")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build()).admitted()).isTrue();
        taskId = UUID.randomUUID().toString();
        TaskSpec spec = DelegationFixtures.spec(CHECK).toBuilder()
                .setContract(DeliverableFixtures.contract())
                .build();
        bridge.offer(WORKER, taskId, spec, LEASE, null);
        bridge.accept(WORKER, taskId, 1);
        assertThat(coordinator.state().tasks().get(taskId).phase())
                .isEqualTo(DelegationReducer.Phase.LEASED);
    }

    private void awaitRevisionRequested() throws InterruptedException {
        long cursor = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("revision decision did not arrive");
            }
            InProcessDelegationCoordinator.Event event = coordinator.waitForEvent(
                    taskId, cursor, Duration.ofNanos(remaining)).orElseThrow();
            if (event.entry().getLane() == Lane.LANE_COORDINATOR
                    && event.entry().getCoordinatorFrame().hasRevisionRequested()) {
                return;
            }
            cursor = event.cursor();
        }
    }

    private static CompletionCandidate candidate(Any result) {
        CompletionCandidate.Builder candidate = CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(1)
                .setSummary("a report with recorded checks")
                .addEvidence(DelegationFixtures.evidence(CHECK))
                .addCommits(DelegationFixtures.commit("judge-gate"));
        if (result != null) {
            candidate.setResult(result);
        }
        return candidate.build();
    }
}

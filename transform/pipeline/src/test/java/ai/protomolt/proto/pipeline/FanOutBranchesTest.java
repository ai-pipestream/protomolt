package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fan-out branch runner's failure policies, evidence order, and deadline cancellation,
 * exercised directly so each policy outcome is pinned independently of a full pipeline.
 */
class FanOutBranchesTest {

    private static final Descriptor TICKET = PipelineFixtures.type(PipelineFixtures.TICKET);

    @Test
    void failFastAbandonsUnstartedBranchesAndRethrowsTheFailure() {
        AtomicInteger started = new AtomicInteger();
        PipelineExecutionException first = PipelineExecutor.failure("process",
                FailureKind.GRPC, Status.Code.UNAVAILABLE, "gRPC UNAVAILABLE: down", null);
        List<BranchOutcome> evidence = new ArrayList<>();

        assertThatThrownBy(() -> FanOutBranches.run("process", items("a", "b", "c"),
                fanOut(1, BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST),
                input -> {
                    started.incrementAndGet();
                    throw first;
                }, deadlineIn(10_000), evidence))
                .isSameAs(first);

        // One permit: whichever branch takes it first fails, and the branches waiting on the
        // permit see the abandon flag and never run.
        assertThat(started).hasValue(1);
        assertThat(evidence).extracting(BranchOutcome::id)
                .containsExactly("process#0", "process#1", "process#2");
        assertThat(evidence).extracting(BranchOutcome::index).containsExactly(0, 1, 2);
        assertThat(evidence).noneMatch(BranchOutcome::succeeded);
        assertThat(evidence).filteredOn(outcome -> outcome.kind() == FailureKind.GRPC)
                .singleElement()
                .satisfies(outcome -> assertThat(outcome.error())
                        .isEqualTo("gRPC UNAVAILABLE: down"));
        assertThat(evidence).filteredOn(outcome -> outcome.kind() == FailureKind.FAN_OUT)
                .hasSize(2)
                .allSatisfy(outcome -> assertThat(outcome.error())
                        .isEqualTo("abandoned after an earlier branch failure"));
    }

    @Test
    void continueReturnsSurvivorsInItemOrderAndRecordsAnUnclassifiedFailure()
            throws Exception {
        List<BranchOutcome> evidence = new ArrayList<>();

        List<DynamicMessage> outputs = FanOutBranches.run("process", items("a", "bad", "c"),
                fanOut(3, BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE),
                input -> {
                    if (title(input).equals("bad")) {
                        throw new IllegalStateException("worker crashed");
                    }
                    return ticket("done:" + title(input));
                }, deadlineIn(10_000), evidence);

        assertThat(outputs).extracting(FanOutBranchesTest::title)
                .containsExactly("done:a", "done:c");
        assertThat(evidence).containsExactly(
                new BranchOutcome("process#0", 0, true, null, ""),
                new BranchOutcome("process#1", 1, false, FailureKind.FAN_OUT,
                        "branch failed: worker crashed"),
                new BranchOutcome("process#2", 2, true, null, ""));
    }

    @Test
    void failureEvidenceIsBoundedToFourThousandCharacters() throws Exception {
        String longMessage = "x".repeat(5_000);
        List<BranchOutcome> evidence = new ArrayList<>();

        FanOutBranches.run("process", items("a"),
                fanOut(1, BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE),
                input -> {
                    throw PipelineExecutor.failure("process", FailureKind.STRUCTURED,
                            longMessage, null);
                }, deadlineIn(10_000), evidence);

        assertThat(evidence).singleElement().satisfies(outcome -> {
            assertThat(outcome.kind()).isEqualTo(FailureKind.STRUCTURED);
            assertThat(outcome.error()).isEqualTo(longMessage.substring(0, 4_000));
        });
    }

    @Test
    void deadlineExhaustionCancelsRunningBranchesBeforeRecordingEvidence()
            throws Exception {
        CountDownLatch interrupted = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        List<BranchOutcome> evidence = new ArrayList<>();

        assertThatThrownBy(() -> FanOutBranches.run("process", items("slow"),
                fanOut(1, BranchFailurePolicy.BRANCH_FAILURE_POLICY_CONTINUE),
                input -> {
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                        finished.set(true);
                    } catch (InterruptedException e) {
                        interrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                    return input;
                }, deadlineIn(200), evidence))
                .isInstanceOfSatisfying(PipelineExecutionException.class, failure -> {
                    assertThat(failure.kind()).isEqualTo(FailureKind.DEADLINE);
                    assertThat(failure.step()).isEqualTo("process");
                    assertThat(failure.getMessage())
                            .isEqualTo("pipeline step deadline exhausted during fan-out");
                });

        assertThat(interrupted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(finished).isFalse();
        assertThat(evidence).isEmpty();
    }

    private static FanOutSpec fanOut(int concurrency, BranchFailurePolicy policy) {
        return FanOutSpec.newBuilder().setItems("items").setMaxItems(10)
                .setMaxConcurrency(concurrency).setFailurePolicy(policy)
                .setCollectType(PipelineFixtures.TICKET_BOX).setCollectInto("tickets")
                .build();
    }

    private static long deadlineIn(long millis) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    }

    private static List<DynamicMessage> items(String... titles) {
        List<DynamicMessage> items = new ArrayList<>();
        for (String title : titles) {
            items.add(ticket(title));
        }
        return items;
    }

    private static DynamicMessage ticket(String title) {
        return DynamicMessage.newBuilder(TICKET)
                .setField(TICKET.findFieldByName("title"), title).build();
    }

    private static String title(DynamicMessage message) {
        return (String) message.getField(TICKET.findFieldByName("title"));
    }
}

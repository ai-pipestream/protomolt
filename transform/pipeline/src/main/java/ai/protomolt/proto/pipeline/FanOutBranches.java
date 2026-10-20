package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.workflow.v1.BranchFailurePolicy;
import ai.protomolt.proto.grpc.workflow.v1.FanOutSpec;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs one fan-out step's branches on virtual threads under the step's concurrency cap,
 * failure policy, and deadline.
 *
 * <p>State ownership is split on purpose. Everything that exists only for one fan-out (the
 * result slots, the per-branch failures, the abandon flag, the permits, and the executor) is
 * created and discarded inside {@link #run}. The calling step keeps everything else: it has
 * already mapped, projected, validated, and capped the items, it owns the deadline, it binds
 * the returned outputs into its own scope, and it owns the run's branch evidence list, which
 * this class only appends to in stable branch-index order once every branch has settled.</p>
 *
 * <p>A FAIL_FAST branch failure stops branches that have not yet started and rethrows the
 * lowest-index failure after evidence is recorded; a CONTINUE failure is recorded and the
 * surviving outputs are returned in item order. Deadline exhaustion cancels every pending
 * branch and fails the step with {@link FailureKind#DEADLINE} before any evidence is
 * recorded.</p>
 */
final class FanOutBranches {

    /** Longest branch failure message kept as evidence. */
    private static final int MAX_EVIDENCE_LENGTH = 4_000;

    /** One branch's work: the delivered item in, the branch output out. */
    @FunctionalInterface
    interface BranchCall {
        /**
         * Runs one branch.
         *
         * @param input the delivered item for this branch
         * @return the branch output collected into the step's result
         * @throws PipelineExecutionException when the branch fails with a classified failure
         */
        DynamicMessage run(DynamicMessage input) throws PipelineExecutionException;
    }

    private FanOutBranches() {
    }

    /**
     * Runs one branch per item and returns the successful outputs in item order.
     *
     * @param step the fan-out step's name, used for failures and evidence ids
     * @param items the delivered branch inputs, already bounded by the caller
     * @param fanOut the step's concurrency cap and failure policy
     * @param call the work for one branch
     * @param deadlineNanos the step deadline on the {@link System#nanoTime()} clock
     * @param branchEvidence the run's evidence list, appended to once per item
     * @return the successful branch outputs, in item order
     * @throws PipelineExecutionException on a FAIL_FAST branch failure, deadline exhaustion,
     *         interruption, or a branch worker failure
     */
    static List<DynamicMessage> run(String step, List<DynamicMessage> items,
                                    FanOutSpec fanOut, BranchCall call, long deadlineNanos,
                                    List<BranchOutcome> branchEvidence)
            throws PipelineExecutionException {
        DynamicMessage[] slots = new DynamicMessage[items.size()];
        PipelineExecutionException[] failures = new PipelineExecutionException[items.size()];
        Semaphore permits = new Semaphore(fanOut.getMaxConcurrency());
        AtomicBoolean abandoned = new AtomicBoolean();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<?>> futures = new ArrayList<>(items.size());
            for (int i = 0; i < items.size(); i++) {
                int index = i;
                futures.add(executor.submit(() -> {
                    if (abandoned.get()) {
                        return;
                    }
                    boolean acquired = false;
                    try {
                        permits.acquire();
                        acquired = true;
                        if (!abandoned.get()) {
                            slots[index] = call.run(items.get(index));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failures[index] = PipelineExecutor.failure(step, FailureKind.FAN_OUT,
                                "branch interrupted", e);
                    } catch (PipelineExecutionException e) {
                        failures[index] = e;
                        if (fanOut.getFailurePolicy()
                                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST) {
                            abandoned.set(true);
                        }
                    } catch (RuntimeException e) {
                        failures[index] = PipelineExecutor.failure(step, FailureKind.FAN_OUT,
                                "branch failed: " + e.getMessage(), e);
                        if (fanOut.getFailurePolicy()
                                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST) {
                            abandoned.set(true);
                        }
                    } finally {
                        if (acquired) {
                            permits.release();
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                try {
                    long remaining = deadlineNanos - System.nanoTime();
                    if (remaining <= 0) {
                        throw new TimeoutException("pipeline step deadline exhausted");
                    }
                    future.get(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw PipelineExecutor.failure(step, FailureKind.FAN_OUT,
                            "interrupted awaiting branches", e);
                } catch (ExecutionException e) {
                    throw PipelineExecutor.failure(step, FailureKind.FAN_OUT,
                            "branch worker failed: " + e.getCause().getMessage(), e.getCause());
                } catch (TimeoutException e) {
                    futures.forEach(pending -> pending.cancel(true));
                    throw PipelineExecutor.failure(step, FailureKind.DEADLINE,
                            "pipeline step deadline exhausted during fan-out", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }
        List<DynamicMessage> outputs = new ArrayList<>();
        PipelineExecutionException firstFailure = null;
        for (int i = 0; i < slots.length; i++) {
            DynamicMessage slot = slots[i];
            if (slot != null) {
                outputs.add(slot);
                branchEvidence.add(new BranchOutcome(step + "#" + i, i, true,
                        null, ""));
            } else {
                PipelineExecutionException branchFailure = failures[i];
                if (firstFailure == null && branchFailure != null) {
                    firstFailure = branchFailure;
                }
                branchEvidence.add(new BranchOutcome(step + "#" + i, i, false,
                        branchFailure == null ? FailureKind.FAN_OUT : branchFailure.kind(),
                        branchFailure == null
                                ? "abandoned after an earlier branch failure"
                                : bounded(branchFailure.getMessage())));
            }
        }
        if (firstFailure != null && fanOut.getFailurePolicy()
                == BranchFailurePolicy.BRANCH_FAILURE_POLICY_FAIL_FAST) {
            throw firstFailure;
        }
        return outputs;
    }

    private static String bounded(String value) {
        if (value == null) {
            return "branch failed";
        }
        return value.length() <= MAX_EVIDENCE_LENGTH
                ? value : value.substring(0, MAX_EVIDENCE_LENGTH);
    }
}

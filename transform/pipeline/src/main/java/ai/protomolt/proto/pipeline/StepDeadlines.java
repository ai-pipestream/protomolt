package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import com.google.protobuf.Duration;

/**
 * Deadline arithmetic for one pipeline run on the {@link System#nanoTime()} clock. Every
 * method is a pure function of its arguments and the clock: the run owns its pipeline
 * deadline, each step derives its own step deadline from it, and nothing here keeps state
 * between calls.
 */
final class StepDeadlines {

    private StepDeadlines() {
    }

    /**
     * The pipeline deadline measured from now.
     *
     * @throws PipelineExecutionException {@link FailureKind#PREFLIGHT} when the declared
     *         deadline is not positive or does not fit the clock
     */
    static long pipelineDeadline(Pipeline pipeline) throws PipelineExecutionException {
        try {
            return Math.addExact(System.nanoTime(),
                    durationNanos(pipeline.getDeadline(), "pipeline deadline"));
        } catch (ArithmeticException e) {
            throw PipelineExecutor.failure("", FailureKind.PREFLIGHT,
                    "pipeline deadline is too large", e);
        }
    }

    /**
     * The step deadline: the pipeline deadline when the step declares none, otherwise the
     * earlier of the pipeline deadline and the step's declared deadline measured from now.
     * A declared deadline whose end does not fit the clock ends after the pipeline deadline,
     * so the pipeline deadline bounds the step.
     *
     * @throws PipelineExecutionException {@link FailureKind#PREFLIGHT} when the declared step
     *         deadline is not positive or is too large to express in nanoseconds
     */
    static long stepDeadline(PipelineStep step, long pipelineDeadlineNanos)
            throws PipelineExecutionException {
        if (step.getDeadline().getSeconds() == 0 && step.getDeadline().getNanos() == 0) {
            return pipelineDeadlineNanos;
        }
        long declared = durationNanos(step.getDeadline(), "step deadline");
        try {
            return Math.min(pipelineDeadlineNanos,
                    Math.addExact(System.nanoTime(), declared));
        } catch (ArithmeticException e) {
            return pipelineDeadlineNanos;
        }
    }

    /**
     * The whole milliseconds left before {@code deadlineNanos}, rounded up so a positive
     * remainder never becomes a zero transport deadline.
     *
     * @throws PipelineExecutionException {@link FailureKind#DEADLINE} when no time is left
     */
    static long remainingMillis(String step, long deadlineNanos)
            throws PipelineExecutionException {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw PipelineExecutor.failure(step, FailureKind.DEADLINE,
                    "pipeline step deadline exhausted", null);
        }
        return Math.floorDiv(remainingNanos - 1L, 1_000_000L) + 1L;
    }

    /**
     * Refuses to start a step once the pipeline deadline has passed.
     *
     * @throws PipelineExecutionException {@link FailureKind#DEADLINE} when the deadline has
     *         passed
     */
    static void requireTime(String step, long deadlineNanos)
            throws PipelineExecutionException {
        if (System.nanoTime() >= deadlineNanos) {
            throw PipelineExecutor.failure(step, FailureKind.DEADLINE,
                    "pipeline deadline exhausted before the step ran", null);
        }
    }

    private static long durationNanos(Duration duration, String field)
            throws PipelineExecutionException {
        try {
            long nanos = Math.addExact(
                    Math.multiplyExact(duration.getSeconds(), 1_000_000_000L),
                    duration.getNanos());
            if (nanos <= 0) {
                throw PipelineExecutor.failure("", FailureKind.PREFLIGHT,
                        field + " must be positive", null);
            }
            return nanos;
        } catch (ArithmeticException e) {
            throw PipelineExecutor.failure("", FailureKind.PREFLIGHT, field + " is too large", e);
        }
    }
}

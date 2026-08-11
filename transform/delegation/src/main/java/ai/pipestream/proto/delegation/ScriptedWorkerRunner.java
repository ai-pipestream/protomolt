package ai.pipestream.proto.delegation;

import ai.pipestream.proto.delegation.v1.CompletionCandidate;
import ai.pipestream.proto.delegation.v1.WorkerHello;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;

/** Deterministic worker runner for in-process protocol and integration tests. */
public final class ScriptedWorkerRunner implements WorkerRunner {

    private final WorkerHello hello;
    private final Deque<Step> steps;

    /** Creates a runner that consumes the supplied steps in order. */
    public ScriptedWorkerRunner(WorkerHello hello, Collection<? extends Step> steps) {
        this.hello = Objects.requireNonNull(hello, "hello");
        this.steps = new ArrayDeque<>(steps);
        DelegationValidation.validate(hello);
    }

    @Override
    public WorkerHello hello() {
        return hello;
    }

    @Override
    public synchronized CompletionCandidate run(WorkerTask task, WorkerEvents events)
            throws Exception {
        Step step = steps.pollFirst();
        if (step == null) {
            throw new IllegalStateException("the scripted worker has no remaining step");
        }
        return step.run(task, events);
    }

    /** One deterministic worker invocation. */
    @FunctionalInterface
    public interface Step {
        /** Produces one candidate or throws the scripted failure. */
        CompletionCandidate run(WorkerTask task, WorkerEvents events) throws Exception;
    }
}

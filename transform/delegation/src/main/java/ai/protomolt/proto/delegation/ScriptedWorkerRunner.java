package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.contract.DelegationValidation;

import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.WorkerHello;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/** Deterministic worker runner for in-process protocol and integration tests. */
public final class ScriptedWorkerRunner implements WorkerRunner {

    private final ReentrantLock lock = new ReentrantLock();
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
    public CompletionCandidate run(WorkerTask task, WorkerEvents events)
            throws Exception {
        lock.lock();
        try {
            Step step = steps.pollFirst();
            if (step == null) {
                throw new IllegalStateException("the scripted worker has no remaining step");
            }
            return step.run(task, events);
        } finally {
            lock.unlock();
        }
    }

    /** One deterministic worker invocation. */
    @FunctionalInterface
    public interface Step {
        /** Produces one candidate or throws the scripted failure. */
        CompletionCandidate run(WorkerTask task, WorkerEvents events) throws Exception;
    }
}

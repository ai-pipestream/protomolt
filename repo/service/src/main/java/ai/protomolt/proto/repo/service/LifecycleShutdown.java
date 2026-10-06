package ai.protomolt.proto.repo.service;

import java.time.Duration;
import java.util.List;

/** A failed join retains borrowed resources so shutdown can be retried safely. */
final class LifecycleShutdown {
    private LifecycleShutdown() {}

    static void stopBeforeRelease(List<Thread> workers, Duration timeout, Runnable release) {
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("Shutdown timeout must be positive");
        long budget = timeout.toNanos();
        long started = System.nanoTime();
        workers.forEach(Thread::interrupt);
        for (Thread worker : workers) {
            long remaining = budget - (System.nanoTime() - started);
            try {
                if (worker.isAlive() && (remaining <= 0 || !worker.join(Duration.ofNanos(remaining))))
                    throw new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.LIFECYCLE_WORKER,
                            "Repository lifecycle worker did not terminate: " + worker.getName()
                            + "; resources retained, retry close after the worker stops");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Repository shutdown interrupted; resources retained", interrupted);
            }
        }
        release.run();
    }
}

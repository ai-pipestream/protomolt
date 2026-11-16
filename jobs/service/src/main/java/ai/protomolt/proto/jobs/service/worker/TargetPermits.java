package ai.protomolt.proto.jobs.service.worker;

import io.grpc.Status;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Bounds how many calls one worker has in flight to a single target. Every target gets the
 * same number of permits; a caller holds one for as long as its call needs the target.
 */
final class TargetPermits {

    private final int maxConcurrentPerTarget;
    private final ConcurrentHashMap<String, Semaphore> permits = new ConcurrentHashMap<>();

    TargetPermits(int maxConcurrentPerTarget) {
        if (maxConcurrentPerTarget <= 0) {
            throw new IllegalArgumentException("maxConcurrentPerTarget must be positive; got "
                    + maxConcurrentPerTarget);
        }
        this.maxConcurrentPerTarget = maxConcurrentPerTarget;
    }

    /** Waits for a permit to {@code target} for as long as it takes. */
    Semaphore acquire(String target) {
        Semaphore semaphore = semaphoreFor(target);
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted acquiring the per-target permit "
                    + "for " + target, e);
        }
        return semaphore;
    }

    /**
     * Waits for a permit to {@code target} for at most {@code waitMillis}.
     *
     * @throws io.grpc.StatusRuntimeException DEADLINE_EXCEEDED when no permit came free in
     *         time, CANCELLED when the wait was interrupted; the call is not made in either
     *         case
     */
    Semaphore acquireWithin(String target, long waitMillis) {
        Semaphore semaphore = semaphoreFor(target);
        try {
            if (semaphore.tryAcquire(waitMillis, TimeUnit.MILLISECONDS)) {
                return semaphore;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("interrupted waiting for a call slot to " + target)
                    .withCause(e)
                    .asRuntimeException();
        }
        throw Status.DEADLINE_EXCEEDED
                .withDescription("no call slot to " + target + " came free within " + waitMillis
                        + " ms (" + maxConcurrentPerTarget + " concurrent call(s) allowed)")
                .asRuntimeException();
    }

    private Semaphore semaphoreFor(String target) {
        return permits.computeIfAbsent(target, key -> new Semaphore(maxConcurrentPerTarget));
    }
}

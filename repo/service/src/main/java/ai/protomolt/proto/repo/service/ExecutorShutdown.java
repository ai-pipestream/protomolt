package ai.protomolt.proto.repo.service;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** A timed-out executor remains owned and must be joined again before releasing its dependencies. */
final class ExecutorShutdown {
    private ExecutorShutdown() {}

    static void stop(ExecutorService executor, Duration timeout) throws InterruptedException {
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("Shutdown timeout must be positive");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS))
                    throw new IllegalStateException("Repository transport executor did not terminate; retry close after handlers stop");
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }
}

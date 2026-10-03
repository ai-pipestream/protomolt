package ai.protomolt.proto.repo.container.ledger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/** Bounded ordered work. A failed or interrupted caller still waits for every started worker. */
final class DocumentPartWorkers {
    private DocumentPartWorkers() {}

    static <T> List<T> run(int count, int parallelism, Semaphore shared, Runnable check, BiFunction<Integer, Runnable, T> action) {
        var next = new AtomicInteger();
        var failure = new AtomicReference<Throwable>();
        Object[] results = new Object[count];
        Runnable active = () -> {
            if (failure.get() != null) throw new CancellationException("Another document part worker failed");
            check.run();
        };
        boolean interrupted = false;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (int worker = 0; worker < Math.min(count, parallelism); worker++) {
                    futures.add(executor.submit(() -> {
                        try {
                            while (failure.get() == null) {
                                check.run();
                                if (!shared.tryAcquire(50, TimeUnit.MILLISECONDS)) continue;
                                try {
                                    if (failure.get() != null) return;
                                    check.run();
                                    int ordinal = next.getAndIncrement();
                                    if (ordinal >= count) return;
                                    results[ordinal] = action.apply(ordinal, active);
                                } finally { shared.release(); }
                            }
                        } catch (Throwable cause) { record(failure, cause); }
                    }));
                }
            } catch (Throwable cause) { record(failure, cause); }
            for (var future : futures) {
                boolean done = false;
                while (!done) {
                    try { future.get(); done = true; }
                    catch (InterruptedException cause) {
                        interrupted = true;
                        record(failure, new CancellationException("Document staging caller interrupted"));
                    } catch (java.util.concurrent.ExecutionException cause) {
                        record(failure, cause.getCause()); done = true;
                    }
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
        var cause = failure.get();
        if (cause instanceof Error error) throw error;
        if (cause instanceof RuntimeException runtime) throw runtime;
        if (cause != null) throw new IllegalStateException("Document part worker failed", cause);
        @SuppressWarnings("unchecked") List<T> ordered = (List<T>) (List<?>) List.copyOf(Arrays.asList(results));
        return ordered;
    }

    private static void record(AtomicReference<Throwable> failure, Throwable cause) {
        if (!failure.compareAndSet(null, cause)) {
            var first = failure.get();
            if (first != cause) first.addSuppressed(cause);
        }
    }
}

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
        return run(count, parallelism, shared, check, action, failure -> {});
    }

    /** Notify the operation's other tasks of failure, including caller interruption, before draining workers. */
    static <T> List<T> run(int count, int parallelism, Semaphore shared, Runnable check,
            BiFunction<Integer, Runnable, T> action, java.util.function.Consumer<Throwable> failed) {
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
                        } catch (Throwable cause) { record(failure, cause, failed); }
                    }));
                }
            } catch (Throwable cause) { record(failure, cause, failed); }
            for (var future : futures) {
                boolean done = false;
                while (!done) {
                    try { future.get(); done = true; }
                    catch (InterruptedException cause) {
                        interrupted = true;
                        record(failure, new CancellationException("Document staging caller interrupted"), failed);
                    } catch (java.util.concurrent.ExecutionException cause) {
                        record(failure, cause.getCause(), failed); done = true;
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

    private static void record(AtomicReference<Throwable> failure, Throwable cause, java.util.function.Consumer<Throwable> failed) {
        // Callback failures cannot bypass worker draining or hide the original error.
        try { failed.accept(cause); }
        catch (Throwable notificationFailure) { if (notificationFailure != cause) cause.addSuppressed(notificationFailure); }
        if (!failure.compareAndSet(null, cause)) {
            var first = failure.get();
            if (first != cause) first.addSuppressed(cause);
        }
    }
}

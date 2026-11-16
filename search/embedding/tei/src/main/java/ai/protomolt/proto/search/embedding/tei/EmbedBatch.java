package ai.protomolt.proto.search.embedding.tei;

import ai.protomolt.proto.search.embedding.EmbeddingInFlightLimit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Supervises the virtual-thread workers of one {@link TeiEmbeddingProvider#embedAll} call.
 *
 * <p>The caller admits each worker with an {@link EmbeddingInFlightLimit.Permit} and hands
 * the permit to {@link #startWorker}; the worker closes it when its RPC finishes. Results are
 * stored by input index. The first worker failure cancels the batch and interrupts the other
 * running workers, and {@link #awaitWorkersExitedUninterruptibly} lets the caller wait until
 * no worker outlives the call.
 */
final class EmbedBatch {
    private static final String INTERRUPTED_MESSAGE = "Interrupted while embedding a batch";

    private final ReentrantLock batchLock = new ReentrantLock();
    private final Condition doneOrFailed = batchLock.newCondition();
    private final Condition workersExited = batchLock.newCondition();
    private final Set<Thread> runningWorkers = new HashSet<>();
    private final float[][] vectors;
    private final AtomicInteger providerWorkers;
    private volatile boolean cancelled;
    private Throwable firstFailure;
    private int activeWorkers;

    EmbedBatch(int size, AtomicInteger providerWorkers) {
        this.vectors = new float[size][];
        this.providerWorkers = providerWorkers;
    }

    boolean isCancelled() {
        return cancelled;
    }

    boolean startWorker(
            int index,
            Supplier<float[]> call,
            EmbeddingInFlightLimit.Permit permit) {
        boolean closePermit = true;
        try {
            batchLock.lock();
            try {
                if (cancelled) {
                    return false;
                }
                Thread worker;
                try {
                    worker = Thread.ofVirtual().unstarted(() -> runWorker(index, call, permit));
                } catch (Throwable startFailure) {
                    recordFailureLocked(startFailure);
                    throw startFailure;
                }
                activeWorkers++;
                providerWorkers.incrementAndGet();
                runningWorkers.add(worker);
                try {
                    worker.start();
                    closePermit = false;
                } catch (Throwable startFailure) {
                    runningWorkers.remove(worker);
                    providerWorkers.decrementAndGet();
                    activeWorkers--;
                    recordFailureLocked(startFailure);
                    if (activeWorkers == 0) {
                        workersExited.signalAll();
                    }
                    throw startFailure;
                }
                return true;
            } finally {
                batchLock.unlock();
            }
        } finally {
            if (closePermit) {
                permit.close();
            }
        }
    }

    private void runWorker(
            int index,
            Supplier<float[]> call,
            EmbeddingInFlightLimit.Permit permit) {
        Throwable problem = null;
        try {
            vectors[index] = call.get();
        } catch (Throwable failure) {
            problem = failure;
        } finally {
            if (problem != null) {
                batchLock.lock();
                try {
                    recordFailureLocked(problem);
                } finally {
                    batchLock.unlock();
                }
            }
            permit.close();
            batchLock.lock();
            try {
                runningWorkers.remove(Thread.currentThread());
                providerWorkers.decrementAndGet();
                activeWorkers--;
                if (activeWorkers == 0) {
                    doneOrFailed.signalAll();
                    workersExited.signalAll();
                }
            } finally {
                batchLock.unlock();
            }
        }
    }

    private void recordFailureLocked(Throwable failure) {
        if (firstFailure == null) {
            firstFailure = failure;
        }
        cancelled = true;
        Thread current = Thread.currentThread();
        for (Thread worker : runningWorkers) {
            if (worker != current) {
                worker.interrupt();
            }
        }
        doneOrFailed.signalAll();
    }

    void cancelAll() {
        batchLock.lock();
        try {
            cancelled = true;
            for (Thread worker : runningWorkers) {
                worker.interrupt();
            }
            doneOrFailed.signalAll();
        } finally {
            batchLock.unlock();
        }
    }

    List<float[]> awaitResults(int dispatched) {
        batchLock.lock();
        try {
            while (!cancelled && activeWorkers > 0) {
                doneOrFailed.await();
            }
            if (firstFailure != null) {
                throw rethrow(firstFailure);
            }
            if (dispatched != vectors.length) {
                throw new IllegalStateException("Batch embedding stopped after "
                        + dispatched + " of " + vectors.length + " items");
            }
            return new ArrayList<>(Arrays.asList(vectors));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(INTERRUPTED_MESSAGE, e);
        } finally {
            batchLock.unlock();
        }
    }

    void awaitWorkersExitedUninterruptibly() {
        batchLock.lock();
        try {
            while (activeWorkers > 0) {
                workersExited.awaitUninterruptibly();
            }
        } finally {
            batchLock.unlock();
        }
    }

    static RuntimeException rethrow(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Embedding a batch failed", failure);
    }
}

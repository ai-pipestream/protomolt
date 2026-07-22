package ai.pipestream.proto.connector;

import com.google.protobuf.Message;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Bridges a push-style {@link StreamSource} to a synchronous, pull-style consumer: the
 * source fills a bounded queue, the consumer drains it one message at a time with
 * {@link #take}. When the queue fills, the pump pauses the source and then blocks the
 * producer thread until there is room; when the consumer drains the queue below half, the
 * pump resumes the source. Backpressure is real but bounded: a paused source may still
 * deliver what was already in flight, which the queue absorbs.
 *
 * <p>Terminal signals follow the drained queue. After a failure, {@link #take} returns
 * every message that arrived before the failure and then throws {@link SourceException}.
 * After a normal end, {@link #isClosed()} becomes true once the queue empties.</p>
 *
 * <p>One producer thread and one consumer thread at a time.</p>
 */
public final class SourcePump implements StreamSource.Listener, AutoCloseable {

    /** Default queue depth; deep enough to absorb a paused source's in-flight overshoot. */
    public static final int DEFAULT_CAPACITY = 64;

    private static final long SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private final BlockingQueue<Message> queue;
    private final int resumeAt;
    private final Object flowLock = new Object();
    private volatile StreamSource.Handle handle;
    private volatile boolean paused;
    private volatile boolean completed;
    private volatile Throwable failure;
    private volatile boolean closed;

    public SourcePump() {
        this(DEFAULT_CAPACITY);
    }

    public SourcePump(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.resumeAt = Math.max(1, capacity / 2);
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Attaches the handle returned by {@link StreamSource#open}. The source may already be
     * pushing by the time {@code open} returns, so the pump tolerates a brief unattached
     * window: it cannot pause the source until this is called.
     */
    public void attach(StreamSource.Handle handle) {
        this.handle = handle;
        if (queue.remainingCapacity() == 0) {
            pauseSource();
        }
    }

    @Override
    public void onMessage(Message message) {
        // Offer in slices so close() frees a producer blocked on a full queue promptly.
        while (!closed) {
            try {
                if (queue.offer(message, SLICE_NANOS, TimeUnit.NANOSECONDS)) {
                    return;
                }
                pauseSource();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void onComplete() {
        completed = true;
    }

    @Override
    public void onError(Throwable error) {
        failure = error;
    }

    /**
     * Takes the next message, waiting up to {@code timeout}. Returns {@code null} on a
     * quiet interval, and once the stream has ended and drained.
     *
     * @throws SourceException once the stream has failed and every message buffered before
     *         the failure has been returned
     */
    public Message take(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Message next = queue.poll();
            if (next != null) {
                resumeIfDrained();
                return next;
            }
            Throwable failed = failure;
            if (failed != null) {
                throw failed instanceof SourceException se ? se
                        : new SourceException("stream source failed", failed);
            }
            if (completed || closed) {
                return null;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return null;
            }
            try {
                next = queue.poll(Math.min(remaining, SLICE_NANOS), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            if (next != null) {
                resumeIfDrained();
                return next;
            }
        }
    }

    /** True once the stream has ended (completed, failed, or closed) and the queue is drained. */
    public boolean isClosed() {
        return (completed || closed || failure != null) && queue.isEmpty();
    }

    /** True once the source has signalled a normal end. */
    public boolean isCompleted() {
        return completed;
    }

    /** How many messages are buffered right now; for tests and monitoring. */
    public int depth() {
        return queue.size();
    }

    /**
     * Stops the source and frees a producer blocked in {@link #onMessage}; that producer's
     * in-flight message is dropped. Messages already queued remain drainable.
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            StreamSource.Handle active = handle;
            if (active != null) {
                active.close();
            }
        }
    }

    private void pauseSource() {
        synchronized (flowLock) {
            StreamSource.Handle active = handle;
            if (!paused && active != null && !closed) {
                paused = true;
                active.pause();
            }
        }
    }

    private void resumeIfDrained() {
        synchronized (flowLock) {
            StreamSource.Handle active = handle;
            if (paused && active != null && queue.size() <= resumeAt) {
                paused = false;
                active.resume();
            }
        }
    }
}

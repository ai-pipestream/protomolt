package ai.pipestream.proto.grpc.invoke;

import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A long-lived, flow-controlled handle on a server-streaming method, opened with
 * {@link DynamicGrpcCalls#openServerStream}. Where {@link DynamicGrpcCalls#call} collects a
 * capped answer and hangs up, this handle consumes an open-ended stream in batches:
 * messages are requested from the server only as {@link #take} drains them, so the local
 * buffer stays bounded no matter how fast the server produces.
 *
 * <p>The {@code take(max, timeout)} shape is deliberately the contract of a poll-based
 * consumer (a Kafka Connect {@code SourceTask.poll()}, a worker loop): block up to the
 * timeout, return up to {@code max} messages, return an empty list on a quiet interval.
 * A server failure surfaces as a {@link StatusRuntimeException} only after the buffered
 * messages before it have been drained.</p>
 *
 * <p>Thread-safety: one consumer thread at a time may call {@link #take}; {@link #cancel}
 * may be called from any thread.</p>
 */
public final class DynamicGrpcStream implements AutoCloseable {

    private static final long POLL_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private final LinkedBlockingQueue<DynamicMessage> buffer = new LinkedBlockingQueue<>();
    private final AtomicInteger outstanding = new AtomicInteger();
    private volatile ClientCallStreamObserver<DynamicMessage> call;
    private volatile Status terminal;

    DynamicGrpcStream() {
    }

    /** The gRPC-facing observer; wired by {@link DynamicGrpcCalls#openServerStream}. */
    ClientResponseObserver<DynamicMessage, DynamicMessage> observer() {
        return new ClientResponseObserver<>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<DynamicMessage> requestStream) {
                // Manual flow control: the server may send only what take() has asked for.
                requestStream.disableAutoRequestWithInitial(0);
                call = requestStream;
            }

            @Override
            public void onNext(DynamicMessage value) {
                outstanding.decrementAndGet();
                buffer.add(value);
            }

            @Override
            public void onError(Throwable t) {
                terminal = Status.fromThrowable(t);
            }

            @Override
            public void onCompleted() {
                terminal = Status.OK;
            }
        };
    }

    /**
     * Takes up to {@code max} messages, waiting at most {@code timeout} for the first ones to
     * arrive. Returns fewer than {@code max} (possibly none) when the interval is quiet or
     * the stream ends.
     *
     * @throws StatusRuntimeException once the stream has failed and its prior messages have
     *         been drained
     */
    public List<DynamicMessage> take(int max, Duration timeout) {
        if (max <= 0) {
            throw new IllegalArgumentException("max must be positive: " + max);
        }
        request(max);
        List<DynamicMessage> out = new ArrayList<>(Math.min(max, 64));
        long deadline = System.nanoTime() + timeout.toNanos();
        while (out.size() < max) {
            DynamicMessage next = buffer.poll();
            if (next != null) {
                out.add(next);
                continue;
            }
            Status status = terminal;
            if (status != null) {
                // Terminal and drained: report the failure unless messages soften the blow.
                if (!status.isOk() && out.isEmpty()) {
                    throw status.asRuntimeException();
                }
                break;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            try {
                // Wait in short slices: a terminal status set by onError/onCompleted must
                // unblock a waiting take() promptly, and nothing interrupts a queue poll.
                next = buffer.poll(Math.min(remaining, POLL_SLICE_NANOS), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (next != null) {
                out.add(next);
            }
        }
        return out;
    }

    private void request(int max) {
        // Keep enough outstanding demand to fill this take; a benign race can over-request
        // by at most max, which only deepens the buffer by one batch.
        int need = max - buffer.size() - outstanding.get();
        if (need > 0) {
            outstanding.addAndGet(need);
            ClientCallStreamObserver<DynamicMessage> active = call;
            if (active != null) {
                active.request(need);
            }
        }
    }

    /** True once the stream has ended (completed, failed, or cancelled) and is drained. */
    public boolean isClosed() {
        return terminal != null && buffer.isEmpty();
    }

    /** The terminal status, when the stream has ended: OK, an error, or CANCELLED. */
    public Status terminalStatus() {
        return terminal;
    }

    /** Cancels the call; the server sees the cancellation. Idempotent. */
    public void cancel() {
        ClientCallStreamObserver<DynamicMessage> active = call;
        if (active != null && terminal == null) {
            active.cancel("cancelled by the consumer", null);
        }
    }

    @Override
    public void close() {
        cancel();
    }
}

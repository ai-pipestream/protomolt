package ai.pipestream.proto.connector;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcStream;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A server-streaming gRPC method as a {@link StreamSource}. A pump thread pulls batches
 * from the flow-controlled {@link DynamicGrpcStream} handle and pushes them to the
 * listener. Pausing simply stops pulling, and the transport's own flow control stops the
 * server from sending; closing cancels the call, which the server observes.
 */
public final class GrpcStreamSource implements StreamSource<GrpcSourcePlan> {

    private static final int BATCH = 16;
    private static final Duration POLL = Duration.ofMillis(100);

    @Override
    public String name() {
        return "grpc";
    }

    @Override
    public Handle open(GrpcSourcePlan plan, Listener listener) {
        DynamicGrpcStream stream = DynamicGrpcCalls.openServerStream(
                plan.channel(), plan.method(), plan.request(), plan.options(), plan.headers());
        GrpcHandle handle = new GrpcHandle(stream, listener);
        handle.thread.start();
        return handle;
    }

    private static final class GrpcHandle implements Handle {
        private final DynamicGrpcStream stream;
        private final Listener listener;
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final Object pauseLock = new Object();
        private final Thread thread;
        private volatile boolean paused;
        private volatile boolean running = true;

        GrpcHandle(DynamicGrpcStream stream, Listener listener) {
            this.stream = stream;
            this.listener = listener;
            this.thread = new Thread(this::pump, "grpc-stream-source");
            this.thread.setDaemon(true);
        }

        private void pump() {
            try {
                while (running) {
                    awaitIfPaused();
                    if (!running) {
                        break;
                    }
                    List<DynamicMessage> batch;
                    try {
                        batch = stream.take(BATCH, POLL);
                    } catch (RuntimeException e) {
                        // A failure signalled by take(); during close() it is the
                        // cancellation we asked for, not an error to report.
                        if (running) {
                            fireError(e);
                        }
                        return;
                    }
                    for (DynamicMessage message : batch) {
                        if (!running) {
                            return;
                        }
                        listener.onMessage(message);
                    }
                    if (stream.isClosed()) {
                        Status status = stream.terminalStatus();
                        if (status == null || status.isOk()) {
                            fireComplete();
                        } else {
                            fireError(status.asRuntimeException());
                        }
                        return;
                    }
                }
            } finally {
                stream.close();
            }
        }

        private void awaitIfPaused() {
            synchronized (pauseLock) {
                while (paused && running) {
                    try {
                        pauseLock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        @Override
        public void pause() {
            synchronized (pauseLock) {
                paused = true;
            }
        }

        @Override
        public void resume() {
            synchronized (pauseLock) {
                paused = false;
                pauseLock.notifyAll();
            }
        }

        @Override
        public void close() {
            running = false;
            resume();
            stream.cancel();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            fireComplete();
        }

        private void fireComplete() {
            if (terminated.compareAndSet(false, true)) {
                listener.onComplete();
            }
        }

        private void fireError(Throwable error) {
            if (terminated.compareAndSet(false, true)) {
                listener.onError(error);
            }
        }
    }
}

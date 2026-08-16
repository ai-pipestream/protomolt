package ai.pipestream.proto.parse.grparse;

import ai.pipestream.parse.v1.DocumentChunk;
import ai.pipestream.parse.v1.DocumentStreamEvent;
import ai.pipestream.parse.v1.ParseStreamingServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A scriptable gRParse: records every {@code DocumentChunk} it receives and,
 * on half-close, either replays the scripted event list or fails the stream
 * with the configured status. Configure per test via {@link #reset}.
 */
final class FakeGrparse extends ParseStreamingServiceGrpc.ParseStreamingServiceImplBase {

    private volatile List<DocumentStreamEvent> script = List.of();
    private volatile Status failWith;
    private volatile boolean replayAsync;
    private volatile CountDownLatch resumeReplay = new CountDownLatch(1);
    private volatile CountDownLatch upstreamCancelled = new CountDownLatch(1);

    /** Every chunk received, across the current script, in arrival order. */
    final List<DocumentChunk> received = Collections.synchronizedList(new ArrayList<>());

    /** Arms the fake to replay {@code events} after consuming the request. */
    void reset(List<DocumentStreamEvent> events) {
        script = List.copyOf(events);
        failWith = null;
        replayAsync = false;
        resumeReplay = new CountDownLatch(1);
        upstreamCancelled = new CountDownLatch(1);
        received.clear();
    }

    /** Arms the fake to fail the stream with {@code status} on half-close. */
    void resetFailing(Status status) {
        script = List.of();
        failWith = status;
        replayAsync = false;
        resumeReplay = new CountDownLatch(1);
        upstreamCancelled = new CountDownLatch(1);
        received.clear();
    }

    /**
     * Arms the fake to replay {@code events} on a background thread, parking
     * after the first event until {@link #releaseReplay} lets it continue:
     * a deterministic mid-stream window between two events.
     */
    void resetAsyncReplay(List<DocumentStreamEvent> events) {
        script = List.copyOf(events);
        failWith = null;
        replayAsync = true;
        resumeReplay = new CountDownLatch(1);
        upstreamCancelled = new CountDownLatch(1);
        received.clear();
    }

    /** Releases a replay parked after its first event. */
    void releaseReplay() {
        resumeReplay.countDown();
    }

    /**
     * Waits for the adapter to cancel the gRParse request stream, up to
     * {@code timeoutSeconds}.
     *
     * @return whether the cancellation arrived in time
     */
    boolean awaitUpstreamCancelled(long timeoutSeconds) throws InterruptedException {
        return upstreamCancelled.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    @Override
    public StreamObserver<DocumentChunk> streamProcessDocument(
            StreamObserver<DocumentStreamEvent> out) {
        ((ServerCallStreamObserver<?>) out).setOnCancelHandler(upstreamCancelled::countDown);
        return new StreamObserver<>() {
            @Override
            public void onNext(DocumentChunk chunk) {
                received.add(chunk);
            }

            @Override
            public void onError(Throwable t) {
                // The adapter went away; nothing to replay.
            }

            @Override
            public void onCompleted() {
                Status failure = failWith;
                if (failure != null) {
                    out.onError(failure.asRuntimeException());
                    return;
                }
                if (replayAsync) {
                    replayOnBackgroundThread(out);
                    return;
                }
                for (DocumentStreamEvent event : script) {
                    out.onNext(event);
                }
                out.onCompleted();
            }
        };
    }

    /**
     * Replays the script on a daemon thread, parking after the first event
     * until {@link #releaseReplay} fires. Events sent after the caller cancels
     * are dropped by the transport, like any late write.
     */
    private void replayOnBackgroundThread(StreamObserver<DocumentStreamEvent> out) {
        List<DocumentStreamEvent> events = script;
        CountDownLatch resume = resumeReplay;
        Thread replay = new Thread(
                () -> {
                    out.onNext(events.getFirst());
                    try {
                        if (!resume.await(10, TimeUnit.SECONDS)) {
                            return; // The test gave up; leave the rest unsent.
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 1; i < events.size(); i++) {
                        out.onNext(events.get(i));
                    }
                    // No onCompleted: the replay leaves the stream open so
                    // the test observes how the adapter ends it; the channel
                    // teardown cleans up.
                },
                "fake-grparse-replay");
        replay.setDaemon(true);
        replay.start();
    }
}

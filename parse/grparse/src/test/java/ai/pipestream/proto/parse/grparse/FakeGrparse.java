package ai.pipestream.proto.parse.grparse;

import ai.pipestream.parse.v1.DocumentChunk;
import ai.pipestream.parse.v1.DocumentStreamEvent;
import ai.pipestream.parse.v1.ParseStreamingServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A scriptable gRParse: records every {@code DocumentChunk} it receives and,
 * on half-close, either replays the scripted event list or fails the stream
 * with the configured status. Configure per test via {@link #reset}.
 */
final class FakeGrparse extends ParseStreamingServiceGrpc.ParseStreamingServiceImplBase {

    private volatile List<DocumentStreamEvent> script = List.of();
    private volatile Status failWith;

    /** Every chunk received, across the current script, in arrival order. */
    final List<DocumentChunk> received = Collections.synchronizedList(new ArrayList<>());

    /** Arms the fake to replay {@code events} after consuming the request. */
    void reset(List<DocumentStreamEvent> events) {
        script = List.copyOf(events);
        failWith = null;
        received.clear();
    }

    /** Arms the fake to fail the stream with {@code status} on half-close. */
    void resetFailing(Status status) {
        script = List.of();
        failWith = status;
        received.clear();
    }

    @Override
    public StreamObserver<DocumentChunk> streamProcessDocument(
            StreamObserver<DocumentStreamEvent> out) {
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
                for (DocumentStreamEvent event : script) {
                    out.onNext(event);
                }
                out.onCompleted();
            }
        };
    }
}

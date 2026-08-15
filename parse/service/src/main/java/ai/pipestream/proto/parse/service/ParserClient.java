package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.parse.plugin.v1.DocumentClaims;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoRequest;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoResponse;
import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.plugin.v1.ParseRequest;
import ai.pipestream.proto.parse.plugin.v1.ParseResponse;
import ai.pipestream.proto.parse.plugin.v1.ParserOutput;
import ai.pipestream.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * One parser target behind the {@code ParserPluginService} contract: the
 * coordinator's zero-bespoke client. {@link #parse} drives the wire idiom —
 * one options frame, then 1 MiB data frames, typed events back — and reduces
 * the event stream to a {@link ParseOutcome} the coordinator can store.
 *
 * <p>Progress, page, and preview events are tolerated and dropped: they exist
 * for front ends materializing a document progressively, not for
 * coordination. Claims are collected for the search-metadata fold; the single
 * {@link ParserOutput} is the product. A stream that terminates with a gRPC
 * status error, or completes without emitting a document, is a failed
 * outcome — recorded, never thrown.
 */
public final class ParserClient {

    /** Data frame size of the chunked request stream: 1 MiB. */
    static final int CHUNK_BYTES = 1024 * 1024;

    private final ManagedChannel channel;
    private volatile GetParserInfoResponse info;

    /**
     * Wraps one parser channel. The channel is owned by the caller (the
     * {@link ParserRegistry} in the coordinator) and closed there.
     *
     * @param channel the channel to the parser service
     */
    public ParserClient(ManagedChannel channel) {
        if (channel == null) {
            throw new IllegalArgumentException("channel must not be null");
        }
        this.channel = channel;
    }

    /**
     * The parser's identity, capabilities, and limits — fetched once and
     * cached (the contract declares it cheap and side-effect free).
     *
     * @return the cached {@code GetParserInfo} answer
     */
    public GetParserInfoResponse info() {
        GetParserInfoResponse cached = info;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            if (info == null) {
                info = ParserPluginServiceGrpc.newBlockingStub(channel)
                        .getParserInfo(GetParserInfoRequest.getDefaultInstance());
            }
            return info;
        }
    }

    /**
     * The reduced outcome of one {@code Parse} stream.
     *
     * @param output the final parsed document; {@code null} when failed
     * @param claims every {@code DocumentClaims} event, in stream order
     * @param failed whether the parse failed (status error or no document)
     * @param error the failure reason; empty when not failed
     */
    public record ParseOutcome(
            ParserOutput output, List<DocumentClaims> claims, boolean failed, String error) {
    }

    /**
     * Runs one parse: options frame, chunked payload, events reduced to an
     * outcome. Blocking — call on a virtual thread.
     *
     * @param options the per-parse options (the mandatory first frame)
     * @param payload the document bytes
     * @param deadline per-parse deadline; the stream is cancelled beyond it
     * @return the outcome; a failure is an outcome, never an exception
     */
    public ParseOutcome parse(ParseOptions options, byte[] payload, Duration deadline) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (deadline == null || deadline.isZero() || deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must be positive");
        }
        List<DocumentClaims> claims = new ArrayList<>();
        // Single-element holders written by the event observer, read after the latch.
        ParserOutput[] output = new ParserOutput[1];
        String[] error = new String[1];
        CountDownLatch done = new CountDownLatch(1);

        StreamObserver<ParseRequest> requests = ParserPluginServiceGrpc.newStub(channel)
                .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                .parse(new StreamObserver<>() {
                    @Override
                    public void onNext(ParseResponse event) {
                        switch (event.getEventCase()) {
                            case CLAIMS -> claims.add(event.getClaims());
                            case DOCUMENT -> output[0] = event.getDocument();
                            // Progress, pages, and previews are front-end fare.
                            case PROGRESS, PAGE, PREVIEW, EVENT_NOT_SET -> {
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        Status status = Status.fromThrowable(t);
                        error[0] = status.getDescription() == null || status.getDescription().isBlank()
                                ? status.getCode().toString()
                                : status.getDescription();
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        try {
            requests.onNext(ParseRequest.newBuilder().setOptions(options).build());
            for (int offset = 0; offset < payload.length; offset += CHUNK_BYTES) {
                int length = Math.min(CHUNK_BYTES, payload.length - offset);
                requests.onNext(ParseRequest.newBuilder()
                        .setData(ByteString.copyFrom(payload, offset, length))
                        .build());
            }
            requests.onCompleted();
        } catch (IllegalStateException alreadyClosed) {
            // The server failed the stream while frames were in flight; the
            // terminal error still arrives on the event observer.
        }
        try {
            if (!done.await(deadline.toMillis() + TimeUnit.SECONDS.toMillis(10),
                    TimeUnit.MILLISECONDS)) {
                return new ParseOutcome(null, List.copyOf(claims), true,
                        "parse stream did not terminate within its deadline");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ParseOutcome(null, List.copyOf(claims), true, "parse interrupted");
        }
        if (error[0] != null) {
            return new ParseOutcome(null, List.copyOf(claims), true, error[0]);
        }
        if (output[0] == null) {
            return new ParseOutcome(null, List.copyOf(claims), true,
                    "parser completed without emitting a document");
        }
        return new ParseOutcome(output[0], List.copyOf(claims), false, "");
    }
}

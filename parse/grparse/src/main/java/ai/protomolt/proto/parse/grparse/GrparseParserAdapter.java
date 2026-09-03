package ai.protomolt.proto.parse.grparse;

import ai.protomolt.proto.parse.document.v1.Document;
import ai.protomolt.proto.parse.grparse.v1.CollectorDocument;
import ai.protomolt.proto.parse.grparse.v1.DocumentChunk;
import ai.protomolt.proto.parse.grparse.v1.DocumentComplete;
import ai.protomolt.proto.parse.grparse.v1.DocumentStreamEvent;
import ai.protomolt.proto.parse.grparse.v1.PageData;
import ai.protomolt.proto.parse.grparse.v1.ParseStreamingServiceGrpc;
import ai.protomolt.proto.parse.document.DoclingProjection;
import ai.protomolt.proto.parse.plugin.v1.DocumentClaims;
import ai.protomolt.proto.parse.plugin.v1.GetParserInfoRequest;
import ai.protomolt.proto.parse.plugin.v1.GetParserInfoResponse;
import ai.protomolt.proto.parse.plugin.v1.PagePreview;
import ai.protomolt.proto.parse.plugin.v1.ParseOptions;
import ai.protomolt.proto.parse.plugin.v1.ParseProgress;
import ai.protomolt.proto.parse.plugin.v1.ParseRequest;
import ai.protomolt.proto.parse.plugin.v1.ParseResponse;
import ai.protomolt.proto.parse.plugin.v1.ParsedPage;
import ai.protomolt.proto.parse.plugin.v1.ParserOutput;
import ai.protomolt.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The gRParse adapter: a {@code ParserPluginService} that bridges the
 * coordinator's plugin contract to gRParse's
 * {@code ParseStreamingService.StreamProcessDocument} page stream. The C++
 * fleet parser joins the platform through this sidecar with zero C++
 * changes: run the adapter next to gRParse, register a service profile
 * pointing at it, and the coordinator discovers {@value #PARSER_NAME}.
 *
 * <p>Bridge shape: the plugin's options-first chunked request stream is
 * buffered (capped by {@link GrparseAdapterOptions#maxDocumentBytes()}) and,
 * on half-close, replayed to gRParse as {@code DocumentChunk}s — a metadata
 * first chunk, 1 MiB data chunks, then a terminal {@code complete=true}
 * chunk. gRParse's events map onto the plugin envelope as they arrive:
 * {@code PageData} becomes a {@code ParsedPage} (when the caller asked for
 * pages) and is always accumulated for final assembly;
 * {@code DocumentComplete} contributes the origin and collector failures;
 * {@code CollectorDocument} contributes an out-of-process collector's full
 * document. When gRParse completes, the accumulated content is assembled
 * into one fleet-model document ({@link FleetAssembly}), claims are offered,
 * and the {@code ParserOutput} is emitted exactly once.
 *
 * <p>Previews: the streaming wire carries no processing options, so the
 * adapter cannot request page renders; page images arrive exactly when the
 * gRParse fleet is built to render them into
 * {@code PageData.page_meta.image} (a data URI). When the caller asked for
 * previews, each such image is decoded and forwarded live as a
 * {@code PagePreview} ({@link PagePreviews}); {@code emits_previews}
 * advertises the deployment fact from
 * {@link GrparseAdapterOptions#emitsPreviews()}. {@code PreviewSpec}
 * parameters cannot reach the fleet and are ignored, as the plugin contract
 * allows for parameters a parser cannot honor.
 *
 * <p>Degradation: gRParse degrades instead of failing while any collector
 * succeeds; each {@code CollectorFailure} becomes one
 * {@code ParserOutput.warnings} line, which the coordinator stores as
 * PARTIAL. A gRParse stream error fails the plugin stream with the same
 * status class, description prefixed {@code "grparse: "}, which the
 * coordinator records as FAILED.
 */
public final class GrparseParserAdapter extends ParserPluginServiceGrpc.ParserPluginServiceImplBase
        implements AutoCloseable {

    /** THE parser identity: routing rules and result maps key on it. */
    public static final String PARSER_NAME = "grparse";

    /** Prefix selecting an in-process gRParse channel: {@code inprocess:<name>}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    /** Data chunk size on the gRParse stream: 1 MiB. */
    static final int CHUNK_BYTES = 1024 * 1024;

    private final ManagedChannel grparse;
    private final GrparseAdapterOptions options;
    private final boolean ownsChannel;

    /**
     * Wraps an existing channel to gRParse. The caller keeps ownership of
     * the channel; {@link #close()} leaves it open.
     *
     * @param grparse the channel to gRParse's streaming service
     * @param options adapter configuration
     */
    public GrparseParserAdapter(ManagedChannel grparse, GrparseAdapterOptions options) {
        this(grparse, options, false);
    }

    /**
     * Opens a channel to gRParse and wraps it. The adapter owns the channel
     * and {@link #close()} shuts it down.
     *
     * @param target the gRParse endpoint — {@code host:port} or
     *        {@code inprocess:<name>}
     * @param options adapter configuration
     */
    public GrparseParserAdapter(String target, GrparseAdapterOptions options) {
        this(openChannel(target), options, true);
    }

    private GrparseParserAdapter(
            ManagedChannel grparse, GrparseAdapterOptions options, boolean ownsChannel) {
        if (grparse == null) {
            throw new IllegalArgumentException("grparse channel must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        this.grparse = grparse;
        this.options = options;
        this.ownsChannel = ownsChannel;
    }

    @Override
    public void getParserInfo(
            GetParserInfoRequest request, StreamObserver<GetParserInfoResponse> observer) {
        observer.onNext(
                GetParserInfoResponse.newBuilder()
                        .setParserName(PARSER_NAME)
                        .setParserVersion(options.parserVersion())
                        .setApiVersion("v1")
                        .addSupportedMimeTypes("application/pdf")
                        .addSupportedMimeTypes("image/png")
                        .addSupportedMimeTypes("image/jpeg")
                        .addSupportedMimeTypes("image/tiff")
                        .addSupportedMimeTypes("image/webp")
                        .addSupportedExtensions("pdf")
                        .addSupportedExtensions("png")
                        .addSupportedExtensions("jpg")
                        .addSupportedExtensions("jpeg")
                        .addSupportedExtensions("tif")
                        .addSupportedExtensions("tiff")
                        .addSupportedExtensions("webp")
                        .setMaxDocumentBytes(options.maxDocumentBytes())
                        .setEmitsPages(true)
                        .setEmitsPreviews(options.emitsPreviews())
                        .setEmitsClaims(true)
                        .build());
        observer.onCompleted();
    }

    @Override
    public StreamObserver<ParseRequest> parse(StreamObserver<ParseResponse> observer) {
        return new ParseCall(observer);
    }

    /** Shuts down the gRParse channel when this adapter opened it. */
    @Override
    public void close() {
        if (!ownsChannel) {
            return;
        }
        grparse.shutdownNow();
        try {
            grparse.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ManagedChannel openChannel(String target) {
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("target must not be blank");
        }
        if (target.startsWith(INPROCESS_TARGET_PREFIX)) {
            return InProcessChannelBuilder.forName(
                    target.substring(INPROCESS_TARGET_PREFIX.length())).build();
        }
        return NettyChannelBuilder.forTarget(target).usePlaintext().build();
    }

    // ------------------------------------------------------------------

    /**
     * One Parse call: buffers the plugin request stream, replays it to
     * gRParse on half-close, and folds the event stream back. One instance
     * per call, driven by gRPC's serialized callbacks — no locking needed.
     */
    private final class ParseCall implements StreamObserver<ParseRequest> {

        private final StreamObserver<ParseResponse> observer;
        private final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        private ParseOptions parseOptions;
        private boolean failed;
        private long sequence;
        private StreamObserver<DocumentChunk> upstream;
        private boolean upstreamDone;

        /**
         * Pages by page number; iteration order is page order regardless of
         * arrival order.
         */
        private final SortedMap<Integer, PageData> pages = new TreeMap<>();

        private final List<CollectorDocument> collectorDocuments = new ArrayList<>();
        private DocumentComplete documentComplete;

        private ParseCall(StreamObserver<ParseResponse> observer) {
            this.observer = observer;
        }

        @Override
        public void onNext(ParseRequest frame) {
            if (failed) {
                return;
            }
            try {
                switch (frame.getFrameCase()) {
                    case OPTIONS -> acceptOptions(frame.getOptions());
                    case DATA -> acceptData(frame.getData());
                    case FRAME_NOT_SET -> throw Status.INVALID_ARGUMENT
                            .withDescription("frame is required: set options or data")
                            .asRuntimeException();
                }
            } catch (Throwable t) {
                fail(t);
            }
        }

        private void acceptOptions(ParseOptions first) {
            if (parseOptions != null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("options frame after the first frame")
                        .asRuntimeException();
            }
            parseOptions = first;
            emit(builder -> builder.setProgress(
                    ParseProgress.newBuilder()
                            .setPhase(ParseProgress.Phase.PHASE_STARTED)
                            .setMessage("grparse parse started")));
        }

        private void acceptData(ByteString data) {
            if (parseOptions == null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("the first frame must carry options, not data")
                        .asRuntimeException();
            }
            if ((long) payload.size() + data.size() > options.maxDocumentBytes()) {
                throw Status.RESOURCE_EXHAUSTED
                        .withDescription("payload exceeds " + options.maxDocumentBytes() + " bytes")
                        .asRuntimeException();
            }
            payload.writeBytes(data.toByteArray());
        }

        @Override
        public void onError(Throwable t) {
            failed = true;
            cancelUpstream("plugin caller went away");
        }

        @Override
        public void onCompleted() {
            if (failed) {
                return;
            }
            try {
                if (parseOptions == null) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("the stream closed without an options frame")
                            .asRuntimeException();
                }
                openGrparseStream();
                sendChunks(payload.toByteArray());
            } catch (Throwable t) {
                fail(t);
            }
        }

        private void openGrparseStream() {
            upstream = ParseStreamingServiceGrpc.newStub(grparse)
                    .withDeadlineAfter(options.deadline().toMillis(), TimeUnit.MILLISECONDS)
                    .streamProcessDocument(new StreamObserver<>() {
                        @Override
                        public void onNext(DocumentStreamEvent event) {
                            acceptEvent(event);
                        }

                        @Override
                        public void onError(Throwable t) {
                            upstreamDone = true;
                            failUpstream(t);
                        }

                        @Override
                        public void onCompleted() {
                            upstreamDone = true;
                            assembleAndFinish();
                        }
                    });
        }

        /**
         * Replays the buffered payload: a metadata-only first chunk, 1 MiB
         * data chunks, then a terminal {@code complete=true} chunk.
         */
        private void sendChunks(byte[] bytes) {
            upstream.onNext(DocumentChunk.newBuilder()
                    .setDocumentId(parseOptions.getDocumentId())
                    .setFilename(parseOptions.getFilename())
                    .setContentType(parseOptions.getContentType())
                    .build());
            for (int offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
                upstream.onNext(DocumentChunk.newBuilder()
                        .setData(ByteString.copyFrom(
                                bytes, offset, Math.min(CHUNK_BYTES, bytes.length - offset)))
                        .build());
            }
            upstream.onNext(DocumentChunk.newBuilder().setComplete(true).build());
            upstream.onCompleted();
        }

        private void acceptEvent(DocumentStreamEvent event) {
            if (failed) {
                return;
            }
            try {
                switch (event.getEventCase()) {
                    case PAGE -> acceptPage(event);
                    case COMPLETE -> documentComplete = event.getComplete();
                    case COLLECTOR_DOCUMENT -> collectorDocuments.add(event.getCollectorDocument());
                    case EVENT_NOT_SET -> {
                        // A bare envelope carries nothing to map; skip it.
                    }
                }
            } catch (Throwable t) {
                fail(t);
            }
        }

        /**
         * One completed page. The page number comes from
         * {@code PageData.page_number} — the field gRParse scopes the event
         * by ("its repeated fields contain only nodes belonging to
         * page_number"); {@code page_meta.page_no} merely duplicates it
         * inside the docling metadata and may be absent. Emitted as a
         * {@code ParsedPage} only when the caller asked for pages; always
         * accumulated for final assembly.
         */
        private void acceptPage(DocumentStreamEvent event) {
            PageData page = event.getPage();
            pages.put(page.getPageNumber(), page);
            if (parseOptions.getEmitPages()) {
                emit(builder -> builder.setPage(ParsedPage.newBuilder()
                        .setPageNumber(page.getPageNumber())
                        .setTotalPages(Math.max(0, event.getTotalPages()))
                        .setContent(Any.pack(page))
                        .setText(DoclingTexts.pageText(page))));
            }
            if (parseOptions.getEmitPreviews()) {
                emitPreview(page);
            }
        }

        /**
         * Forwards the page's rendered image, when the fleet supplied one
         * as an embedded data URI; pages without one are skipped, never
         * failed (see {@link PagePreviews}).
         */
        private void emitPreview(PageData page) {
            PagePreview preview = PagePreviews.fromPage(page);
            if (preview != null) {
                emit(builder -> builder.setPreview(preview));
            }
        }

        private void assembleAndFinish() {
            if (failed) {
                return;
            }
            try {
                if (pages.isEmpty() && collectorDocuments.isEmpty()) {
                    throw Status.INTERNAL
                            .withDescription(
                                    "grparse: stream completed with no pages and no collector documents")
                            .asRuntimeException();
                }
                Document assembled = FleetAssembly.assemble(
                        pages, collectorDocuments, documentComplete, parseOptions.getFilename());
                emitClaims(assembled);
                DoclingProjection.Projected projected = DoclingProjection.toParserDocument(assembled);
                emit(builder -> builder.setDocument(
                        ParserOutput.newBuilder()
                                .setDocument(projected.document())
                                .addAllWarnings(FleetAssembly.warningsOf(
                                        documentComplete, collectorDocuments))));
                observer.onCompleted();
            } catch (Throwable t) {
                fail(t);
            }
        }

        /**
         * Offers document claims before the final output: a title when the
         * assembled texts carry one (never fabricated from the filename),
         * and the page count when the pages map has entries.
         */
        private void emitClaims(Document assembled) {
            Struct.Builder claims = Struct.newBuilder();
            String title = DoclingTexts.titleOf(assembled);
            if (!title.isBlank()) {
                claims.putFields("title", Value.newBuilder().setStringValue(title).build());
            }
            if (assembled.getPagesCount() > 0) {
                claims.putFields(
                        "page_count",
                        Value.newBuilder().setNumberValue(assembled.getPagesCount()).build());
            }
            if (claims.getFieldsCount() > 0) {
                Struct built = claims.build();
                emit(builder -> builder.setClaims(DocumentClaims.newBuilder().setClaims(built)));
            }
        }

        private void emit(java.util.function.Consumer<ParseResponse.Builder> filler) {
            ParseResponse.Builder builder = ParseResponse.newBuilder()
                    .setDocumentId(parseOptions == null ? "" : parseOptions.getDocumentId())
                    .setSequenceNumber(sequence++);
            filler.accept(builder);
            observer.onNext(builder.build());
        }

        /** Fails the plugin stream with the gRParse status class, prefixed. */
        private void failUpstream(Throwable t) {
            if (failed) {
                return;
            }
            failed = true;
            Status status = Status.fromThrowable(t);
            String description = status.getDescription() == null
                    ? status.getCode().toString()
                    : status.getDescription();
            observer.onError(Status.fromCode(status.getCode())
                    .withDescription("grparse: " + description)
                    .asRuntimeException());
        }

        private void fail(Throwable t) {
            failed = true;
            // An adapter-side failure abandons the gRParse stream too:
            // leaving it half-open would burn fleet compute until the
            // deadline.
            cancelUpstream("adapter failed the parse");
            if (t instanceof io.grpc.StatusRuntimeException) {
                observer.onError(t);
            } else {
                observer.onError(Status.INTERNAL
                        .withDescription(t.getMessage() == null ? "parse failed" : t.getMessage())
                        .asRuntimeException());
            }
        }

        private void cancelUpstream(String reason) {
            if (upstream != null && !upstreamDone) {
                upstreamDone = true;
                upstream.onError(Status.CANCELLED
                        .withDescription(reason)
                        .asRuntimeException());
            }
        }
    }
}

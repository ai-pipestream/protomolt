package ai.pipestream.proto.parse.grparse;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.parse.document.v1.BaseTextItem;
import ai.pipestream.proto.parse.document.v1.Document;
import ai.pipestream.proto.parse.document.v1.DocumentOrigin;
import ai.pipestream.proto.parse.document.v1.ImageRef;
import ai.pipestream.proto.parse.document.v1.PageItem;
import ai.pipestream.proto.parse.document.v1.Size;
import ai.pipestream.proto.parse.document.v1.TextItem;
import ai.pipestream.proto.parse.document.v1.TextItemBase;
import ai.pipestream.proto.parse.document.v1.TitleItem;
import ai.pipestream.proto.parse.grparse.v1.Collector;
import ai.pipestream.proto.parse.grparse.v1.CollectorDocument;
import ai.pipestream.proto.parse.grparse.v1.CollectorFailure;
import ai.pipestream.proto.parse.grparse.v1.DocumentChunk;
import ai.pipestream.proto.parse.grparse.v1.DocumentComplete;
import ai.pipestream.proto.parse.grparse.v1.DocumentStreamEvent;
import ai.pipestream.proto.parse.grparse.v1.PageData;
import ai.pipestream.proto.parse.document.DoclingProjection;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoRequest;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoResponse;
import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.plugin.v1.ParseRequest;
import ai.pipestream.proto.parse.plugin.v1.ParseResponse;
import ai.pipestream.proto.parse.plugin.v1.PagePreview;
import ai.pipestream.proto.parse.plugin.v1.ParsedPage;
import ai.pipestream.proto.parse.plugin.v1.ParserOutput;
import ai.pipestream.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Drives the gRParse adapter over the wire against a scripted
 * {@link FakeGrparse} and pins the bridge: chunk replay, page flow, final
 * assembly, collector handling, degradation, and failure mapping.
 */
class GrparseParserAdapterTest {

    static FakeGrparse fake;
    static String grparseName;
    static Server grparseServer;
    static GrparseParserAdapter adapter;
    static Server adapterServer;
    static ManagedChannel channel;
    static ParserPluginServiceGrpc.ParserPluginServiceStub stub;
    static ParserPluginServiceGrpc.ParserPluginServiceBlockingStub blocking;

    @BeforeAll
    static void boot() throws Exception {
        fake = new FakeGrparse();
        grparseName = InProcessServerBuilder.generateName();
        grparseServer = InProcessServerBuilder.forName(grparseName)
                .directExecutor()
                .addService(fake)
                .build()
                .start();
        // The inprocess:<name> convenience target is part of the contract.
        adapter = new GrparseParserAdapter(
                GrparseParserAdapter.INPROCESS_TARGET_PREFIX + grparseName,
                GrparseAdapterOptions.defaults());
        String adapterName = InProcessServerBuilder.generateName();
        adapterServer = InProcessServerBuilder.forName(adapterName)
                .directExecutor()
                .addService(adapter)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(adapterName).build();
        stub = ParserPluginServiceGrpc.newStub(channel);
        blocking = ParserPluginServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        adapterServer.shutdownNow();
        adapter.close();
        grparseServer.shutdownNow();
    }

    @Test
    void chunkReplayIsPinned() throws Exception {
        fake.reset(List.of(pageEvent(1, 1, "Only page."), completeEvent()));
        byte[] payload = new byte[5 * 1024 * 1024 / 2];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }

        parse(payload, false);

        List<DocumentChunk> chunks = List.copyOf(fake.received);
        assertThat(chunks).hasSize(5);
        // First chunk: metadata only.
        DocumentChunk first = chunks.getFirst();
        assertThat(first.getDocumentId()).isEqualTo("doc-g1");
        assertThat(first.getFilename()).isEqualTo("scan.pdf");
        assertThat(first.getContentType()).isEqualTo("application/pdf");
        assertThat(first.getData().isEmpty()).isTrue();
        assertThat(first.getComplete()).isFalse();
        // Data chunks: 1 MiB, 1 MiB, 0.5 MiB, no metadata.
        assertThat(chunks.get(1).getData().size()).isEqualTo(1024 * 1024);
        assertThat(chunks.get(2).getData().size()).isEqualTo(1024 * 1024);
        assertThat(chunks.get(3).getData().size()).isEqualTo(512 * 1024);
        for (DocumentChunk data : chunks.subList(1, 4)) {
            assertThat(data.getDocumentId()).isEmpty();
            assertThat(data.getComplete()).isFalse();
        }
        // Terminal chunk: complete=true, nothing else.
        DocumentChunk last = chunks.getLast();
        assertThat(last.getComplete()).isTrue();
        assertThat(last.getData().isEmpty()).isTrue();
        // Concatenation is byte-identical.
        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (DocumentChunk chunk : chunks) {
            reassembled.writeBytes(chunk.getData().toByteArray());
        }
        assertThat(reassembled.toByteArray()).isEqualTo(payload);
    }

    @Test
    void pageFlowHonorsEmitPages() throws Exception {
        List<DocumentStreamEvent> script = List.of(
                pageEvent(1, 2, "Alpha text."),
                pageEvent(2, 2, "Beta text."),
                completeEvent());

        fake.reset(script);
        List<ParseResponse> withPages = parse("payload".getBytes(), true);
        // Sequence numbers are monotonic from zero and echo the document id.
        for (int i = 0; i < withPages.size(); i++) {
            assertThat(withPages.get(i).getSequenceNumber()).isEqualTo(i);
            assertThat(withPages.get(i).getDocumentId()).isEqualTo("doc-g1");
        }
        List<ParseResponse> pageEvents = withPages.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.PAGE)
                .toList();
        assertThat(pageEvents).hasSize(2);
        ParsedPage pageOne = pageEvents.get(0).getPage();
        assertThat(pageOne.getPageNumber()).isEqualTo(1);
        assertThat(pageOne.getTotalPages()).isEqualTo(2);
        assertThat(pageOne.getText()).isEqualTo("Alpha text.");
        assertThat(pageOne.getContent().is(PageData.class)).isTrue();
        assertThat(pageEvents.get(1).getPage().getPageNumber()).isEqualTo(2);
        assertThat(pageEvents.get(1).getPage().getText()).isEqualTo("Beta text.");
        // Every page event precedes the final document.
        int documentIndex = indexOfDocument(withPages);
        for (ParseResponse pageEvent : pageEvents) {
            assertThat(withPages.indexOf(pageEvent)).isLessThan(documentIndex);
        }
        ParserOutput outputWithPages = withPages.get(documentIndex).getDocument();

        fake.reset(script);
        List<ParseResponse> withoutPages = parse("payload".getBytes(), false);
        assertThat(withoutPages)
                .noneMatch(e -> e.getEventCase() == ParseResponse.EventCase.PAGE);
        ParserOutput outputWithoutPages =
                withoutPages.get(indexOfDocument(withoutPages)).getDocument();
        assertThat(outputWithoutPages).isEqualTo(outputWithPages);
    }

    @Test
    void assemblyConcatenatesPagesInOrder() throws Exception {
        fake.reset(List.of(
                pageEvent(1, 2, "Alpha text."),
                pageEvent(2, 2, "Beta text."),
                completeEvent()));

        List<ParseResponse> events = parse("payload".getBytes(), true);

        // The page_count claim reflects the pages map.
        ParseResponse claims = events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.CLAIMS)
                .findFirst()
                .orElseThrow();
        assertThat(claims.getClaims().getClaims().getFieldsMap()
                .get("page_count").getNumberValue()).isEqualTo(2.0);

        ParserOutput output = events.get(indexOfDocument(events)).getDocument();
        // The assembled document round-trips through the projection.
        Document docling = DoclingProjection.fromParserDocument(output.getDocument()).orElseThrow();
        assertThat(docling.getTextsList()).hasSize(2);
        assertThat(docling.getTexts(0).getText().getBase().getText()).isEqualTo("Alpha text.");
        assertThat(docling.getTexts(1).getText().getBase().getText()).isEqualTo("Beta text.");
        assertThat(docling.getOrigin().getMimetype()).isEqualTo("application/pdf");
        assertThat(docling.getOrigin().getFilename()).isEqualTo("scan.pdf");
        assertThat(docling.getPagesMap()).hasSize(2);
        assertThat(docling.getPagesMap().get(1).getPageNo()).isEqualTo(1);
        assertThat(docling.getSchemaName()).isEqualTo("docling_document_v2");
        assertThat(docling.getName()).isEqualTo("scan.pdf");
    }

    @Test
    void aSoleCollectorDocumentIsTheOutput() throws Exception {
        Document collected = Document.newBuilder()
                .setSchemaName("docling_document_v2")
                .setName("report.docx")
                .setOrigin(DocumentOrigin.newBuilder()
                        .setMimetype("application/vnd.openxmlformats-officedocument"
                                + ".wordprocessingml.document")
                        .setFilename("report.docx"))
                .addTexts(BaseTextItem.newBuilder()
                        .setTitle(TitleItem.newBuilder()
                                .setBase(TextItemBase.newBuilder()
                                        .setSelfRef("#/texts/0")
                                        .setText("Quarterly Report"))))
                .addTexts(textItem("#/texts/1", "Body paragraph."))
                .build();
        fake.reset(List.of(
                DocumentStreamEvent.newBuilder()
                        .setDocumentId("doc-g1")
                        .setCollectorDocument(CollectorDocument.newBuilder()
                                .setCollector(Collector.COLLECTOR_LIBREOFFICE)
                                .setDocument(collected))
                        .build(),
                completeEvent()));

        List<ParseResponse> events = parse("payload".getBytes(), true);

        ParserOutput output = events.get(indexOfDocument(events)).getDocument();
        Document docling = DoclingProjection.fromParserDocument(output.getDocument()).orElseThrow();
        assertThat(docling).isEqualTo(collected);
        assertThat(output.getWarningsList()).isEmpty();
        // The collector's title is claimed, never fabricated.
        ParseResponse claims = events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.CLAIMS)
                .findFirst()
                .orElseThrow();
        assertThat(claims.getClaims().getClaims().getFieldsMap()
                .get("title").getStringValue()).isEqualTo("Quarterly Report");
    }

    @Test
    void aCollectorFailureDegradesToAWarning() throws Exception {
        fake.reset(List.of(
                pageEvent(1, 1, "Only page."),
                DocumentStreamEvent.newBuilder()
                        .setDocumentId("doc-g1")
                        .setComplete(DocumentComplete.newBuilder()
                                .setOrigin(origin())
                                .addCollectorFailures(CollectorFailure.newBuilder()
                                        .setCollector(Collector.COLLECTOR_LIBREOFFICE)
                                        .setError("connection refused")))
                        .build()));

        List<ParseResponse> events = parse("payload".getBytes(), false);

        ParserOutput output = events.get(indexOfDocument(events)).getDocument();
        assertThat(output.getWarningsList())
                .containsExactly("collector COLLECTOR_LIBREOFFICE: connection refused");
        // The document is still emitted; the loss is reported, not dropped.
        Document docling = DoclingProjection.fromParserDocument(output.getDocument()).orElseThrow();
        assertThat(docling.getTextsList()).hasSize(1);
    }

    @Test
    void grparseFailureMapsToTheSameStatusClassPrefixed() throws Exception {
        fake.resetFailing(Status.UNAVAILABLE.withDescription("engine down"));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests =
                stub.parse(observer(new ArrayList<>(), failure, done));
        requests.onNext(optionsFrame(false));
        requests.onNext(dataFrame("payload".getBytes()));
        requests.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
        Status status = ((StatusRuntimeException) failure.get()).getStatus();
        assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(status.getDescription()).isEqualTo("grparse: engine down");
    }

    @Test
    void advertisesItselfHonestly() {
        GetParserInfoResponse info =
                blocking.getParserInfo(GetParserInfoRequest.getDefaultInstance());
        assertThat(info.getParserName()).isEqualTo("grparse");
        assertThat(info.getParserVersion())
                .isEqualTo(GrparseAdapterOptions.DEFAULT_PARSER_VERSION);
        assertThat(info.getApiVersion()).isEqualTo("v1");
        assertThat(info.getSupportedMimeTypesList()).contains(
                "application/pdf", "image/png", "image/jpeg", "image/tiff", "image/webp");
        assertThat(info.getSupportedExtensionsList()).contains(
                "pdf", "png", "jpg", "jpeg", "tif", "tiff", "webp");
        assertThat(info.getEmitsPages()).isTrue();
        // Previews are a deployment fact; the default configuration does not
        // claim the fleet renders page images.
        assertThat(info.getEmitsPreviews()).isFalse();
        assertThat(info.getEmitsClaims()).isTrue();
        assertThat(info.getMaxDocumentBytes())
                .isEqualTo(GrparseAdapterOptions.DEFAULT_MAX_DOCUMENT_BYTES);
    }

    @Test
    void fleetRenderedPageImagesForwardAsPreviewsWhenAsked() throws Exception {
        byte[] png = new byte[] {(byte) 0x89, 'P', 'N', 'G', 0, 1, 2, 3};
        fake.reset(List.of(
                pageWithImage(1, 2, "Alpha text.",
                        "data:image/png;base64," + Base64.getEncoder().encodeToString(png),
                        612, 792),
                pageEvent(2, 2, "Beta text."),
                completeEvent()));
        List<ParseResponse> events = parse("payload".getBytes(), false, true);
        List<ParseResponse> previews = events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.PREVIEW)
                .toList();
        // The unrendered page is a page the fleet did not render: skipped.
        assertThat(previews).hasSize(1);
        PagePreview preview = previews.getFirst().getPreview();
        assertThat(preview.getPageNumber()).isEqualTo(1);
        assertThat(preview.getMimeType()).isEqualTo("image/png");
        assertThat(preview.getImage().toByteArray()).isEqualTo(png);
        assertThat(preview.getWidth()).isEqualTo(612);
        assertThat(preview.getHeight()).isEqualTo(792);
    }

    @Test
    void previewsStayUnemittedWhenNotAsked() throws Exception {
        byte[] png = new byte[] {(byte) 0x89, 'P', 'N', 'G'};
        fake.reset(List.of(
                pageWithImage(1, 1, "Alpha text.",
                        "data:image/png;base64," + Base64.getEncoder().encodeToString(png),
                        612, 792),
                completeEvent()));
        List<ParseResponse> events = parse("payload".getBytes(), false, false);
        assertThat(events)
                .noneMatch(e -> e.getEventCase() == ParseResponse.EventCase.PREVIEW);
    }

    @Test
    void nonDataImageUrisAreSkippedNotFailed() throws Exception {
        fake.reset(List.of(
                pageWithImage(1, 1, "Alpha text.", "s3://bucket/page-1.png", 612, 792),
                completeEvent()));
        List<ParseResponse> events = parse("payload".getBytes(), false, true);
        assertThat(events)
                .noneMatch(e -> e.getEventCase() == ParseResponse.EventCase.PREVIEW);
    }

    @Test
    void malformedImageDataUrisAreSkippedNotFailed() throws Exception {
        fake.reset(List.of(
                pageWithImage(1, 1, "Alpha text.", "data:image/png;base64,not!!base64", 612, 792),
                pageWithImage(2, 2, "Beta text.", "data:image/png;base64,", 612, 792),
                completeEvent()));
        List<ParseResponse> events = parse("payload".getBytes(), false, true);
        assertThat(events)
                .noneMatch(e -> e.getEventCase() == ParseResponse.EventCase.PREVIEW);
    }

    @Test
    void aSecondOptionsFrameIsInvalidArgument() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests =
                stub.parse(observer(new ArrayList<>(), failure, done));
        requests.onNext(optionsFrame(false));
        requests.onNext(optionsFrame(false));

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) failure.get()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void aStreamClosingWithoutOptionsIsInvalidArgument() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests =
                stub.parse(observer(new ArrayList<>(), failure, done));
        try {
            requests.onCompleted();
        } catch (RuntimeException alreadyClosed) {
            // The server already rejected the stream.
        }

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) failure.get()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    void anOversizedPayloadIsResourceExhaustedBeforeGrparse() throws Exception {
        // A second adapter with a 1 KiB cap, over the same fake: the payload
        // is rejected before anything reaches gRParse.
        fake.reset(List.of());
        String tinyName = InProcessServerBuilder.generateName();
        GrparseParserAdapter tiny = new GrparseParserAdapter(
                InProcessChannelBuilder.forName(grparseName).build(),
                new GrparseAdapterOptions(
                        GrparseAdapterOptions.DEFAULT_PARSER_VERSION,
                        1024,
                        GrparseAdapterOptions.DEFAULT_DEADLINE));
        Server tinyServer = InProcessServerBuilder.forName(tinyName)
                .directExecutor()
                .addService(tiny)
                .build()
                .start();
        ManagedChannel tinyChannel = InProcessChannelBuilder.forName(tinyName).build();
        try {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            StreamObserver<ParseRequest> requests =
                    ParserPluginServiceGrpc.newStub(tinyChannel)
                            .parse(observer(new ArrayList<>(), failure, done));
            requests.onNext(optionsFrame(false));
            requests.onNext(dataFrame(new byte[2048]));
            requests.onCompleted();

            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
            assertThat(((StatusRuntimeException) failure.get()).getStatus().getCode())
                    .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
            assertThat(fake.received).isEmpty();
        } finally {
            tinyChannel.shutdownNow();
            tinyServer.shutdownNow();
        }
    }

    @Test
    void anAdapterFailureMidStreamCancelsTheGrparseStream() throws Exception {
        // A second adapter behind two fault injectors: a server interceptor
        // breaks the plugin response stream when armed, and a client
        // interceptor records every ClientCall.cancel toward gRParse. The
        // recording rides on ClientCall.cancel because gRPC's context
        // propagation cancels the stream below ClientCall — only the
        // adapter's explicit cancelUpstream shows up here. The replay parks
        // after the first page; with the send fault armed, the second page's
        // emit throws inside the adapter — the fail() path, which must cancel
        // the gRParse stream instead of leaking it.
        fake.resetAsyncReplay(List.of(
                pageEvent(1, 2, "Alpha text."),
                pageEvent(2, 2, "Beta text."),
                completeEvent()));
        AtomicReference<Throwable> upstreamCancelCause = new AtomicReference<>();
        CountDownLatch upstreamCancelCalled = new CountDownLatch(1);
        ManagedChannel grparseChannel = InProcessChannelBuilder.forName(grparseName)
                .intercept(new ClientInterceptor() {
                    @Override
                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
                            Channel next) {
                        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                                next.newCall(method, callOptions)) {
                            @Override
                            public void cancel(String message, Throwable cause) {
                                upstreamCancelCause.set(cause);
                                upstreamCancelCalled.countDown();
                                super.cancel(message, cause);
                            }
                        };
                    }
                })
                .build();
        AtomicBoolean sendFaultArmed = new AtomicBoolean();
        GrparseParserAdapter faultedAdapter =
                new GrparseParserAdapter(grparseChannel, GrparseAdapterOptions.defaults());
        String faultedName = InProcessServerBuilder.generateName();
        Server faultedServer = InProcessServerBuilder.forName(faultedName)
                .directExecutor()
                .addService(ServerInterceptors.intercept(faultedAdapter, new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                            ServerCall<ReqT, RespT> call, Metadata headers,
                            ServerCallHandler<ReqT, RespT> next) {
                        ServerCall<ReqT, RespT> faulty =
                                new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(
                                        call) {
                                    @Override
                                    public void sendMessage(RespT message) {
                                        if (sendFaultArmed.get()) {
                                            throw Status.INTERNAL
                                                    .withDescription("injected send failure")
                                                    .asRuntimeException();
                                        }
                                        super.sendMessage(message);
                                    }
                                };
                        return next.startCall(faulty, headers);
                    }
                }))
                .build()
                .start();
        ManagedChannel faultedChannel = InProcessChannelBuilder.forName(faultedName).build();
        try {
            List<ParseResponse> events = new ArrayList<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            CountDownLatch firstPage = new CountDownLatch(1);
            StreamObserver<ParseRequest> requests =
                    ParserPluginServiceGrpc.newStub(faultedChannel)
                            .parse(new StreamObserver<>() {
                                @Override
                                public void onNext(ParseResponse value) {
                                    events.add(value);
                                    if (value.getEventCase() == ParseResponse.EventCase.PAGE) {
                                        firstPage.countDown();
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    failure.set(t);
                                    done.countDown();
                                }

                                @Override
                                public void onCompleted() {
                                    done.countDown();
                                }
                            });
            requests.onNext(optionsFrame(true));
            requests.onNext(dataFrame("payload".getBytes()));
            requests.onCompleted();

            // The first page went out: the gRParse stream is open, fed, and
            // parked mid-replay.
            assertThat(firstPage.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(fake.received).hasSize(3);
            sendFaultArmed.set(true);
            fake.releaseReplay();

            // The second page's emit throws; fail() cancels the gRParse
            // stream with its own reason.
            assertThat(upstreamCancelCalled.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(upstreamCancelCause.get()).isInstanceOf(StatusRuntimeException.class);
            Status cancelStatus =
                    ((StatusRuntimeException) upstreamCancelCause.get()).getStatus();
            assertThat(cancelStatus.getCode()).isEqualTo(Status.Code.CANCELLED);
            assertThat(cancelStatus.getDescription()).isEqualTo("adapter failed the parse");
            // The cancellation reached gRParse, and the plugin stream failed
            // with the injected fault.
            assertThat(fake.awaitUpstreamCancelled(10)).isTrue();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
            assertThat(((StatusRuntimeException) failure.get()).getStatus().getDescription())
                    .isEqualTo("injected send failure");
        } finally {
            faultedChannel.shutdownNow();
            faultedServer.shutdownNow();
            grparseChannel.shutdownNow();
        }
    }

    @Test
    void anEmptyGrparseStreamIsAnInternalFailure() throws Exception {
        fake.reset(List.of());

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests =
                stub.parse(observer(new ArrayList<>(), failure, done));
        requests.onNext(optionsFrame(false));
        requests.onNext(dataFrame("payload".getBytes()));
        requests.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
        Status status = ((StatusRuntimeException) failure.get()).getStatus();
        assertThat(status.getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(status.getDescription()).contains("no pages and no collector documents");
    }

    // ------------------------------------------------------------------

    static DocumentStreamEvent pageWithImage(
            int pageNumber, int totalPages, String text, String uri, double width, double height) {
        DocumentStreamEvent base = pageEvent(pageNumber, totalPages, text);
        return base.toBuilder()
                .setPage(base.getPage().toBuilder()
                        .setPageMeta(base.getPage().getPageMeta().toBuilder()
                                .setImage(ImageRef.newBuilder()
                                        .setMimetype("image/png")
                                        .setUri(uri)
                                        .setSize(Size.newBuilder()
                                                .setWidth(width)
                                                .setHeight(height)))))
                .build();
    }

    static DocumentStreamEvent pageEvent(int pageNumber, int totalPages, String text) {
        return DocumentStreamEvent.newBuilder()
                .setDocumentId("doc-g1")
                .setTotalPages(totalPages)
                .setPage(PageData.newBuilder()
                        .setPageNumber(pageNumber)
                        .setPageMeta(PageItem.newBuilder().setPageNo(pageNumber))
                        .addTexts(textItem("#/texts/" + (pageNumber - 1), text)))
                .build();
    }

    static BaseTextItem.Builder textItem(String selfRef, String text) {
        return BaseTextItem.newBuilder()
                .setText(TextItem.newBuilder()
                        .setBase(TextItemBase.newBuilder()
                                .setSelfRef(selfRef)
                                .setOrig(text)
                                .setText(text)));
    }

    static DocumentStreamEvent completeEvent() {
        return DocumentStreamEvent.newBuilder()
                .setDocumentId("doc-g1")
                .setComplete(DocumentComplete.newBuilder().setOrigin(origin()))
                .build();
    }

    static DocumentOrigin origin() {
        return DocumentOrigin.newBuilder()
                .setMimetype("application/pdf")
                .setFilename("scan.pdf")
                .build();
    }

    static ParseRequest optionsFrame(boolean emitPages) {
        return optionsFrame(emitPages, false);
    }

    static ParseRequest optionsFrame(boolean emitPages, boolean emitPreviews) {
        return ParseRequest.newBuilder()
                .setOptions(ParseOptions.newBuilder()
                        .setDocumentId("doc-g1")
                        .setFilename("scan.pdf")
                        .setContentType("application/pdf")
                        .setEmitPages(emitPages)
                        .setEmitPreviews(emitPreviews))
                .build();
    }

    static ParseRequest dataFrame(byte[] data) {
        return ParseRequest.newBuilder().setData(ByteString.copyFrom(data)).build();
    }

    static int indexOfDocument(List<ParseResponse> events) {
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).getEventCase() == ParseResponse.EventCase.DOCUMENT) {
                return i;
            }
        }
        throw new AssertionError("no ParserOutput event in " + events);
    }

    static List<ParseResponse> parse(byte[] payload, boolean emitPages) throws Exception {
        return parse(payload, emitPages, false);
    }

    static List<ParseResponse> parse(byte[] payload, boolean emitPages, boolean emitPreviews)
            throws Exception {
        List<ParseResponse> events = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests = stub.parse(observer(events, failure, done));
        requests.onNext(optionsFrame(emitPages, emitPreviews));
        requests.onNext(dataFrame(payload));
        requests.onCompleted();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        if (failure.get() != null) {
            throw new AssertionError("parse failed", failure.get());
        }
        // The final document arrives exactly once.
        assertThat(events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.DOCUMENT)
                .count()).isEqualTo(1);
        return events;
    }

    static StreamObserver<ParseResponse> observer(
            List<ParseResponse> events, AtomicReference<Throwable> failure, CountDownLatch done) {
        return new StreamObserver<>() {
            @Override
            public void onNext(ParseResponse value) {
                events.add(value);
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        };
    }
}

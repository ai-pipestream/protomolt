package ai.pipestream.proto.parse.grparse;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.DocumentOrigin;
import ai.pipestream.document.v1.PageItem;
import ai.pipestream.document.v1.TextItem;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.document.v1.TitleItem;
import ai.pipestream.parse.v1.Collector;
import ai.pipestream.parse.v1.CollectorDocument;
import ai.pipestream.parse.v1.CollectorFailure;
import ai.pipestream.parse.v1.DocumentChunk;
import ai.pipestream.parse.v1.DocumentComplete;
import ai.pipestream.parse.v1.DocumentStreamEvent;
import ai.pipestream.parse.v1.PageData;
import ai.pipestream.proto.parse.document.DoclingProjection;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoRequest;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoResponse;
import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.plugin.v1.ParseRequest;
import ai.pipestream.proto.parse.plugin.v1.ParseResponse;
import ai.pipestream.proto.parse.plugin.v1.ParsedPage;
import ai.pipestream.proto.parse.plugin.v1.ParserOutput;
import ai.pipestream.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    static Server grparseServer;
    static GrparseParserAdapter adapter;
    static Server adapterServer;
    static ManagedChannel channel;
    static ParserPluginServiceGrpc.ParserPluginServiceStub stub;
    static ParserPluginServiceGrpc.ParserPluginServiceBlockingStub blocking;

    @BeforeAll
    static void boot() throws Exception {
        fake = new FakeGrparse();
        String grparseName = InProcessServerBuilder.generateName();
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
        // StreamProcessDocument has no preview surface; the adapter says so.
        assertThat(info.getEmitsPreviews()).isFalse();
        assertThat(info.getEmitsClaims()).isTrue();
        assertThat(info.getMaxDocumentBytes())
                .isEqualTo(GrparseAdapterOptions.DEFAULT_MAX_DOCUMENT_BYTES);
    }

    // ------------------------------------------------------------------

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
        return ParseRequest.newBuilder()
                .setOptions(ParseOptions.newBuilder()
                        .setDocumentId("doc-g1")
                        .setFilename("scan.pdf")
                        .setContentType("application/pdf")
                        .setEmitPages(emitPages))
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
        List<ParseResponse> events = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests = stub.parse(observer(events, failure, done));
        requests.onNext(optionsFrame(emitPages));
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

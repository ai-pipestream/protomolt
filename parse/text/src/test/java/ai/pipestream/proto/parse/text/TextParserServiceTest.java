package ai.pipestream.proto.parse.text;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.Document;
import ai.pipestream.proto.parse.document.DoclingProjection;
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
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Drives the reference parser over the wire and pins the plugin contract. */
class TextParserServiceTest {

    static Server server;
    static ManagedChannel channel;
    static ParserPluginServiceGrpc.ParserPluginServiceStub stub;
    static ParserPluginServiceGrpc.ParserPluginServiceBlockingStub blocking;

    @BeforeAll
    static void boot() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new TextParserService())
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        stub = ParserPluginServiceGrpc.newStub(channel);
        blocking = ParserPluginServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void advertisesItselfHonestly() {
        GetParserInfoResponse info =
                blocking.getParserInfo(GetParserInfoRequest.getDefaultInstance());
        assertThat(info.getParserName()).isEqualTo("text");
        assertThat(info.getSupportedMimeTypesList()).contains("text/plain", "text/markdown");
        assertThat(info.getEmitsClaims()).isTrue();
        assertThat(info.getEmitsPages()).isFalse();
        assertThat(info.getEmitsPreviews()).isFalse();
    }

    @Test
    void parsesMarkdownIntoTheFleetModelWithATitleClaim() throws Exception {
        List<ParseResponse> events = parse(
                "# Habeas Overview\n\nFirst paragraph\nstill first.\n\nSecond paragraph.",
                "overview.md");

        // Sequence numbers are monotonic from zero.
        for (int i = 0; i < events.size(); i++) {
            assertThat(events.get(i).getSequenceNumber()).isEqualTo(i);
            assertThat(events.get(i).getDocumentId()).isEqualTo("doc-t1");
        }
        // The claim precedes the document; the document arrives exactly once.
        DocumentClaims claims = events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.CLAIMS)
                .map(ParseResponse::getClaims)
                .findFirst()
                .orElseThrow();
        assertThat(claims.getClaims().getFieldsMap().get("title").getStringValue())
                .isEqualTo("Habeas Overview");
        List<ParserOutput> outputs = events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.DOCUMENT)
                .map(ParseResponse::getDocument)
                .toList();
        assertThat(outputs).hasSize(1);

        Document docling =
                DoclingProjection.fromParserDocument(outputs.getFirst().getDocument()).orElseThrow();
        assertThat(docling.getTextsList()).hasSize(3);
        assertThat(docling.getTexts(0).getItemCase()).isEqualTo(BaseTextItem.ItemCase.TITLE);
        assertThat(docling.getTexts(0).getTitle().getBase().getText()).isEqualTo("Habeas Overview");
        // Single newlines inside a block flow into one item.
        assertThat(docling.getTexts(1).getText().getBase().getText())
                .isEqualTo("First paragraph still first.");
        assertThat(docling.getTexts(2).getText().getBase().getText()).isEqualTo("Second paragraph.");
        assertThat(docling.getOrigin().getFilename()).isEqualTo("overview.md");
    }

    @Test
    void plainProseWithoutAHeadingClaimsNoTitleButStillClaimsTheBody() throws Exception {
        List<ParseResponse> events = parse(
                "This is a full sentence that ends with a period.\n\nAnother paragraph.", "notes.txt");
        assertThat(events)
                .noneMatch(e -> e.getEventCase() == ParseResponse.EventCase.CLAIMS
                        && e.getClaims().getClaims().containsFields("title"));
        assertThat(events)
                .anyMatch(e -> e.getEventCase() == ParseResponse.EventCase.CLAIMS
                        && e.getClaims().getClaims().getFieldsOrThrow("body").getStringValue()
                                .startsWith("This is a full sentence"));
        ParserOutput output = events.stream()
                .filter(e -> e.getEventCase() == ParseResponse.EventCase.DOCUMENT)
                .map(ParseResponse::getDocument)
                .findFirst()
                .orElseThrow();
        Document docling =
                DoclingProjection.fromParserDocument(output.getDocument()).orElseThrow();
        assertThat(docling.getTextsList()).hasSize(2);
        assertThat(docling.getTexts(0).getItemCase()).isEqualTo(BaseTextItem.ItemCase.TEXT);
    }

    @Test
    void dataBeforeOptionsIsInvalidArgument() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests = stub.parse(observer(new ArrayList<>(), failure, done));
        requests.onNext(ParseRequest.newBuilder()
                .setData(ByteString.copyFromUtf8("data first"))
                .build());
        try {
            requests.onCompleted();
        } catch (IllegalStateException alreadyClosed) {
            // The server already rejected the stream.
        }
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) failure.get()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    // ------------------------------------------------------------------

    static List<ParseResponse> parse(String text, String filename) throws Exception {
        List<ParseResponse> events = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests = stub.parse(observer(events, failure, done));
        requests.onNext(ParseRequest.newBuilder()
                .setOptions(ParseOptions.newBuilder()
                        .setDocumentId("doc-t1")
                        .setFilename(filename)
                        .setContentType("text/plain"))
                .build());
        requests.onNext(ParseRequest.newBuilder()
                .setData(ByteString.copyFromUtf8(text))
                .build());
        requests.onCompleted();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        if (failure.get() != null) {
            throw new AssertionError("parse failed", failure.get());
        }
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

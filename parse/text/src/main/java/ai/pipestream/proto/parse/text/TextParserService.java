package ai.pipestream.proto.parse.text;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.DocumentOrigin;
import ai.pipestream.document.v1.TextItem;
import ai.pipestream.document.v1.TextItemBase;
import ai.pipestream.document.v1.TitleItem;
import ai.pipestream.proto.parse.document.DoclingProjection;
import ai.pipestream.proto.parse.plugin.v1.DocumentClaims;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoRequest;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoResponse;
import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.plugin.v1.ParseProgress;
import ai.pipestream.proto.parse.plugin.v1.ParseRequest;
import ai.pipestream.proto.parse.plugin.v1.ParseResponse;
import ai.pipestream.proto.parse.plugin.v1.ParserOutput;
import ai.pipestream.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * The reference parser: plain text and markdown into the fleet document
 * model, implemented straight against the parser plugin contract with no
 * external engine. It is the in-JVM parser for tests, demos, and the
 * all-in-one container — and doubles as the living example a new parser
 * author copies.
 *
 * <p>Parsing model: the payload decodes as UTF-8; blank-line-separated
 * blocks become text items; the first markdown heading (or, absent one, the
 * first line when it looks like a heading) becomes the title item. Claims:
 * {@code title} when one was found. No pages, no previews — the parser
 * advertises exactly what it does.
 */
public final class TextParserService extends ParserPluginServiceGrpc.ParserPluginServiceImplBase {

    /** THE parser identity: routing rules and result maps key on it. */
    public static final String PARSER_NAME = "text";

    /** The parser build version advertised by GetParserInfo. */
    public static final String PARSER_VERSION = "1.0.0";

    /** Per-document byte cap; larger payloads are RESOURCE_EXHAUSTED. */
    public static final long MAX_DOCUMENT_BYTES = 32L * 1024 * 1024;

    @Override
    public void getParserInfo(
            GetParserInfoRequest request, StreamObserver<GetParserInfoResponse> observer) {
        observer.onNext(
                GetParserInfoResponse.newBuilder()
                        .setParserName(PARSER_NAME)
                        .setParserVersion(PARSER_VERSION)
                        .setApiVersion("v1")
                        .addSupportedMimeTypes("text/plain")
                        .addSupportedMimeTypes("text/markdown")
                        .addSupportedExtensions("txt")
                        .addSupportedExtensions("md")
                        .setMaxDocumentBytes(MAX_DOCUMENT_BYTES)
                        .setMaxConcurrentParses(64)
                        .setEmitsPages(false)
                        .setEmitsPreviews(false)
                        .setEmitsClaims(true)
                        .build());
        observer.onCompleted();
    }

    @Override
    public StreamObserver<ParseRequest> parse(StreamObserver<ParseResponse> observer) {
        return new StreamObserver<>() {
            private ParseOptions options;
            private final ByteArrayOutputStream payload = new ByteArrayOutputStream();
            private boolean failed;
            private long sequence;

            @Override
            public void onNext(ParseRequest frame) {
                if (failed) {
                    return;
                }
                try {
                    switch (frame.getFrameCase()) {
                        case OPTIONS -> acceptOptions(frame.getOptions());
                        case DATA -> acceptData(frame.getData().toByteArray());
                        case FRAME_NOT_SET -> throw Status.INVALID_ARGUMENT
                                .withDescription("frame is required: set options or data")
                                .asRuntimeException();
                    }
                } catch (Throwable t) {
                    fail(t);
                }
            }

            private void acceptOptions(ParseOptions first) {
                if (options != null) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("options frame after the first frame")
                            .asRuntimeException();
                }
                options = first;
                emit(builder -> builder.setProgress(
                        ParseProgress.newBuilder()
                                .setPhase(ParseProgress.Phase.PHASE_STARTED)
                                .setMessage("text parse started")));
            }

            private void acceptData(byte[] data) {
                if (options == null) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("the first frame must carry options, not data")
                            .asRuntimeException();
                }
                if ((long) payload.size() + data.length > MAX_DOCUMENT_BYTES) {
                    throw Status.RESOURCE_EXHAUSTED
                            .withDescription("payload exceeds " + MAX_DOCUMENT_BYTES + " bytes")
                            .asRuntimeException();
                }
                payload.writeBytes(data);
            }

            @Override
            public void onError(Throwable t) {
                failed = true;
            }

            @Override
            public void onCompleted() {
                if (failed) {
                    return;
                }
                try {
                    if (options == null) {
                        throw Status.INVALID_ARGUMENT
                                .withDescription("the stream closed without an options frame")
                                .asRuntimeException();
                    }
                    Parsed parsed = parseText(
                            payload.toString(StandardCharsets.UTF_8), options.getFilename());
                    // The searchable identity of a text document: the title
                    // and the parsed body, folded into SearchMetadata so
                    // downstream indexing has text without re-reading blobs.
                    Struct.Builder claims = Struct.newBuilder();
                    if (!parsed.title().isBlank()) {
                        claims.putFields("title", Value.newBuilder()
                                .setStringValue(parsed.title()).build());
                    }
                    if (!parsed.body().isBlank()) {
                        claims.putFields("body", Value.newBuilder()
                                .setStringValue(parsed.body()).build());
                    }
                    if (claims.getFieldsCount() > 0) {
                        emit(builder -> builder.setClaims(
                                DocumentClaims.newBuilder().setClaims(claims)));
                    }
                    emit(builder -> builder.setProgress(
                            ParseProgress.newBuilder()
                                    .setPhase(ParseProgress.Phase.PHASE_EXPORTING)
                                    .setProgress(0.9f)));
                    DoclingProjection.Projected projected =
                            DoclingProjection.toParserDocument(parsed.document());
                    emit(builder -> builder.setDocument(
                            ParserOutput.newBuilder().setDocument(projected.document())));
                    observer.onCompleted();
                } catch (Throwable t) {
                    fail(t);
                }
            }

            private void emit(java.util.function.Consumer<ParseResponse.Builder> filler) {
                ParseResponse.Builder builder = ParseResponse.newBuilder()
                        .setDocumentId(options == null ? "" : options.getDocumentId())
                        .setSequenceNumber(sequence++);
                filler.accept(builder);
                observer.onNext(builder.build());
            }

            private void fail(Throwable t) {
                failed = true;
                if (t instanceof io.grpc.StatusRuntimeException) {
                    observer.onError(t);
                } else {
                    observer.onError(Status.INTERNAL
                            .withDescription(t.getMessage() == null ? "parse failed" : t.getMessage())
                            .asRuntimeException());
                }
            }
        };
    }

    /** One parse outcome: the document plus the title and body claims (may be blank). */
    private record Parsed(Document document, String title, String body) {}

    /**
     * Blank-line-separated blocks become text items; a leading markdown
     * heading (or heading-looking first line) becomes the title item.
     */
    private static Parsed parseText(String text, String filename) {
        Document.Builder document = Document.newBuilder()
                .setSchemaName("docling_document_v2")
                .setName(filename == null ? "" : filename);
        if (filename != null && !filename.isBlank()) {
            document.setOrigin(DocumentOrigin.newBuilder()
                    .setMimetype("text/plain")
                    .setFilename(filename));
        }
        String title = "";
        StringBuilder body = new StringBuilder();
        int index = 0;
        for (String block : TextCleaning.splitOnBlankLines(text)) {
            String trimmed = TextCleaning.trim(block);
            if (trimmed.isEmpty()) {
                continue;
            }
            String selfRef = "#/texts/" + index;
            if (index == 0 && looksLikeHeading(trimmed)) {
                title = TextCleaning.stripHeadingMarkup(trimmed);
                document.addTexts(BaseTextItem.newBuilder()
                        .setTitle(TitleItem.newBuilder()
                                .setBase(TextItemBase.newBuilder()
                                        .setSelfRef(selfRef)
                                        .setOrig(trimmed)
                                        .setText(title))));
            } else {
                // Rejoin single newlines inside a block: one item per block.
                String flowed = TextCleaning.flowLines(trimmed);
                document.addTexts(BaseTextItem.newBuilder()
                        .setText(TextItem.newBuilder()
                                .setBase(TextItemBase.newBuilder()
                                        .setSelfRef(selfRef)
                                        .setOrig(trimmed)
                                        .setText(flowed))));
                if (!body.isEmpty()) {
                    body.append("\n\n");
                }
                body.append(flowed);
            }
            index++;
        }
        return new Parsed(document.build(), title, body.toString());
    }

    private static boolean looksLikeHeading(String block) {
        if (block.contains("\n")) {
            return false;
        }
        if (block.startsWith("#")) {
            return true;
        }
        return block.length() <= 120 && !block.endsWith(".");
    }
}

package ai.pipestream.proto.parse.service;

import ai.pipestream.proto.parse.plugin.v1.DocumentClaims;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoRequest;
import ai.pipestream.proto.parse.plugin.v1.GetParserInfoResponse;
import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.plugin.v1.ParseProgress;
import ai.pipestream.proto.parse.plugin.v1.ParsedPage;
import ai.pipestream.proto.parse.plugin.v1.ParseRequest;
import ai.pipestream.proto.parse.plugin.v1.ParseResponse;
import ai.pipestream.proto.parse.plugin.v1.ParserOutput;
import ai.pipestream.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import ai.pipestream.proto.repo.v1.ParserDocument;
import com.google.protobuf.Struct;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A configurable in-process parser plugin: emits progress noise (the
 * coordinator must tolerate it), the configured claims, and then either the
 * configured output, a bare completion (no document — a contract violation
 * the coordinator records as FAILED), or a status failure.
 */
final class FakeParserPlugin extends ParserPluginServiceGrpc.ParserPluginServiceImplBase {

    private final String parserName;
    private final String parserVersion;

    volatile List<Struct> claimsToEmit = List.of();
    volatile List<String> pagesToEmit = List.of();
    volatile List<String> warnings = List.of();
    volatile ParserDocument outputDocument = ParserDocument.getDefaultInstance();
    volatile boolean emitOutput = true;
    volatile Status failWith;

    final List<ParseOptions> optionsSeen = new CopyOnWriteArrayList<>();
    final List<byte[]> payloadsSeen = new CopyOnWriteArrayList<>();

    FakeParserPlugin(String parserName, String parserVersion) {
        this.parserName = parserName;
        this.parserVersion = parserVersion;
    }

    /** Resets the per-test behavior and recordings. */
    void reset() {
        claimsToEmit = List.of();
        pagesToEmit = List.of();
        warnings = List.of();
        outputDocument = ParserDocument.getDefaultInstance();
        emitOutput = true;
        failWith = null;
        optionsSeen.clear();
        payloadsSeen.clear();
    }

    @Override
    public void getParserInfo(
            GetParserInfoRequest request, StreamObserver<GetParserInfoResponse> observer) {
        observer.onNext(GetParserInfoResponse.newBuilder()
                .setParserName(parserName)
                .setParserVersion(parserVersion)
                .setApiVersion("v1")
                .setEmitsClaims(true)
                .build());
        observer.onCompleted();
    }

    @Override
    public StreamObserver<ParseRequest> parse(StreamObserver<ParseResponse> observer) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        ParseOptions[] options = new ParseOptions[1];
        return new StreamObserver<>() {
            @Override
            public void onNext(ParseRequest frame) {
                switch (frame.getFrameCase()) {
                    case OPTIONS -> options[0] = frame.getOptions();
                    case DATA -> payload.writeBytes(frame.getData().toByteArray());
                    case FRAME_NOT_SET -> {
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                // Client cancelled; nothing to do.
            }

            @Override
            public void onCompleted() {
                optionsSeen.add(options[0]);
                payloadsSeen.add(payload.toByteArray());
                String documentId = options[0] == null ? "" : options[0].getDocumentId();
                long sequence = 0;
                observer.onNext(event(documentId, ++sequence)
                        .setProgress(ParseProgress.newBuilder()
                                .setPhase(ParseProgress.Phase.PHASE_PROCESSING)
                                .setProgress(0.5f))
                        .build());
                for (int i = 0; i < pagesToEmit.size(); i++) {
                    observer.onNext(event(documentId, ++sequence)
                            .setPage(ParsedPage.newBuilder()
                                    .setPageNumber(i + 1)
                                    .setTotalPages(pagesToEmit.size())
                                    .setText(pagesToEmit.get(i)))
                            .build());
                }
                for (Struct claims : claimsToEmit) {
                    observer.onNext(event(documentId, ++sequence)
                            .setClaims(DocumentClaims.newBuilder().setClaims(claims))
                            .build());
                }
                if (failWith != null) {
                    observer.onError(failWith.asRuntimeException());
                    return;
                }
                if (emitOutput) {
                    observer.onNext(event(documentId, ++sequence)
                            .setDocument(ParserOutput.newBuilder()
                                    .setDocument(outputDocument)
                                    .addAllWarnings(warnings))
                            .build());
                }
                observer.onCompleted();
            }
        };
    }

    private static ParseResponse.Builder event(String documentId, long sequence) {
        return ParseResponse.newBuilder().setDocumentId(documentId).setSequenceNumber(sequence);
    }
}

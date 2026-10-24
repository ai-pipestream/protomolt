package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.UploadAccepted;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.UploadTrailer;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streaming upload coordinator for {@code UploadDocument} and the HTTP raw upload surface
 * (requirements §6, §7, §11, §15.10, §15.11, §15.17).
 * <p>
 * Both entry points execute the same save pipeline as {@code SaveDocument} ({@link DocumentSaves}),
 * streaming payload bytes directly to the stack with bounded heap memory and aborting the stack write
 * without recording an operation outcome if the client cancels mid-stream.
 */
public final class DocumentUploads {
    private static final int QUEUE_CAPACITY = 16;
    private static final int INITIAL_DATA_REQUESTS = 4;
    private static final int OPEN = 0;
    private static final int COMMITTING = 1;
    private static final int WITHDRAWN = 2;

    private final DocumentSaves saves;

    /**
     * Creates a new streaming upload coordinator backed by the given {@link DocumentSaves}.
     *
     * @param saves the document save coordinator
     */
    public DocumentUploads(DocumentSaves saves) {
        this.saves = Objects.requireNonNull(saves, "saves");
    }

    /**
     * The access rules of one revision of an intake document, which an HTTP upload that
     * replaces the revision keeps.
     *
     * @param caller the uploading caller
     * @param accountId the document's account
     * @param datasourceId the intake datasource
     * @param docId the document ID
     * @param revision the revision the upload expects
     * @return the revision's document security, if it carries any
     */
    public Optional<DocumentSecurity> intakeRevisionSecurity(RepositoryCaller caller, String accountId,
            String datasourceId, String docId, long revision) {
        return saves.intakeRevisionSecurity(caller, accountId, datasourceId, docId, revision);
    }

    /**
     * Executes a single streaming upload from an {@link UploadHeader} and payload {@link InputStream}.
     *
     * @param caller the authenticated repository caller
     * @param header the upload header carrying the save command and stream selector
     * @param body the payload byte stream
     * @return the committed or replayed upload response
     */
    public UploadDocumentResponse upload(RepositoryCaller caller, UploadHeader header, InputStream body) {
        return saves.upload(caller, header, body);
    }

    /**
     * Executes a single streaming upload and returns the response together with the uploaded payload's
     * verified SHA-256 and byte length.
     *
     * @param caller the authenticated repository caller
     * @param header the upload header carrying the save command and stream selector
     * @param body the payload byte stream
     * @return the upload result containing the response, SHA-256 hex digest, and byte length
     */
    DocumentSaves.UploadResult uploadResult(RepositoryCaller caller, UploadHeader header, InputStream body) {
        return saves.uploadResult(caller, header, body, () -> false);
    }

    /**
     * Creates the request observer of one bidirectional {@code UploadDocument} call.
     * <p>
     * Frame 1 must be an {@link UploadHeader}. It is validated and admitted (authentication was
     * resolved before the call reached here; ownership, owner match, writer authority and the
     * revision checks run now) before any payload byte is accepted. An inline body then commits
     * and is answered {@code saved}. A streamed body is answered {@code replayed} when the header
     * alone fixes an intent already recorded under its operation ID, and otherwise
     * {@code accepted}, after which data frames are fed through a bounded queue with gRPC
     * flow-control backpressure into a virtual thread running the stack upload and catalog
     * commit. The trailer, when the header announced one, is applied and admitted again once the
     * client half-closes, before the one commit.
     *
     * @param caller the authenticated repository caller
     * @param responseObserver the server response observer
     * @return the request stream observer
     */
    public StreamObserver<UploadDocumentRequest> uploadDocument(
            RepositoryCaller caller,
            StreamObserver<UploadDocumentResponse> responseObserver) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(responseObserver, "responseObserver");
        var cancelled = new AtomicBoolean(false);
        var terminated = new AtomicBoolean(false);
        var responseLock = new Object();
        var serverObserver = responseObserver instanceof ServerCallStreamObserver<UploadDocumentResponse> s ? s : null;
        var streamHolder = new ChunkQueueInputStream[1];
        var trailer = new AtomicReference<UploadTrailer>();
        // OPEN until the upload claims its commit (COMMITTING) or the call is refused or
        // cancelled first (WITHDRAWN). The claim and the refusal exclude each other, so a call
        // answered with an error never commits and a committing call is never answered with one.
        var commitState = new java.util.concurrent.atomic.AtomicInteger(OPEN);
        java.util.function.BooleanSupplier claimCommit =
                () -> !cancelled.get() && commitState.compareAndSet(OPEN, COMMITTING);

        if (serverObserver != null) {
            serverObserver.setOnCancelHandler(() -> {
                cancelled.set(true);
                commitState.compareAndSet(OPEN, WITHDRAWN);
                terminated.set(true);
                var stream = streamHolder[0];
                if (stream != null) stream.cancel();
            });
            serverObserver.disableAutoRequest();
            serverObserver.request(1);
        }

        return new StreamObserver<>() {
            private boolean headerReceived;
            private boolean inline;
            private boolean trailerAnnounced;
            private boolean trailerReceived;

            @Override
            public void onNext(UploadDocumentRequest request) {
                if (cancelled.get() || terminated.get()) return;
                if (!headerReceived) {
                    if (request == null || request.getFrameCase() != UploadDocumentRequest.FrameCase.HEADER) {
                        fail(DocumentSaves.malformedException("The first UploadDocument frame must be UploadHeader"));
                        return;
                    }
                    headerReceived = true;
                    var header = request.getHeader();
                    final DocumentSaves.PreparedUpload prepared;
                    final Optional<SaveDocumentResponse> replayed;
                    try {
                        prepared = saves.precheckUpload(caller, header);
                        replayed = header.hasInlineData() ? Optional.empty() : saves.headerReplay(prepared);
                    } catch (AccessionException failure) {
                        fail(failure);
                        return;
                    } catch (RuntimeException failure) {
                        failRuntime(failure);
                        return;
                    }
                    var name = "repo-upload-" + prepared.request().getOperationId();
                    if (header.hasInlineData()) {
                        inline = true;
                        Thread.ofVirtual().name(name).start(() -> runUpload(prepared, header.getInlineData().newInput(), null));
                        request(1);
                        return;
                    }
                    if (replayed.isPresent()) {
                        answerLast(UploadDocumentResponse.newBuilder().setReplayed(replayed.get()).build());
                        return;
                    }
                    trailerAnnounced = header.getStreamed().getCoreInTrailer();
                    var stream = new ChunkQueueInputStream(serverObserver, cancelled);
                    streamHolder[0] = stream;
                    synchronized (responseLock) {
                        if (terminated.get()) return;
                        responseObserver.onNext(UploadDocumentResponse.newBuilder()
                                .setAccepted(UploadAccepted.getDefaultInstance()).build());
                    }
                    Thread.ofVirtual().name(name).start(() -> runUpload(prepared, stream, stream));
                    request(INITIAL_DATA_REQUESTS);
                    return;
                }

                if (inline) {
                    refuse(DocumentSaves.malformedException("An inline UploadDocument takes no further frames"));
                    return;
                }
                var stream = streamHolder[0];
                var frame = request == null ? UploadDocumentRequest.FrameCase.FRAME_NOT_SET : request.getFrameCase();
                switch (frame) {
                    case DATA -> {
                        if (trailerReceived) {
                            stream.failProtocol("No UploadDocument frame may follow the trailer");
                            return;
                        }
                        var chunk = request.getData();
                        if (chunk.isEmpty()) {
                            request(1);
                            return;
                        }
                        stream.enqueue(chunk);
                    }
                    case TRAILER -> {
                        if (!trailerAnnounced) {
                            stream.failProtocol("The upload header announced no UploadTrailer");
                            return;
                        }
                        if (trailerReceived) {
                            stream.failProtocol("No UploadDocument frame may follow the trailer");
                            return;
                        }
                        trailerReceived = true;
                        trailer.set(request.getTrailer());
                        request(1);
                    }
                    default -> stream.failProtocol("Subsequent UploadDocument frames must carry data or the trailer");
                }
            }

            @Override
            public void onError(Throwable t) {
                cancelled.set(true);
                commitState.compareAndSet(OPEN, WITHDRAWN);
                terminated.set(true);
                var stream = streamHolder[0];
                if (stream != null) stream.cancel();
            }

            @Override
            public void onCompleted() {
                if (cancelled.get() || terminated.get()) return;
                if (!headerReceived) {
                    fail(DocumentSaves.malformedException("UploadDocument stream completed without UploadHeader"));
                    return;
                }
                var stream = streamHolder[0];
                if (stream != null) {
                    stream.completeInput();
                }
            }

            private void request(int frames) {
                if (serverObserver != null && !cancelled.get() && !terminated.get()) {
                    serverObserver.request(frames);
                }
            }

            private void runUpload(DocumentSaves.PreparedUpload prepared, InputStream body, ChunkQueueInputStream stream) {
                try {
                    var result = saves.executeUpload(prepared, body, cancelled::get, trailer::get, claimCommit);
                    if (cancelled.get()) return;
                    answerLast(result.response());
                } catch (AccessionException failure) {
                    if (!cancelled.get()) fail(failure);
                } catch (RuntimeException failure) {
                    if (!cancelled.get()) failRuntime(failure);
                } finally {
                    if (stream != null) stream.closeConsumer();
                }
            }

            private void answerLast(UploadDocumentResponse answer) {
                synchronized (responseLock) {
                    if (terminated.compareAndSet(false, true)) {
                        responseObserver.onNext(answer);
                        responseObserver.onCompleted();
                    }
                }
            }

            /**
             * Refuses the call for a frame the client sent. Once the upload has claimed its commit
             * the refusal is dropped: that commit's own answer ends the call.
             */
            private void refuse(AccessionException failure) {
                synchronized (responseLock) {
                    if (commitState.get() == COMMITTING) return;
                    fail(failure);
                }
            }

            private void fail(AccessionException failure) {
                synchronized (responseLock) {
                    cancelled.set(true);
                    commitState.compareAndSet(OPEN, WITHDRAWN);
                    if (terminated.compareAndSet(false, true)) {
                        var stream = streamHolder[0];
                        if (stream != null) stream.closeConsumer();
                        responseObserver.onError(AccessionGrpcService.toStatus(failure));
                    }
                }
            }

            private void failRuntime(RuntimeException failure) {
                synchronized (responseLock) {
                    cancelled.set(true);
                    commitState.compareAndSet(OPEN, WITHDRAWN);
                    if (terminated.compareAndSet(false, true)) {
                        var stream = streamHolder[0];
                        if (stream != null) stream.closeConsumer();
                        responseObserver.onError(Status.INTERNAL
                                .withDescription("Repository failure")
                                .withCause(failure)
                                .asRuntimeException());
                    }
                }
            }
        };
    }

    /**
     * Bounded blocking queue {@link InputStream} bridging gRPC {@code data} frames to {@code Stack.put}.
     */
    private static final class ChunkQueueInputStream extends InputStream {
        private static final ByteString EOF_SENTINEL = ByteString.EMPTY;

        private final ArrayBlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        private final ServerCallStreamObserver<UploadDocumentResponse> serverObserver;
        private final AtomicBoolean cancelled;
        private volatile boolean consumerClosed;
        private volatile String protocolViolation;
        private ByteString current = ByteString.EMPTY;
        private int offset;
        private boolean eof;

        ChunkQueueInputStream(
                ServerCallStreamObserver<UploadDocumentResponse> serverObserver,
                AtomicBoolean cancelled) {
            this.serverObserver = serverObserver;
            this.cancelled = cancelled;
        }

        void enqueue(ByteString chunk) {
            while (!consumerClosed && !cancelled.get() && protocolViolation == null) {
                try {
                    if (queue.offer(chunk, 50, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    cancel();
                    return;
                }
            }
        }

        void completeInput() {
            enqueue(EOF_SENTINEL);
        }

        void failProtocol(String message) {
            this.protocolViolation = message;
            queue.clear();
            queue.offer(EOF_SENTINEL);
        }

        void cancel() {
            cancelled.set(true);
            queue.clear();
            queue.offer(EOF_SENTINEL);
        }

        void closeConsumer() {
            consumerClosed = true;
            queue.clear();
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int n = read(single, 0, 1);
            return n < 0 ? -1 : (single[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            if (len == 0) return 0;
            if (!advanceChunk()) return -1;
            int count = Math.min(len, current.size() - offset);
            current.substring(offset, offset + count).copyTo(b, off);
            offset += count;
            return count;
        }

        private boolean advanceChunk() throws IOException {
            while (offset >= current.size()) {
                if (cancelled.get()) {
                    throw new DocumentSaves.CancelledUploadException("Upload stream cancelled");
                }
                if (protocolViolation != null) {
                    throw new DocumentSaves.UploadProtocolException(
                            DocumentSaves.malformedException(protocolViolation));
                }
                if (eof) {
                    return false;
                }
                final ByteString next;
                try {
                    next = queue.poll(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new DocumentSaves.CancelledUploadException("Upload stream cancelled");
                }
                if (next == null) {
                    continue;
                }
                if (cancelled.get()) {
                    throw new DocumentSaves.CancelledUploadException("Upload stream cancelled");
                }
                if (protocolViolation != null) {
                    throw new DocumentSaves.UploadProtocolException(
                            DocumentSaves.malformedException(protocolViolation));
                }
                if (next == EOF_SENTINEL) {
                    eof = true;
                    return false;
                }
                current = next;
                offset = 0;
                if (serverObserver != null && !cancelled.get()) {
                    serverObserver.request(1);
                }
            }
            return true;
        }

        @Override
        public void close() {
            closeConsumer();
        }
    }
}

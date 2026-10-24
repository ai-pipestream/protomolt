package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.UploadHeader;
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
     * Creates a gRPC client-streaming observer for {@code UploadDocument}.
     * <p>
     * Frame 1 must be an {@link UploadHeader}, which is validated and pre-authorized before any bytes
     * are accepted or written to the stack. Subsequent frames must carry {@code data} chunks, which
     * are fed through a bounded queue with gRPC flow-control backpressure into a virtual thread
     * running the stack upload and catalog commit.
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

        if (serverObserver != null) {
            serverObserver.setOnCancelHandler(() -> {
                cancelled.set(true);
                terminated.set(true);
                var stream = streamHolder[0];
                if (stream != null) stream.cancel();
            });
            serverObserver.disableAutoRequest();
            serverObserver.request(1);
        }

        return new StreamObserver<>() {
            private boolean headerReceived;
            private boolean dataReceived;
            private boolean trailingHeaderReceived;
            private DocumentSaves.PreparedUpload activePrepared;

            @Override
            public void onNext(UploadDocumentRequest request) {
                if (cancelled.get() || terminated.get()) return;
                if (!headerReceived) {
                    if (request == null || request.getFrameCase() != UploadDocumentRequest.FrameCase.HEADER) {
                        fail(DocumentSaves.malformedException("The first UploadDocument frame must be UploadHeader"));
                        return;
                    }
                    headerReceived = true;
                    final DocumentSaves.PreparedUpload prepared;
                    try {
                        prepared = saves.precheckUpload(caller, request.getHeader());
                    } catch (AccessionException failure) {
                        fail(failure);
                        return;
                    } catch (RuntimeException failure) {
                        failRuntime(failure);
                        return;
                    }
                    activePrepared = prepared;
                    var stream = new ChunkQueueInputStream(serverObserver, cancelled);
                    streamHolder[0] = stream;
                    Thread.ofVirtual()
                            .name("repo-upload-" + prepared.request().getOperationId())
                            .start(() -> runUpload(prepared, stream));
                    if (serverObserver != null && !cancelled.get() && !terminated.get()) {
                        serverObserver.request(INITIAL_DATA_REQUESTS);
                    }
                    return;
                }

                var stream = streamHolder[0];
                if (request != null
                        && request.getFrameCase() == UploadDocumentRequest.FrameCase.HEADER
                        && dataReceived
                        && !trailingHeaderReceived
                        && activePrepared != null
                        && activePrepared.header().hasBlobId()) {
                    trailingHeaderReceived = true;
                    try {
                        saves.refreshUploadCore(activePrepared, request.getHeader());
                    } catch (AccessionException failure) {
                        if (stream != null) {
                            stream.failProtocol(failure.getMessage());
                        } else {
                            fail(failure);
                        }
                        return;
                    } catch (RuntimeException failure) {
                        failRuntime(failure);
                        return;
                    }
                    if (serverObserver != null && !cancelled.get() && !terminated.get()) {
                        serverObserver.request(1);
                    }
                    return;
                }
                if (trailingHeaderReceived || request == null || request.getFrameCase() != UploadDocumentRequest.FrameCase.DATA) {
                    if (stream != null) {
                        stream.failProtocol("Subsequent UploadDocument frames must carry data");
                    } else {
                        fail(DocumentSaves.malformedException("Subsequent UploadDocument frames must carry data"));
                    }
                    return;
                }
                var chunk = request.getData();
                if (chunk.isEmpty()) {
                    if (serverObserver != null && !cancelled.get() && !terminated.get()) {
                        serverObserver.request(1);
                    }
                    return;
                }
                dataReceived = true;
                if (stream != null) {
                    stream.enqueue(chunk);
                }
            }

            @Override
            public void onError(Throwable t) {
                cancelled.set(true);
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

            private void runUpload(DocumentSaves.PreparedUpload prepared, ChunkQueueInputStream stream) {
                try {
                    var result = saves.executeUpload(prepared, stream, cancelled::get);
                    if (cancelled.get()) return;
                    synchronized (responseLock) {
                        if (terminated.compareAndSet(false, true)) {
                            responseObserver.onNext(result.response());
                            responseObserver.onCompleted();
                        }
                    }
                } catch (AccessionException failure) {
                    if (!cancelled.get()) fail(failure);
                } catch (RuntimeException failure) {
                    if (!cancelled.get()) failRuntime(failure);
                } finally {
                    stream.closeConsumer();
                }
            }

            private void fail(AccessionException failure) {
                synchronized (responseLock) {
                    if (terminated.compareAndSet(false, true)) {
                        var stream = streamHolder[0];
                        if (stream != null) stream.closeConsumer();
                        responseObserver.onError(AccessionGrpcService.toStatus(failure));
                    }
                }
            }

            private void failRuntime(RuntimeException failure) {
                synchronized (responseLock) {
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

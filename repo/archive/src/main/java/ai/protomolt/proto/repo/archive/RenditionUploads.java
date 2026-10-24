package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.WindowCapture;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@code UploadRendition} as one v2 {@code UploadDocument}: the header frame
 * becomes the v2 upload header (after the outcome lookup, which may answer the
 * call outright), and each chunk becomes a v2 data frame. v2 streams through a
 * digest, refuses length and digest mismatches and leaves an uncommitted object
 * unreferenced, so the archive stages nothing.
 *
 * <p>Flow control is end to end: the next archive frame is requested only when
 * the v2 call can take another, so a slow repository holds back the producer
 * instead of filling memory.</p>
 *
 * <p>A header with a supplied operation ID and no {@code expected_sha256} has a
 * content digest that covers no byte, so the chunks are digested as they pass:
 * a recorded outcome under that ID is replayed only when the bytes sent match
 * the rendition it recorded, and other bytes are {@code ALREADY_EXISTS}.</p>
 */
public final class RenditionUploads {

    /**
     * Maximum bytes buffered before opening the downstream {@code UploadDocument}
     * stream when an upload carries the entry's primary rendition, allowing both
     * leading (64 KiB) and trailing (128 KiB) characterization windows to be
     * captured into the single revision's CORE while keeping memory strictly bounded.
     */
    static final int MAX_CHARACTERIZE_BUFFER_BYTES = 1024 * 1024;

    /**
     * A flow-controlled upload request observer for non-gRPC producers (such as HTTP
     * streaming handlers) that wait for the underlying v2 {@code UploadDocument}
     * stream to become ready before reading and sending the next chunk.
     */
    public interface FlowControlledUpload extends StreamObserver<UploadRenditionRequest> {
        /**
         * Returns {@code true} when the underlying upload stream can accept another frame
         * without unbounded queueing, or when the upload has already terminated.
         *
         * @return whether the producer may send the next frame immediately
         */
        boolean isReady();

        /**
         * Blocks the calling thread until {@link #isReady()} is {@code true} or the upload
         * finishes, up to {@code timeoutNanos}.
         *
         * @param timeoutNanos maximum time to wait in nanoseconds
         * @return {@code true} if the upload is ready or finished; {@code false} if the wait timed out
         * @throws InterruptedException if the thread is interrupted while waiting
         */
        boolean awaitReady(long timeoutNanos) throws InterruptedException;
    }

    private final DocumentArchive archive;
    private final DocumentServiceGrpc.DocumentServiceStub documents;

    /**
     * Constructs an upload translator using {@code archive}'s own v2 document service channel.
     *
     * @param archive the archive the upload is translated by
     */
    public RenditionUploads(DocumentArchive archive) {
        this(Objects.requireNonNull(archive, "archive"), archive.documents().getChannel());
    }

    /**
     * @param archive the archive the upload is translated by
     * @param documents the channel to the same v2 document service
     */
    public RenditionUploads(DocumentArchive archive, Channel documents) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.documents = DocumentServiceGrpc.newStub(Objects.requireNonNull(documents, "documents"));
    }

    /**
     * Returns the channel to the v2 document service used by this translator.
     *
     * @return the v2 document service channel
     */
    public Channel channel() {
        return documents.getChannel();
    }

    /** Starts one upload call; the returned observer receives the client's frames. */
    public StreamObserver<UploadRenditionRequest> upload(StreamObserver<UploadRenditionResponse> responses) {
        return new Call(responses);
    }

    /**
     * Starts one flow-controlled upload call for a non-gRPC producer.
     *
     * @param responses receives the upload response or error
     * @return the flow-controlled request observer
     */
    public FlowControlledUpload uploadFlowControlled(StreamObserver<UploadRenditionResponse> responses) {
        return new Call(responses);
    }

    private final class Call implements FlowControlledUpload {
        private final StreamObserver<UploadRenditionResponse> responses;
        private final ServerCallStreamObserver<UploadRenditionResponse> server;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition readyChanged = lock.newCondition();
        private DocumentArchive.Pending pending;
        private DocumentArchive.Verify verifying;
        private ClientCallStreamObserver<UploadDocumentRequest> upload;
        private final MessageDigest sent = RenditionReads.sha256();
        private final List<ByteString> bufferedChunks = new ArrayList<>();
        private int bufferedBytes;
        private long sentBytes;
        private boolean clientCompleted;
        private boolean waitingForUpload;
        private boolean finished;

        Call(StreamObserver<UploadRenditionResponse> responses) {
            this.responses = responses;
            this.server = responses instanceof ServerCallStreamObserver<UploadRenditionResponse> call ? call : null;
            if (server != null) {
                server.disableAutoRequest();
                server.request(1);
            }
        }

        @Override
        public boolean isReady() {
            lock.lock();
            try {
                return readyLocked();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean awaitReady(long timeoutNanos) throws InterruptedException {
            long remaining = timeoutNanos;
            lock.lock();
            try {
                while (!readyLocked()) {
                    if (remaining <= 0L) {
                        return false;
                    }
                    remaining = readyChanged.awaitNanos(remaining);
                }
                return true;
            } finally {
                lock.unlock();
            }
        }

        private boolean readyLocked() {
            if (finished || verifying != null || upload == null) {
                return true;
            }
            return upload.isReady();
        }

        @Override
        public void onNext(UploadRenditionRequest frame) {
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                try {
                    if (pending == null && verifying == null) {
                        if (!frame.hasHeader()) {
                            throw ArchiveErrors.invalid("frame", "the first frame of an upload must be the header");
                        }
                        start(frame);
                    } else {
                        if (!frame.hasChunk()) {
                            throw ArchiveErrors.invalid("frame", "only the first frame of an upload is a header");
                        }
                        ByteString chunk = frame.getChunk();
                        chunk.asReadOnlyByteBufferList().forEach(sent::update);
                        sentBytes += chunk.size();
                        if (verifying != null) {
                            if (server != null) {
                                server.request(1);
                            }
                            return;
                        }
                        if (upload == null) {
                            bufferedChunks.add(chunk);
                            bufferedBytes += chunk.size();
                            if (bufferedBytes >= MAX_CHARACTERIZE_BUFFER_BYTES) {
                                openDownstreamUpload(capturedWindows(false));
                                requestNext();
                            } else if (server != null) {
                                server.request(1);
                            }
                            return;
                        }
                        upload.onNext(UploadDocumentRequest.newBuilder().setData(chunk).build());
                        requestNext();
                    }
                } catch (StatusRuntimeException failure) {
                    fail(failure);
                }
            } finally {
                lock.unlock();
            }
        }

        private void start(UploadRenditionRequest frame) {
            DocumentArchive.UploadStart start = archive.startUpload(frame.getHeader());
            if (start instanceof DocumentArchive.Replay replay) {
                finished = true;
                readyChanged.signalAll();
                responses.onNext(replay.response());
                responses.onCompleted();
                return;
            }
            if (start instanceof DocumentArchive.Verify verify) {
                verifying = verify;
                readyChanged.signalAll();
                if (server != null) {
                    server.request(1);
                }
                return;
            }
            pending = (DocumentArchive.Pending) start;
            if (!pending.needsWindows()) {
                openDownstreamUpload(ByteWindows.EMPTY);
                requestNext();
            } else if (server != null) {
                server.request(1);
            }
        }

        private void openDownstreamUpload(ByteWindows windows) {
            documents.uploadDocument(new ClientResponseObserver<UploadDocumentRequest, UploadDocumentResponse>() {
                private UploadDocumentResponse result;

                @Override
                public void beforeStart(ClientCallStreamObserver<UploadDocumentRequest> requests) {
                    upload = requests;
                    requests.setOnReadyHandler(Call.this::uploadReady);
                }

                @Override
                public void onNext(UploadDocumentResponse value) {
                    result = value;
                }

                @Override
                public void onError(Throwable failure) {
                    uploadFailed(failure);
                }

                @Override
                public void onCompleted() {
                    uploadCompleted(result);
                }
            });
            upload.onNext(UploadDocumentRequest.newBuilder().setHeader(pending.header(windows)).build());
            for (ByteString chunk : bufferedChunks) {
                upload.onNext(UploadDocumentRequest.newBuilder().setData(chunk).build());
            }
            bufferedChunks.clear();
            bufferedBytes = 0;
        }

        private void requestNext() {
            if (upload.isReady()) {
                readyChanged.signalAll();
                if (server != null) {
                    server.request(1);
                }
            } else {
                waitingForUpload = true;
            }
        }

        private void uploadReady() {
            lock.lock();
            try {
                readyChanged.signalAll();
                if (waitingForUpload && !finished) {
                    waitingForUpload = false;
                    if (server != null) {
                        server.request(1);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void onError(Throwable failure) {
            lock.lock();
            try {
                finished = true;
                readyChanged.signalAll();
                if (upload != null) {
                    upload.cancel("The archive upload's client failed", failure);
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void onCompleted() {
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                if (verifying != null) {
                    try {
                        DocumentArchive.requireRecordedBytes(verifying.response(), sentSha256(), sentBytes);
                    } catch (StatusRuntimeException failure) {
                        fail(failure);
                        return;
                    }
                    finished = true;
                    readyChanged.signalAll();
                    responses.onNext(verifying.response());
                    responses.onCompleted();
                    return;
                }
                if (pending == null) {
                    fail(ArchiveErrors.invalid("frame", "an upload needs a header frame"));
                    return;
                }
                clientCompleted = true;
                if (upload == null) {
                    try {
                        openDownstreamUpload(capturedWindows(true));
                    } catch (StatusRuntimeException failure) {
                        fail(failure);
                        return;
                    }
                }
                upload.onCompleted();
            } finally {
                lock.unlock();
            }
        }

        private ByteWindows capturedWindows(boolean complete) {
            ByteString buffered = ByteString.copyFrom(bufferedChunks);
            if (!complete) {
                int headLen = Math.min(buffered.size(), ByteWindows.DEFAULT_HEAD_BYTES);
                return ByteWindows.ofHead(buffered.substring(0, headLen).toByteArray());
            }
            try (WindowCapture capture = new WindowCapture(buffered.newInput())) {
                byte[] discard = new byte[8192];
                while (capture.read(discard) >= 0) {
                    // Drain buffered bytes through WindowCapture.
                }
                return capture.windows();
            } catch (IOException impossible) {
                throw new IllegalStateException(impossible);
            }
        }

        private String sentSha256() {
            return HexFormat.of().formatHex(sent.digest());
        }

        private void uploadCompleted(UploadDocumentResponse result) {
            UploadRenditionResponse response;
            try {
                if (result == null) {
                    throw Status.INTERNAL.withDescription("UploadDocument completed without a response")
                            .asRuntimeException();
                }
                response = archive.finishUpload(pending, result.getSaved(), sentSha256(), sentBytes);
            } catch (StatusRuntimeException failure) {
                fail(failure);
                return;
            }
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                finished = true;
                readyChanged.signalAll();
            } finally {
                lock.unlock();
            }
            responses.onNext(response);
            responses.onCompleted();
        }

        private void uploadFailed(Throwable failure) {
            StatusRuntimeException status = failure instanceof StatusRuntimeException runtime
                    ? runtime : Status.fromThrowable(failure).asRuntimeException(Status.trailersFromThrowable(failure));
            UploadRenditionResponse response;
            try {
                String sha256 = null;
                long size = 0;
                if (pending.checkBytes()) {
                    lock.lock();
                    try {
                        if (!clientCompleted) {
                            // Not every byte has arrived, so a lost race cannot be told from other bytes.
                            throw status;
                        }
                        sha256 = sentSha256();
                        size = sentBytes;
                    } finally {
                        lock.unlock();
                    }
                }
                response = archive.uploadResponse(
                        archive.afterLostRace(pending.address(), pending.operationId(), pending.contentSha256(),
                                pending.derivedLastWriteWins(), status).revision(),
                        pending.rendition(), pending.operationId(), true);
                if (sha256 != null) {
                    DocumentArchive.requireRecordedBytes(response, sha256, size);
                }
            } catch (StatusRuntimeException answer) {
                fail(answer);
                return;
            }
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                finished = true;
                readyChanged.signalAll();
            } finally {
                lock.unlock();
            }
            responses.onNext(response);
            responses.onCompleted();
        }

        private void fail(StatusRuntimeException failure) {
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                finished = true;
                readyChanged.signalAll();
            } finally {
                lock.unlock();
            }
            if (upload != null) {
                upload.cancel("The archive upload failed", failure);
            }
            responses.onError(failure);
        }
    }
}

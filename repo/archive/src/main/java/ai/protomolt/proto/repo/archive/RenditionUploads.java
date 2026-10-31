package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.WindowCapture;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.StreamedBody;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.UploadTrailer;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@code UploadRendition} as one v2 {@code UploadDocument}, after the outcome
 * lookup, which may answer the call outright.
 *
 * <p>A rendition of at most {@link #INLINE_BODY_MAX_BYTES} is held in memory
 * and sent as one inline header with its finished CORE, so v2 admits and
 * commits it in one round trip. A larger rendition sends its header first and
 * forwards no chunk until v2 answers {@code accepted}: authentication,
 * ownership, owner match, writer authority and the revision checks all run
 * before a byte moves. Each chunk then becomes a v2 data frame. For the entry's
 * primary rendition the chunks pass through a {@link WindowCapture} (the
 * 64 KiB head and 128 KiB tail, nothing else is kept), and the finished
 * classification goes to v2 in the {@link UploadTrailer}, which v2 admits again
 * before the one commit. No byte is staged on disk.</p>
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
     * The largest rendition sent inline: its header frame, finished CORE and bytes
     * stay well under the default 4 MiB gRPC message size.
     */
    static final int INLINE_BODY_MAX_BYTES = 1024 * 1024;

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
     * @param responses the observer that receives the upload response or failure
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
        private UploadRenditionResponse verifying;
        private ClientCallStreamObserver<UploadDocumentRequest> upload;
        private final MessageDigest sent = RenditionReads.sha256();
        private final Deque<ByteString> bufferedChunks = new ArrayDeque<>();
        private ByteString.Output inline;
        private long declaredSize;
        private WindowCapture windowCapture;
        private boolean accepted;
        private long sentBytes;
        private boolean clientCompleted;
        private boolean downstreamCompleted;
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

        private boolean canAcceptChunk() {
            if (pending == null || verifying != null || inline != null) {
                return true;
            }
            return accepted && bufferedChunks.isEmpty() && upload.isReady();
        }

        @Override
        public boolean isReady() {
            lock.lock();
            try {
                return finished || canAcceptChunk();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean awaitReady(long timeoutNanos) throws InterruptedException {
            long remaining = timeoutNanos;
            lock.lock();
            try {
                while (!finished && !canAcceptChunk()) {
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
                        return;
                    }
                    if (!frame.hasChunk()) {
                        throw ArchiveErrors.invalid("frame", "only the first frame of an upload is a header");
                    }
                    ByteString chunk = frame.getChunk();
                    chunk.asReadOnlyByteBufferList().forEach(sent::update);
                    sentBytes += chunk.size();
                    if (verifying != null) {
                        requestNext();
                        return;
                    }
                    if (inline != null) {
                        if (inline.size() + (long) chunk.size() > INLINE_BODY_MAX_BYTES) {
                            throw ArchiveErrors.invalid("size_bytes",
                                    "the upload sent more bytes than its size_bytes " + declaredSize);
                        }
                        chunk.writeTo(inline);
                        requestNext();
                        return;
                    }
                    if (windowCapture != null) {
                        chunk.asReadOnlyByteBufferList().forEach(windowCapture::update);
                    }
                    if (!bufferedChunks.isEmpty() || !accepted || !upload.isReady()) {
                        bufferedChunks.addLast(chunk);
                        drainBufferedChunks();
                    } else {
                        upload.onNext(UploadDocumentRequest.newBuilder().setData(chunk).build());
                    }
                    requestNext();
                } catch (StatusRuntimeException failure) {
                    fail(failure);
                }
            } catch (java.io.IOException impossible) {
                fail(Status.INTERNAL.withDescription("Failed to hold an inline upload in memory")
                        .withCause(impossible).asRuntimeException());
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
                verifying = verify.response();
                requestNext();
                return;
            }
            pending = (DocumentArchive.Pending) start;
            declaredSize = frame.getHeader().getSizeBytes();
            if (declaredSize <= INLINE_BODY_MAX_BYTES) {
                inline = ByteString.newOutput((int) Math.max(0L, declaredSize));
                requestNext();
                return;
            }
            if (pending.needsWindows()) {
                windowCapture = new WindowCapture();
            }
            UploadHeader header = pending.header(ByteWindows.EMPTY).toBuilder()
                    .setStreamed(StreamedBody.newBuilder().setCoreInTrailer(pending.needsWindows()))
                    .build();
            // The next archive frame is requested once v2 answers accepted.
            waitingForUpload = true;
            openDownstreamCall();
            upload.onNext(UploadDocumentRequest.newBuilder().setHeader(header).build());
        }

        private void openDownstreamCall() {
            documents.uploadDocument(new ClientResponseObserver<UploadDocumentRequest, UploadDocumentResponse>() {
                private SaveDocumentResponse result;
                private boolean replayed;

                @Override
                public void beforeStart(ClientCallStreamObserver<UploadDocumentRequest> requests) {
                    upload = requests;
                    requests.setOnReadyHandler(Call.this::uploadReady);
                }

                @Override
                public void onNext(UploadDocumentResponse value) {
                    switch (value.getAnswerCase()) {
                        case ACCEPTED -> uploadAccepted();
                        case REPLAYED -> {
                            result = value.getReplayed();
                            replayed = true;
                        }
                        case SAVED -> result = value.getSaved();
                        default -> fail(Status.INTERNAL
                                .withDescription("UploadDocument sent an answer of no known kind")
                                .asRuntimeException());
                    }
                }

                @Override
                public void onError(Throwable failure) {
                    uploadFailed(failure);
                }

                @Override
                public void onCompleted() {
                    uploadCompleted(result, replayed);
                }
            });
        }

        private void uploadAccepted() {
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                accepted = true;
                drainBufferedChunks();
                readyChanged.signalAll();
                if (waitingForUpload && canAcceptChunk()) {
                    waitingForUpload = false;
                    if (server != null) {
                        server.request(1);
                    }
                }
            } catch (StatusRuntimeException failure) {
                fail(failure);
            } finally {
                lock.unlock();
            }
        }

        private void drainBufferedChunks() {
            if (!accepted) {
                return;
            }
            while (!bufferedChunks.isEmpty() && upload.isReady()) {
                upload.onNext(UploadDocumentRequest.newBuilder().setData(bufferedChunks.removeFirst()).build());
            }
            if (bufferedChunks.isEmpty() && clientCompleted && !downstreamCompleted) {
                downstreamCompleted = true;
                if (windowCapture != null) {
                    UploadTrailer trailer = UploadTrailer.newBuilder()
                            .setDocument(pending.header(windowCapture.windows()).getSave().getDocument())
                            .build();
                    upload.onNext(UploadDocumentRequest.newBuilder().setTrailer(trailer).build());
                }
                upload.onCompleted();
            }
        }

        private void requestNext() {
            if (canAcceptChunk()) {
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
                if (finished || upload == null) {
                    return;
                }
                try {
                    drainBufferedChunks();
                } catch (StatusRuntimeException failure) {
                    fail(failure);
                    return;
                }
                readyChanged.signalAll();
                if (waitingForUpload && canAcceptChunk()) {
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
                bufferedChunks.clear();
                inline = null;
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
                        DocumentArchive.requireRecordedBytes(verifying, sentSha256(), sentBytes);
                    } catch (StatusRuntimeException failure) {
                        fail(failure);
                        return;
                    }
                    finished = true;
                    readyChanged.signalAll();
                    responses.onNext(verifying);
                    responses.onCompleted();
                    return;
                }
                if (pending == null) {
                    fail(ArchiveErrors.invalid("frame", "an upload needs a header frame"));
                    return;
                }
                clientCompleted = true;
                try {
                    if (inline != null) {
                        sendInline();
                    } else {
                        drainBufferedChunks();
                    }
                } catch (StatusRuntimeException failure) {
                    fail(failure);
                }
            } finally {
                lock.unlock();
            }
        }

        private void sendInline() {
            ByteString body = inline.toByteString();
            inline = null;
            UploadHeader header = pending.header(pending.needsWindows()
                            ? ByteWindows.ofWhole(body.toByteArray())
                            : ByteWindows.EMPTY)
                    .toBuilder()
                    .setInlineData(body)
                    .build();
            openDownstreamCall();
            downstreamCompleted = true;
            upload.onNext(UploadDocumentRequest.newBuilder().setHeader(header).build());
            upload.onCompleted();
        }

        private void uploadCompleted(SaveDocumentResponse result, boolean replayed) {
            UploadRenditionResponse response;
            try {
                if (result == null) {
                    throw Status.INTERNAL.withDescription("UploadDocument completed without a response")
                            .asRuntimeException();
                }
                lock.lock();
                try {
                    if (finished) {
                        return;
                    }
                    if (replayed) {
                        // The header alone fixed an intent v2 had already committed, and no
                        // byte went to v2. When the archive header named no expected_sha256 the
                        // client's bytes are still checked against the recorded rendition.
                        UploadRenditionResponse recorded = archive.finishUpload(pending, result, "", 0L);
                        if (pending.checkBytes() && !clientCompleted) {
                            verifying = recorded;
                            bufferedChunks.clear();
                            requestNext();
                            return;
                        }
                        if (pending.checkBytes()) {
                            DocumentArchive.requireRecordedBytes(recorded, sentSha256(), sentBytes);
                        }
                        response = recorded;
                    } else {
                        if (!result.getReplayed() && !clientCompleted) {
                            throw Status.INTERNAL.withDescription("UploadDocument completed before all upload bytes")
                                    .asRuntimeException();
                        }
                        response = archive.finishUpload(pending, result, sentSha256(), sentBytes);
                    }
                } finally {
                    lock.unlock();
                }
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
                    ? runtime
                    : Status.fromThrowable(failure).asRuntimeException(Status.trailersFromThrowable(failure));
            UploadRenditionResponse replayed;
            try {
                String sha256 = null;
                long bytes = 0;
                lock.lock();
                try {
                    if (finished) {
                        return;
                    }
                    if (pending.checkBytes()) {
                        if (!clientCompleted) {
                            throw status;
                        }
                        sha256 = sentSha256();
                        bytes = sentBytes;
                    }
                } finally {
                    lock.unlock();
                }
                DocumentArchive.Recorded winner = archive.afterLostRace(
                        pending.address(),
                        pending.operationId(),
                        pending.contentSha256(),
                        pending.derivedLastWriteWins(),
                        status);
                replayed = archive.uploadResponse(winner.revision(), pending.rendition(), pending.operationId(), true);
                if (sha256 != null) {
                    DocumentArchive.requireRecordedBytes(replayed, sha256, bytes);
                }
            } catch (StatusRuntimeException translated) {
                fail(translated);
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
            responses.onNext(replayed);
            responses.onCompleted();
        }

        private String sentSha256() {
            try {
                MessageDigest copy = (MessageDigest) sent.clone();
                return HexFormat.of().formatHex(copy.digest());
            } catch (CloneNotSupportedException impossible) {
                throw new IllegalStateException("SHA-256 supports cloning", impossible);
            }
        }

        private void fail(StatusRuntimeException failure) {
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                finished = true;
                bufferedChunks.clear();
                inline = null;
                readyChanged.signalAll();
                if (upload != null && !downstreamCompleted) {
                    upload.cancel("Archive upload failed", failure);
                }
            } finally {
                lock.unlock();
            }
            responses.onError(failure);
        }
    }
}

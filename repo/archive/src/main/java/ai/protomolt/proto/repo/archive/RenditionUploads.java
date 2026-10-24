package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
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

    private final DocumentArchive archive;
    private final DocumentServiceGrpc.DocumentServiceStub documents;

    /**
     * @param archive the archive the upload is translated by
     * @param documents the channel to the same v2 document service
     */
    public RenditionUploads(DocumentArchive archive, Channel documents) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.documents = DocumentServiceGrpc.newStub(Objects.requireNonNull(documents, "documents"));
    }

    /** Starts one upload call; the returned observer receives the client's frames. */
    public StreamObserver<UploadRenditionRequest> upload(StreamObserver<UploadRenditionResponse> responses) {
        return new Call(responses);
    }

    private final class Call implements StreamObserver<UploadRenditionRequest> {
        private final StreamObserver<UploadRenditionResponse> responses;
        private final ServerCallStreamObserver<UploadRenditionResponse> server;
        private final ReentrantLock lock = new ReentrantLock();
        private DocumentArchive.Pending pending;
        private DocumentArchive.Verify verifying;
        private ClientCallStreamObserver<UploadDocumentRequest> upload;
        private final MessageDigest sent = RenditionReads.sha256();
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
                        frame.getChunk().asReadOnlyByteBufferList().forEach(sent::update);
                        sentBytes += frame.getChunk().size();
                        if (verifying != null) {
                            if (server != null) {
                                server.request(1);
                            }
                            return;
                        }
                        upload.onNext(UploadDocumentRequest.newBuilder().setData(frame.getChunk()).build());
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
                responses.onNext(replay.response());
                responses.onCompleted();
                return;
            }
            if (start instanceof DocumentArchive.Verify verify) {
                verifying = verify;
                if (server != null) {
                    server.request(1);
                }
                return;
            }
            pending = (DocumentArchive.Pending) start;
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
            upload.onNext(UploadDocumentRequest.newBuilder().setHeader(pending.header()).build());
            requestNext();
        }

        private void requestNext() {
            if (server == null) {
                return;
            }
            if (upload.isReady()) {
                server.request(1);
            } else {
                waitingForUpload = true;
            }
        }

        private void uploadReady() {
            lock.lock();
            try {
                if (waitingForUpload && !finished) {
                    waitingForUpload = false;
                    server.request(1);
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
                    responses.onNext(verifying.response());
                    responses.onCompleted();
                    return;
                }
                if (pending == null) {
                    fail(ArchiveErrors.invalid("frame", "an upload needs a header frame"));
                    return;
                }
                clientCompleted = true;
                upload.onCompleted();
            } finally {
                lock.unlock();
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
                response = archive.finishUpload(pending, result.getSaved());
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

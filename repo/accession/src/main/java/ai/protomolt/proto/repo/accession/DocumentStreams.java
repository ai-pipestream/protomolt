package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v2.BlobHeader;
import ai.protomolt.proto.repo.v2.BlobObjectRef;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartData;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.ReadComplete;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Server-streaming read handlers for {@code ReadDocument} and {@code ReadBlob}
 * (requirements §6, §8, §15.10, §15.11, §15.12).
 * <p>
 * Both streaming reads resolve and pin the target revision once before sending the
 * first response frame, enforce gRPC outbound flow control via {@link ServerCallStreamObserver},
 * close underlying stack streams immediately on client cancellation, and verify stack
 * object digests before signaling completion.
 */
public final class DocumentStreams {
    /** Maximum byte payload per streamed {@code PartData} or {@code ReadBlobResponse} chunk (64 KiB). */
    public static final int CHUNK_SIZE = 64 * 1024;

    private final DocumentReads reads;

    /**
     * Constructs a streaming read handler backed by the given {@link DocumentReads}.
     *
     * @param reads the document read coordinator
     */
    public DocumentStreams(DocumentReads reads) {
        this.reads = Objects.requireNonNull(reads, "reads");
    }

    /**
     * Executes a server-streaming {@code ReadDocument} call: frame 1 is the revision's
     * {@link DocumentManifest}, followed by {@link PartData} chunks for each selected
     * {@code PART_STATE_PRESENT} part in manifest order, and a final {@link ReadComplete}
     * frame once all selected parts have been verified.
     *
     * @param caller the authenticated repository caller
     * @param request the streaming document read request
     * @param observer the response stream observer
     */
    public void readDocument(
            RepositoryCaller caller,
            ReadDocumentRequest request,
            StreamObserver<ReadDocumentResponse> observer) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(observer, "observer");
        final PreparedDocumentRead prepared;
        try {
            prepared = DocumentReads.guarded(() -> prepareDocumentRead(caller, request));
        } catch (AccessionException failure) {
            observer.onError(AccessionGrpcService.toStatus(failure));
            return;
        } catch (RuntimeException failure) {
            observer.onError(Status.INTERNAL
                    .withDescription("Repository failure")
                    .withCause(failure)
                    .asRuntimeException());
            return;
        }

        var serverObserver = observer instanceof ServerCallStreamObserver<ReadDocumentResponse> s ? s : null;
        var session = new DocumentStreamSession(
                reads,
                observer,
                serverObserver,
                prepared.stored(),
                prepared.manifest(),
                prepared.selectedParts());
        if (serverObserver != null) {
            serverObserver.setOnCancelHandler(session::cancel);
            serverObserver.setOnReadyHandler(session::pump);
        }
        session.pump();
    }

    /**
     * Executes a server-streaming {@code ReadBlob} call: frame 1 is a {@link BlobHeader}
     * carrying the whole blob's length and SHA-256, followed by raw byte chunks for the
     * requested range.
     *
     * @param caller the authenticated repository caller
     * @param request the streaming blob read request
     * @param observer the response stream observer
     */
    public void readBlob(
            RepositoryCaller caller,
            ReadBlobRequest request,
            StreamObserver<ReadBlobResponse> observer) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(observer, "observer");
        final PreparedBlobRead prepared;
        try {
            prepared = DocumentReads.guarded(() -> prepareBlobRead(caller, request));
        } catch (AccessionException failure) {
            observer.onError(AccessionGrpcService.toStatus(failure));
            return;
        } catch (RuntimeException failure) {
            observer.onError(Status.INTERNAL
                    .withDescription("Repository failure")
                    .withCause(failure)
                    .asRuntimeException());
            return;
        }

        var serverObserver = observer instanceof ServerCallStreamObserver<ReadBlobResponse> s ? s : null;
        var session = new BlobStreamSession(observer, serverObserver, prepared.header(), prepared.stream());
        if (serverObserver != null) {
            serverObserver.setOnCancelHandler(session::cancel);
            serverObserver.setOnReadyHandler(session::pump);
        }
        session.pump();
    }

    private PreparedDocumentRead prepareDocumentRead(RepositoryCaller caller, ReadDocumentRequest request) {
        DocumentSaves.requireNoUnknownCommandFields(request);
        DocumentReads.valid(request);
        var stored = reads.resolve(caller, request.getAddress(), request.getRevision());
        var selected = DocumentReads.selected(request.getSelection());
        var selectedParts = new ArrayList<StoredPartEntry>();
        for (var part : stored.getPartsList()) {
            var entry = part.getEntry();
            if (!selected.apply(entry.getSelector()) || entry.getState() != PartState.PART_STATE_PRESENT) {
                continue;
            }
            if (entry.getOpaque() && !request.getAllowOpaque()) {
                throw new AccessionException(
                        Status.Code.FAILED_PRECONDITION,
                        "The revision holds opaque content; request its opaque representation explicitly",
                        null);
            }
            selectedParts.add(part);
        }
        var manifest = DocumentReads.manifest(stored, selected);
        return new PreparedDocumentRead(stored, manifest, List.copyOf(selectedParts));
    }

    private PreparedBlobRead prepareBlobRead(RepositoryCaller caller, ReadBlobRequest request) {
        DocumentSaves.requireNoUnknownCommandFields(request);
        DocumentReads.valid(request);
        var stored = reads.resolve(caller, request.getAddress(), request.getRevision());
        StoredPartEntry blobsPart = null;
        for (var part : stored.getPartsList()) {
            if (part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS
                    && part.getEntry().getState() == PartState.PART_STATE_PRESENT) {
                blobsPart = part;
                break;
            }
        }
        if (blobsPart == null) {
            throw DocumentReads.notFound("Blob not found: " + request.getBlobId());
        }
        BlobObjectRef blobRef = null;
        for (var ref : blobsPart.getBlobRefsList()) {
            if (ref.getBlobId().equals(request.getBlobId())) {
                blobRef = ref;
                break;
            }
        }
        if (blobRef == null) {
            throw DocumentReads.notFound("Blob not found: " + request.getBlobId());
        }
        var blobsFragment = reads.read(blobsPart.getRef());
        var metadata = RawBlobs.metadata(blobsFragment, request.getBlobId());
        var stackRef = blobRef.getRef();
        long totalLength = stackRef.getLength();
        long offset = request.getOffset();
        long length = request.getLength();
        if (offset < 0 || length < 0 || offset > totalLength || length > totalLength - offset) {
            throw new AccessionException(
                    Status.Code.INVALID_ARGUMENT,
                    "Requested byte range [" + offset + ", +" + length + ") is outside blob length " + totalLength,
                    null);
        }

        var stack = reads.drives().byId(stackRef.getStackId());
        final InputStream stream;
        try {
            if (offset == 0 && length == 0) {
                stream = stack.open(stackRef);
            } else {
                long rangeLength = length == 0 ? totalLength - offset : length;
                stream = stack.openRange(stackRef, offset, rangeLength);
            }
        } catch (StackDataLoss loss) {
            throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
        } catch (IOException failure) {
            throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
        }

        var header = BlobHeader.newBuilder()
                .setRevision(stored.getRevision())
                .setTotalLength(totalLength)
                .setSha256(stackRef.getSha256())
                .setMimeType(metadata.mimeType())
                .setFilename(metadata.filename())
                .build();
        return new PreparedBlobRead(header, stream);
    }

    private record PreparedDocumentRead(
            StoredManifest stored,
            DocumentManifest manifest,
            List<StoredPartEntry> selectedParts) {}

    private record PreparedBlobRead(BlobHeader header, InputStream stream) {}

    private static final class DocumentStreamSession {
        private final DocumentReads reads;
        private final StreamObserver<ReadDocumentResponse> observer;
        private final ServerCallStreamObserver<ReadDocumentResponse> serverObserver;
        private final StoredManifest stored;
        private final DocumentManifest manifest;
        private final List<StoredPartEntry> selectedParts;

        private volatile boolean cancelled;
        private boolean completed;
        private boolean pumping;
        private boolean manifestSent;
        private int partIndex;
        private PartSelector currentSelector;
        private ByteString currentBytes;
        private int currentOffset;

        DocumentStreamSession(
                DocumentReads reads,
                StreamObserver<ReadDocumentResponse> observer,
                ServerCallStreamObserver<ReadDocumentResponse> serverObserver,
                StoredManifest stored,
                DocumentManifest manifest,
                List<StoredPartEntry> selectedParts) {
            this.reads = reads;
            this.observer = observer;
            this.serverObserver = serverObserver;
            this.stored = stored;
            this.manifest = manifest;
            this.selectedParts = selectedParts;
        }

        void cancel() {
            cancelled = true;
        }

        synchronized void pump() {
            if (pumping || cancelled || completed) return;
            pumping = true;
            try {
                while (!cancelled && !completed && (serverObserver == null || serverObserver.isReady())) {
                    if (!manifestSent) {
                        manifestSent = true;
                        observer.onNext(ReadDocumentResponse.newBuilder().setManifest(manifest).build());
                        continue;
                    }
                    if (currentBytes == null) {
                        if (partIndex >= selectedParts.size()) {
                            completed = true;
                            observer.onNext(ReadDocumentResponse.newBuilder()
                                    .setComplete(ReadComplete.getDefaultInstance())
                                    .build());
                            observer.onCompleted();
                            return;
                        }
                        var part = selectedParts.get(partIndex);
                        currentSelector = part.getEntry().getSelector();
                        currentBytes = DocumentReads.guarded(() -> loadAndVerifyPart(part));
                        currentOffset = 0;
                    }
                    int remaining = currentBytes.size() - currentOffset;
                    int chunkLen = Math.min(CHUNK_SIZE, remaining);
                    boolean isLast = (currentOffset + chunkLen) >= currentBytes.size();
                    var chunk = currentBytes.substring(currentOffset, currentOffset + chunkLen);
                    var partData = PartData.newBuilder()
                            .setSelector(currentSelector)
                            .setOffset(currentOffset)
                            .setData(chunk)
                            .setLast(isLast)
                            .build();
                    currentOffset += chunkLen;
                    if (isLast) {
                        currentBytes = null;
                        currentSelector = null;
                        partIndex++;
                    }
                    observer.onNext(ReadDocumentResponse.newBuilder().setPartData(partData).build());
                }
            } catch (AccessionException failure) {
                completed = true;
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(failure));
                }
            } catch (RuntimeException failure) {
                completed = true;
                if (!cancelled) {
                    observer.onError(Status.INTERNAL
                            .withDescription("Repository failure")
                            .withCause(failure)
                            .asRuntimeException());
                }
            } finally {
                pumping = false;
            }
        }

        private ByteString loadAndVerifyPart(StoredPartEntry part) {
            var entry = part.getEntry();
            var bytes = reads.read(part.getRef());
            if (!entry.getOpaque()) {
                reads.decodeRetained(stored, entry.getSelector(), bytes);
            }
            if (entry.getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS) {
                bytes = RawBlobs.withClaimChecks(bytes, part.getBlobRefsList());
            }
            return bytes;
        }
    }

    private static final class BlobStreamSession {
        private final StreamObserver<ReadBlobResponse> observer;
        private final ServerCallStreamObserver<ReadBlobResponse> serverObserver;
        private final BlobHeader header;
        private final InputStream stream;
        private final byte[] buffer = new byte[CHUNK_SIZE];

        private volatile boolean cancelled;
        private boolean completed;
        private boolean pumping;
        private boolean headerSent;

        BlobStreamSession(
                StreamObserver<ReadBlobResponse> observer,
                ServerCallStreamObserver<ReadBlobResponse> serverObserver,
                BlobHeader header,
                InputStream stream) {
            this.observer = observer;
            this.serverObserver = serverObserver;
            this.header = header;
            this.stream = stream;
        }

        void cancel() {
            cancelled = true;
            closeQuietly();
        }

        synchronized void pump() {
            if (pumping || cancelled || completed) return;
            pumping = true;
            try {
                while (!cancelled && !completed && (serverObserver == null || serverObserver.isReady())) {
                    if (!headerSent) {
                        headerSent = true;
                        observer.onNext(ReadBlobResponse.newBuilder().setHeader(header).build());
                        continue;
                    }
                    int read = readChunk(stream, buffer);
                    if (cancelled) {
                        closeQuietly();
                        return;
                    }
                    if (read < 0) {
                        completed = true;
                        closeQuietly();
                        observer.onCompleted();
                        return;
                    }
                    observer.onNext(ReadBlobResponse.newBuilder()
                            .setData(ByteString.copyFrom(buffer, 0, read))
                            .build());
                }
            } catch (StackDataLoss loss) {
                completed = true;
                closeQuietly();
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(
                            new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss)));
                }
            } catch (IOException failure) {
                completed = true;
                closeQuietly();
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(
                            new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure)));
                }
            } catch (AccessionException failure) {
                completed = true;
                closeQuietly();
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(failure));
                }
            } catch (RuntimeException failure) {
                completed = true;
                closeQuietly();
                if (!cancelled) {
                    observer.onError(Status.INTERNAL
                            .withDescription("Repository failure")
                            .withCause(failure)
                            .asRuntimeException());
                }
            } finally {
                pumping = false;
            }
        }

        private static int readChunk(InputStream stream, byte[] buffer) throws IOException {
            int total = 0;
            while (total < buffer.length) {
                int n = stream.read(buffer, total, buffer.length - total);
                if (n < 0) {
                    return total == 0 ? -1 : total;
                }
                total += n;
            }
            return total;
        }

        private void closeQuietly() {
            try {
                stream.close();
            } catch (IOException ignored) {
                // Best-effort close on stream completion or cancellation.
            }
        }
    }
}

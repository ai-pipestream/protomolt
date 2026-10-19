package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v2.BlobHeader;
import ai.protomolt.proto.repo.v2.BlobObjectRef;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartData;
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
    private static final System.Logger LOG = System.getLogger(DocumentStreams.class.getName());

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
        try {
            if (serverObserver != null) {
                serverObserver.setOnCancelHandler(session::cancel);
                serverObserver.setOnReadyHandler(session::pump);
            }
            session.pump();
        } catch (Throwable failure) {
            try {
                session.cancel(failure);
            } catch (Throwable cleanupFailure) {
                if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
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
        try {
            if (serverObserver != null) {
                serverObserver.setOnCancelHandler(session::cancel);
                serverObserver.setOnReadyHandler(session::pump);
            }
            session.pump();
        } catch (Throwable failure) {
            try {
                session.cancel(failure);
            } catch (Throwable cleanupFailure) {
                if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
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
        final RawBlobs.Metadata metadata;
        try (var ignored = Objects.requireNonNull(
                reads.reservations().reserve(blobsPart.getRef().getLength()), "reservation lease")) {
            var blobsFragment = reads.read(blobsPart.getRef());
            metadata = RawBlobs.metadata(blobsFragment, request.getBlobId());
        }
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
        private final byte[] buffer = new byte[CHUNK_SIZE];
        private final Object stateLock = new Object();

        private volatile boolean cancelled;
        private volatile InputStream activeStream;
        private boolean completed;
        private boolean pumping;
        private boolean inPump;
        private boolean manifestSent;
        private int partIndex;
        private StoredPartEntry currentPart;
        private ByteString.Output currentPartAccumulator;
        private DocumentAdmissionReservations.Lease currentLease;
        private long currentOffset;
        private int lookahead = -1;

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
            cancel(null);
        }

        void cancel(Throwable failure) {
            cancelled = true;
            var stream = activeStream;
            activeStream = null;
            if (failure == null) closeOnCancel(stream);
            else closeOnFailure(stream, failure);
            DocumentAdmissionReservations.Lease leaseToRelease = null;
            synchronized (stateLock) {
                cancelled = true;
                if (!inPump) {
                    currentPart = null;
                    currentPartAccumulator = null;
                    leaseToRelease = currentLease;
                    currentLease = null;
                }
            }
            if (leaseToRelease != null) {
                leaseToRelease.close();
            }
        }

        synchronized void pump() {
            synchronized (stateLock) {
                if (pumping || cancelled || completed) return;
                pumping = true;
                inPump = true;
            }
            try {
                while (!cancelled && !completed && (serverObserver == null || serverObserver.isReady())) {
                    if (!manifestSent) {
                        manifestSent = true;
                        observer.onNext(ReadDocumentResponse.newBuilder().setManifest(manifest).build());
                        continue;
                    }
                    if (currentPart == null) {
                        if (partIndex >= selectedParts.size()) {
                            completed = true;
                            observer.onNext(ReadDocumentResponse.newBuilder()
                                    .setComplete(ReadComplete.getDefaultInstance())
                                    .build());
                            observer.onCompleted();
                            return;
                        }
                        var nextPart = selectedParts.get(partIndex);
                        DocumentReads.guarded(() -> {
                            openPart(nextPart);
                            return null;
                        });
                        if (cancelled || currentPart == null) {
                            var s = activeStream;
                            activeStream = null;
                            closeOnCancel(s);
                            releaseLease();
                            return;
                        }
                    }

                    var stream = activeStream;
                    if (stream == null || cancelled) {
                        activeStream = null;
                        closeOnCancel(stream);
                        releaseLease();
                        return;
                    }
                    int chunkLen = readNextPartChunk(stream, buffer);
                    if (cancelled) {
                        activeStream = null;
                        closeOnCancel(stream);
                        releaseLease();
                        return;
                    }
                    synchronized (stateLock) {
                        if (cancelled) {
                            activeStream = null;
                            closeOnCancel(stream);
                        } else if (currentPartAccumulator != null && chunkLen > 0) {
                            currentPartAccumulator.write(buffer, 0, chunkLen);
                        }
                    }
                    if (cancelled) {
                        releaseLease();
                        return;
                    }
                    boolean isLast = lookahead < 0;
                    var selector = currentPart.getEntry().getSelector();
                    long offset = currentOffset;
                    currentOffset += chunkLen;
                    if (isLast) {
                        activeStream = null;
                        stream.close();
                        if (!currentPart.getEntry().getOpaque()) {
                            final ByteString bytes;
                            synchronized (stateLock) {
                                bytes = currentPartAccumulator.toByteString();
                                currentPartAccumulator = null;
                            }
                            try {
                                DocumentReads.guarded(() -> {
                                    reads.decodeRetained(stored, selector, bytes);
                                    return null;
                                });
                            } finally {
                                releaseLease();
                            }
                        }
                        synchronized (stateLock) {
                            currentPart = null;
                        }
                        partIndex++;
                    }
                    var partData = PartData.newBuilder()
                            .setSelector(selector)
                            .setOffset(offset)
                            .setData(ByteString.copyFrom(buffer, 0, chunkLen))
                            .setLast(isLast)
                            .build();
                    observer.onNext(ReadDocumentResponse.newBuilder().setPartData(partData).build());
                }
            } catch (StackDataLoss loss) {
                failWith(loss, AccessionGrpcService.toStatus(
                        new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss)));
            } catch (IOException failure) {
                failWith(failure, AccessionGrpcService.toStatus(
                        new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure)));
            } catch (AccessionException failure) {
                failWith(failure, AccessionGrpcService.toStatus(failure));
            } catch (RuntimeException failure) {
                failWith(failure, Status.INTERNAL
                        .withDescription("Repository failure")
                        .withCause(failure)
                        .asRuntimeException());
            } finally {
                DocumentAdmissionReservations.Lease leaseToRelease = null;
                InputStream streamToClose = null;
                synchronized (stateLock) {
                    pumping = false;
                    inPump = false;
                    if (cancelled || completed) {
                        streamToClose = activeStream;
                        activeStream = null;
                        currentPart = null;
                        currentPartAccumulator = null;
                        leaseToRelease = currentLease;
                        currentLease = null;
                    }
                }
                closeOnCancel(streamToClose);
                if (leaseToRelease != null) {
                    leaseToRelease.close();
                }
            }
        }

        private void openPart(StoredPartEntry part) {
            var entry = part.getEntry();
            var ref = part.getRef();
            if (!entry.getOpaque()) {
                var lease = Objects.requireNonNull(
                        reads.reservations().reserve(ref.getLength()), "reservation lease");
                synchronized (stateLock) {
                    if (cancelled) {
                        lease.close();
                        return;
                    }
                    currentLease = lease;
                    currentPartAccumulator = ByteString.newOutput(
                            (int) Math.min(ref.getLength(), Integer.MAX_VALUE));
                }
            } else {
                synchronized (stateLock) {
                    if (cancelled) {
                        return;
                    }
                    currentPartAccumulator = null;
                }
            }
            boolean opened = false;
            InputStream stream = null;
            try {
                stream = reads.drives().byId(ref.getStackId()).open(ref);
                activeStream = stream;
                if (cancelled) {
                    activeStream = null;
                    closeOnCancel(stream);
                    return;
                }
                int firstByte = stream.read();
                synchronized (stateLock) {
                    if (cancelled) {
                        activeStream = null;
                    } else {
                        currentPart = part;
                        currentOffset = 0L;
                        lookahead = firstByte;
                        opened = true;
                    }
                }
                if (!opened) {
                    closeOnCancel(stream);
                }
            } catch (StackDataLoss loss) {
                closeOnFailure(stream, loss);
                throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
            } catch (IOException failure) {
                closeOnFailure(stream, failure);
                throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
            } finally {
                if (!opened) {
                    activeStream = null;
                    releaseLease();
                }
            }
        }

        private int readNextPartChunk(InputStream stream, byte[] buf) throws IOException {
            int total = 0;
            if (lookahead >= 0) {
                buf[0] = (byte) lookahead;
                total = 1;
                lookahead = -1;
            }
            while (total < buf.length && !cancelled) {
                int n = stream.read(buf, total, buf.length - total);
                if (n < 0) {
                    lookahead = -1;
                    return total;
                }
                total += n;
            }
            if (!cancelled) {
                lookahead = stream.read();
            }
            return total;
        }

        private void failWith(Throwable primary, RuntimeException statusError) {
            completed = true;
            var stream = activeStream;
            activeStream = null;
            closeOnFailure(stream, primary);
            releaseLease();
            if (!cancelled) {
                observer.onError(statusError);
            }
        }

        private void releaseLease() {
            final DocumentAdmissionReservations.Lease lease;
            synchronized (stateLock) {
                currentPart = null;
                currentPartAccumulator = null;
                lease = currentLease;
                currentLease = null;
            }
            if (lease != null) {
                lease.close();
            }
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
            cancel(null);
        }

        void cancel(Throwable failure) {
            cancelled = true;
            if (failure == null) closeOnCancel(stream);
            else closeOnFailure(stream, failure);
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
                        closeOnCancel(stream);
                        return;
                    }
                    if (read < 0) {
                        completed = true;
                        stream.close();
                        observer.onCompleted();
                        return;
                    }
                    observer.onNext(ReadBlobResponse.newBuilder()
                            .setData(ByteString.copyFrom(buffer, 0, read))
                            .build());
                }
            } catch (StackDataLoss loss) {
                completed = true;
                closeOnFailure(stream, loss);
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(
                            new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss)));
                }
            } catch (IOException failure) {
                completed = true;
                closeOnFailure(stream, failure);
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(
                            new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure)));
                }
            } catch (AccessionException failure) {
                completed = true;
                closeOnFailure(stream, failure);
                if (!cancelled) {
                    observer.onError(AccessionGrpcService.toStatus(failure));
                }
            } catch (RuntimeException failure) {
                completed = true;
                closeOnFailure(stream, failure);
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
    }

    private static void closeOnFailure(InputStream stream, Throwable primary) {
        if (stream == null) return;
        try {
            stream.close();
        } catch (IOException | RuntimeException closeFailure) {
            if (primary != null && primary != closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }
    }

    private static void closeOnCancel(InputStream stream) {
        if (stream == null) return;
        try {
            stream.close();
        } catch (IOException | RuntimeException closeFailure) {
            LOG.log(
                    System.Logger.Level.WARNING,
                    "Failed to close active stack stream during client cancellation",
                    closeFailure);
        }
    }
}

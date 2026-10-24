package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
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
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Server-streaming read handlers for {@code ReadDocument} and {@code ReadBlob}
 * (requirements §6, §8, §15.10, §15.11, §15.12).
 * <p>
 * Both streaming reads resolve and pin the target revision once before sending the
 * first response frame, enforce gRPC outbound flow control via {@link ServerCallStreamObserver},
 * and close underlying stack streams immediately on client cancellation.
 * <p>
 * {@code ReadDocument} buffers each selected non-opaque part in full, verifies its
 * stack digest and retained schemas, and only then emits the first frame of that part,
 * so a corrupt typed part fails the stream without any of its bytes reaching the
 * client, matching the frame contract documented on
 * {@link DocumentReads#readDocument(RepositoryCaller, ReadDocumentRequest, java.util.function.Consumer)}.
 * The per-part buffer is bounded by an admission reservation taken before the part is
 * read and held until the part's final {@link PartData} frame callback returns (after
 * {@code onNext} returns, any remaining transport buffering is governed by gRPC flow
 * control). An opaque part has no retained-schema check and may far exceed what a host
 * wants to buffer, so it streams chunk by chunk under a fixed two-chunk reservation;
 * the stack stream verifies length and digest at end of stream, reached before the
 * part's final chunk is emitted, so a corrupt opaque part terminates the stream with
 * an error and never delivers its {@code last} frame or {@link ReadComplete}.
 * {@code ReadBlob} streams raw bytes as they arrive and verifies the digest at end of
 * stream, since a raw blob may far exceed what a host can buffer.
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
        private final Object stateLock = new Object();
        private final ReentrantLock pumpLock = new ReentrantLock();

        private volatile boolean cancelled;
        private volatile InputStream activeStream;
        private boolean completed;
        private boolean pumping;
        private boolean inPump;
        private boolean manifestSent;
        private int partIndex;
        private PartSelector currentSelector;
        private ByteString currentBytes;
        private DocumentAdmissionReservations.Lease currentLease;
        private long currentOffset;
        // Streaming state of an opaque part: the open stack stream, the chunk waiting to
        // be emitted, and the one-chunk lookahead that detects end of stream (where the
        // stack verifies length and digest) before the final chunk goes out. The two
        // buffers are allocated for the part under its two-chunk reservation and discarded
        // before that reservation is released.
        private InputStream opaqueStream;
        private byte[] pendingChunk;
        private byte[] lookaheadChunk;
        private int pendingLength = -1;
        private boolean opaqueEof;

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
                    currentSelector = null;
                    currentBytes = null;
                    opaqueStream = null;
                    pendingChunk = null;
                    lookaheadChunk = null;
                    pendingLength = -1;
                    opaqueEof = false;
                    leaseToRelease = currentLease;
                    currentLease = null;
                }
            }
            if (leaseToRelease != null) {
                leaseToRelease.close();
            }
        }

        void pump() {
            pumpLock.lock();
            try {
                pumpLocked();
            } finally {
                pumpLock.unlock();
            }
        }

        private void pumpLocked() {
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
                    if (currentBytes == null && pendingLength < 0) {
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
                            loadPart(nextPart);
                            return null;
                        });
                        if (cancelled || (currentBytes == null && pendingLength < 0)) {
                            releaseLease();
                            return;
                        }
                    }

                    if (currentBytes != null) {
                        int length = (int) Math.min(CHUNK_SIZE, currentBytes.size() - currentOffset);
                        boolean isLast = currentOffset + length == currentBytes.size();
                        var partData = PartData.newBuilder()
                                .setSelector(currentSelector)
                                .setOffset(currentOffset)
                                .setData(currentBytes.substring((int) currentOffset, (int) currentOffset + length))
                                .setLast(isLast)
                                .build();
                        currentOffset += length;
                        observer.onNext(ReadDocumentResponse.newBuilder().setPartData(partData).build());
                        if (isLast) {
                            releaseLease();
                            partIndex++;
                        }
                    } else {
                        emitOpaqueChunk();
                        if (cancelled) {
                            releaseLease();
                            return;
                        }
                    }
                }
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
                        currentSelector = null;
                        currentBytes = null;
                        opaqueStream = null;
                        pendingChunk = null;
                        lookaheadChunk = null;
                        pendingLength = -1;
                        opaqueEof = false;
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

        /**
         * Prepares one part for emission. A non-opaque part is read in full and verified
         * before any of its frames go out: the stack stream checks length and digest at
         * end of stream, and the part is then decoded against its retained schemas. Its
         * catalog length is checked against the part ceiling (negative is DATA_LOSS,
         * above {@code maxFragmentBytes} is RESOURCE_EXHAUSTED) and then reserved before
         * the stack is opened; the part is buffered in one array of exactly that length,
         * and the reservation is held until the part's final frame callback returns. A
         * refused reservation fails the read before any buffering.
         * An opaque part has no retained-schema check, so it streams chunk by chunk
         * under a fixed two-chunk reservation covering its pending and lookahead chunk
         * buffers; the stack stream verifies length and digest at end of stream, which
         * the one-chunk lookahead reaches before the part's final chunk is emitted.
         */
        private void loadPart(StoredPartEntry part) {
            var entry = part.getEntry();
            var ref = part.getRef();
            boolean opaque = entry.getOpaque();
            if (!opaque) reads.requirePartWithinCeiling(ref);
            long reserved = opaque ? 2L * CHUNK_SIZE : ref.getLength();
            var lease = Objects.requireNonNull(
                    reads.reservations().reserve(reserved), "reservation lease");
            synchronized (stateLock) {
                if (cancelled) {
                    lease.close();
                    return;
                }
                currentLease = lease;
            }
            if (opaque) {
                openOpaquePart(entry.getSelector(), ref);
                return;
            }
            InputStream stream = null;
            final ByteString bytes;
            try {
                stream = reads.drives().byId(ref.getStackId()).open(ref);
                activeStream = stream;
                if (cancelled) {
                    activeStream = null;
                    closeOnCancel(stream);
                    return;
                }
                bytes = readFully(stream, ref.getLength());
                activeStream = null;
                if (cancelled) {
                    closeOnCancel(stream);
                    return;
                }
                stream.close();
            } catch (StackDataLoss loss) {
                activeStream = null;
                closeOnFailure(stream, loss);
                throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
            } catch (IOException failure) {
                activeStream = null;
                closeOnFailure(stream, failure);
                throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
            }
            if (cancelled) return;
            reads.decodeRetained(stored, entry.getSelector(), bytes);
            synchronized (stateLock) {
                if (cancelled) return;
                currentSelector = entry.getSelector();
                currentBytes = bytes;
                currentOffset = 0L;
            }
        }

        /** Opens an opaque part's stack stream and reads its first chunk, without buffering the part. */
        private void openOpaquePart(PartSelector selector, StackRef ref) {
            pendingChunk = new byte[CHUNK_SIZE];
            lookaheadChunk = new byte[CHUNK_SIZE];
            InputStream stream = null;
            final int first;
            try {
                stream = reads.drives().byId(ref.getStackId()).open(ref);
                activeStream = stream;
                if (cancelled) {
                    activeStream = null;
                    closeOnCancel(stream);
                    return;
                }
                first = readChunk(stream, pendingChunk);
                if (cancelled) {
                    activeStream = null;
                    closeOnCancel(stream);
                    return;
                }
                if (first < 0) {
                    // Empty part: end of stream already verified length and digest.
                    activeStream = null;
                    stream.close();
                    stream = null;
                }
            } catch (StackDataLoss loss) {
                activeStream = null;
                closeOnFailure(stream, loss);
                throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
            } catch (IOException failure) {
                activeStream = null;
                closeOnFailure(stream, failure);
                throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
            }
            synchronized (stateLock) {
                if (cancelled) {
                    var open = stream;
                    activeStream = null;
                    closeOnCancel(open);
                    return;
                }
                currentSelector = selector;
                currentOffset = 0L;
                opaqueStream = stream;
                pendingLength = Math.max(first, 0);
                opaqueEof = first < 0;
            }
        }

        /**
         * Emits the pending chunk of the streaming opaque part, after reading one chunk
         * ahead: end of stream, where the stack verifies length and digest, is therefore
         * reached before the final chunk is emitted, so a corrupt part fails the stream
         * without its {@code last} frame ever going out.
         */
        private void emitOpaqueChunk() {
            if (!opaqueEof) {
                var stream = opaqueStream;
                final int next;
                try {
                    next = readChunk(stream, lookaheadChunk);
                    if (next < 0) {
                        activeStream = null;
                        opaqueStream = null;
                        opaqueEof = true;
                        stream.close();
                    }
                } catch (StackDataLoss loss) {
                    activeStream = null;
                    opaqueStream = null;
                    closeOnFailure(stream, loss);
                    throw new AccessionException(Status.Code.DATA_LOSS, loss.getMessage(), null, loss);
                } catch (IOException failure) {
                    activeStream = null;
                    opaqueStream = null;
                    closeOnFailure(stream, failure);
                    throw new AccessionException(Status.Code.UNAVAILABLE, "Stack read failed", null, failure);
                }
                if (cancelled) return;
                var partData = PartData.newBuilder()
                        .setSelector(currentSelector)
                        .setOffset(currentOffset)
                        .setData(ByteString.copyFrom(pendingChunk, 0, pendingLength))
                        .setLast(opaqueEof)
                        .build();
                currentOffset += pendingLength;
                if (opaqueEof) {
                    pendingLength = -1;
                } else {
                    var emitted = pendingChunk;
                    pendingChunk = lookaheadChunk;
                    lookaheadChunk = emitted;
                    pendingLength = next;
                }
                observer.onNext(ReadDocumentResponse.newBuilder().setPartData(partData).build());
                if (partData.getLast()) {
                    releaseLease();
                    opaqueEof = false;
                    partIndex++;
                }
                return;
            }
            // Empty part verified at end of stream: one empty frame carries last = true.
            var partData = PartData.newBuilder()
                    .setSelector(currentSelector)
                    .setOffset(currentOffset)
                    .setData(ByteString.empty())
                    .setLast(true)
                    .build();
            pendingLength = -1;
            opaqueEof = false;
            observer.onNext(ReadDocumentResponse.newBuilder().setPartData(partData).build());
            releaseLease();
            partIndex++;
        }

        /**
         * Reads the stream into a single array of exactly {@code expectedLength} bytes,
         * the catalog length {@code loadPart} checked against the part ceiling and
         * reserved before this method runs, so the read allocates no scratch chunk and no
         * growable output beyond the reserved bytes. The array fills at most one chunk
         * per read call so cancellation is checked between reads, and the returned
         * {@link ByteString} wraps the array without copying it. Reading to end of
         * stream lets the verifying stack stream check length and digest; a stream that
         * ends early or runs past {@code expectedLength} is a {@link StackDataLoss}.
         * A cancelled read returns an empty string that the caller discards.
         */
        private ByteString readFully(InputStream stream, long expectedLength) throws IOException {
            var bytes = new byte[Math.toIntExact(expectedLength)];
            int filled = 0;
            while (!cancelled && filled < bytes.length) {
                int n = stream.read(bytes, filled, Math.min(CHUNK_SIZE, bytes.length - filled));
                if (n < 0) break;
                filled += n;
            }
            if (cancelled) return ByteString.EMPTY;
            if (filled != bytes.length || stream.read() >= 0)
                throw new StackDataLoss("Stored object length differs from its reference: "
                        + expectedLength + " bytes");
            return UnsafeByteOperations.unsafeWrap(bytes);
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
                currentSelector = null;
                currentBytes = null;
                opaqueStream = null;
                pendingChunk = null;
                lookaheadChunk = null;
                pendingLength = -1;
                opaqueEof = false;
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
        private final ReentrantLock pumpLock = new ReentrantLock();

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

        void pump() {
            pumpLock.lock();
            try {
                pumpLocked();
            } finally {
                pumpLock.unlock();
            }
        }

        private void pumpLocked() {
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
    }

    /**
     * Reads up to one full buffer, returning {@code -1} only at end of stream with no
     * bytes read. End of stream after a partial buffer returns the partial count; the
     * next call returns {@code -1}.
     */
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

package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.engine.ArchiveOperations;

import ai.protomolt.proto.repo.archive.v1.ArchiveServiceGrpc;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryResponse;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static ai.protomolt.proto.repo.service.GrpcErrors.invalidArgument;

/**
 * The archive's gRPC transport. Unary RPCs delegate straight to
 * {@link ArchiveOperations}; the client-streaming upload bridges the
 * observer's push-delivered chunks onto a blocking {@link InputStream} that
 * a virtual thread streams into the object store, so the bytes flow through
 * the same verified, content-addressed landing as every other door.
 */
final class ArchiveGrpcService extends ArchiveServiceGrpc.ArchiveServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveGrpcService.class);

    /** Bounded chunk buffer between the transport and the store writer. */
    private static final int CHUNK_QUEUE_DEPTH = 64;

    /** Largest data frame of a streamed rendition read (64 KiB). */
    static final int READ_FRAME_BYTES = 64 * 1024;

    /**
     * What one open rendition read reserves on the host read budget: the frame it
     * fills plus one frame the transport may still hold while it reads the next.
     */
    static final long READ_WINDOW_BYTES = 2L * READ_FRAME_BYTES;

    private final ArchiveRepository operations;
    private final RepoCallerBindings callers;
    private final DocumentAdmissionReservations readBudget;

    /**
     * Binds no scoped caller to any account: only process authority reaches archives.
     *
     * @param operations the archive port
     * @param readBudget the host read byte budget streamed rendition reads reserve their window on
     */
    ArchiveGrpcService(ArchiveRepository operations, DocumentAdmissionReservations readBudget) {
        this(operations, new RepoCallerBindings(), readBudget);
    }

    /**
     * Binds scoped callers through the host's account bindings.
     *
     * @param operations the archive port
     * @param callers the host's account bindings
     * @param readBudget the host read byte budget streamed rendition reads reserve their window on
     */
    ArchiveGrpcService(ArchiveRepository operations, RepoCallerBindings callers,
            DocumentAdmissionReservations readBudget) {
        this.operations = Objects.requireNonNull(operations, "operations");
        this.callers = Objects.requireNonNull(callers, "callers");
        this.readBudget = Objects.requireNonNull(readBudget, "readBudget");
    }

    private RepositoryCaller caller() {
        return callers.current();
    }

    @Override
    public void createArchive(CreateArchiveRequest request,
                              StreamObserver<CreateArchiveResponse> observer) {
        GrpcErrors.run(observer, () -> operations.createArchive(caller(), request));
    }

    @Override
    public void getArchive(GetArchiveRequest request,
                           StreamObserver<GetArchiveResponse> observer) {
        GrpcErrors.run(observer, () -> operations.getArchive(caller(), request));
    }

    @Override
    public void listArchives(ListArchivesRequest request,
                             StreamObserver<ListArchivesResponse> observer) {
        GrpcErrors.run(observer, () -> operations.listArchives(caller(), request));
    }

    @Override
    public void putEntry(PutEntryRequest request,
                         StreamObserver<PutEntryResponse> observer) {
        GrpcErrors.run(observer, () -> operations.putEntry(caller(), request));
    }

    @Override
    public void getEntry(GetEntryRequest request,
                         StreamObserver<GetEntryResponse> observer) {
        GrpcErrors.run(observer, () -> operations.getEntry(caller(), request));
    }

    /**
     * Streams one rendition: a header frame, then data frames of at most
     * {@link #READ_FRAME_BYTES}, sent only while the transport is ready, so a slow
     * client stalls the provider read instead of queueing the object in memory. The
     * read reserves {@link #READ_WINDOW_BYTES} on the host read budget before the
     * store is touched and releases it when the stream ends on any path.
     */
    @Override
    public void readRendition(ReadRenditionRequest request, StreamObserver<ReadRenditionResponse> observer) {
        final DocumentAdmissionReservations.Lease lease;
        final ArchiveRepository.RenditionStream opened;
        try {
            lease = Objects.requireNonNull(readBudget.reserve(READ_WINDOW_BYTES), "reservation lease");
        } catch (Throwable failure) {
            observer.onError(GrpcErrors.map(failure));
            return;
        }
        try {
            opened = operations.readRendition(caller(), request);
        } catch (Throwable failure) {
            lease.close();
            observer.onError(GrpcErrors.map(failure));
            return;
        }
        var header = RenditionReadHeader.newBuilder().setVersion(opened.version())
                .setRendition(opened.rendition().getRendition()).setSizeBytes(opened.rendition().getSizeBytes())
                .setSha256(opened.rendition().getSha256()).setOffset(opened.offset()).setLength(opened.length())
                .setWholeObject(opened.wholeObject()).build();
        var server = observer instanceof ServerCallStreamObserver<ReadRenditionResponse> call ? call : null;
        var session = new RenditionReadSession(observer, server, header, opened.body(), lease);
        if (server != null) {
            server.setOnCancelHandler(session::cancel);
            server.setOnReadyHandler(session::pump);
        }
        session.pump();
    }

    /**
     * One rendition read's frames, pumped while the transport is ready. Cancellation
     * stops the pump after the frame in flight; the provider stream, its read pin and
     * the budget window are released once, by whichever side finishes last, so the
     * pin is never released under a provider read still in progress.
     */
    private static final class RenditionReadSession {
        private final StreamObserver<ReadRenditionResponse> observer;
        private final ServerCallStreamObserver<ReadRenditionResponse> server;
        private final RenditionReadHeader header;
        private final InputStream body;
        private final DocumentAdmissionReservations.Lease lease;
        private final byte[] frame = new byte[READ_FRAME_BYTES];
        private final Object state = new Object();
        private volatile boolean cancelled;
        private boolean finished;
        private boolean pumping;
        private boolean headerSent;

        RenditionReadSession(StreamObserver<ReadRenditionResponse> observer,
                ServerCallStreamObserver<ReadRenditionResponse> server, RenditionReadHeader header,
                InputStream body, DocumentAdmissionReservations.Lease lease) {
            this.observer = observer;
            this.server = server;
            this.header = header;
            this.body = body;
            this.lease = lease;
        }

        void cancel() {
            cancelled = true;
            synchronized (state) {
                if (!pumping) release(null);
            }
        }

        void pump() {
            synchronized (state) {
                if (pumping || finished) return;
                pumping = true;
            }
            try {
                while (!cancelled && (server == null || server.isReady())) {
                    if (!headerSent) {
                        headerSent = true;
                        observer.onNext(ReadRenditionResponse.newBuilder().setHeader(header).build());
                        continue;
                    }
                    int read = fill();
                    if (read < 0) {
                        synchronized (state) { release(null); }
                        observer.onCompleted();
                        return;
                    }
                    observer.onNext(ReadRenditionResponse.newBuilder()
                            .setData(ByteString.copyFrom(frame, 0, read)).build());
                }
            } catch (IOException failure) {
                fail(failure, Status.UNAVAILABLE.withDescription("Archive object read failed")
                        .withCause(failure).asRuntimeException());
            } catch (RuntimeException failure) {
                fail(failure, GrpcErrors.map(failure));
            } finally {
                synchronized (state) {
                    pumping = false;
                    if (cancelled) release(null);
                }
            }
        }

        /** Reads up to one full frame; -1 only at a verified end with nothing read. */
        private int fill() throws IOException {
            int total = 0;
            while (total < frame.length) {
                int read = body.read(frame, total, frame.length - total);
                if (read < 0) return total == 0 ? -1 : total;
                total += read;
            }
            return total;
        }

        private void fail(Throwable failure, Throwable status) {
            synchronized (state) { release(failure); }
            if (!cancelled) observer.onError(status);
        }

        /** Closes the provider stream and returns the window, once; the caller holds {@code state}. */
        private void release(Throwable primary) {
            if (finished) return;
            finished = true;
            try {
                body.close();
            } catch (IOException | RuntimeException closeFailure) {
                if (primary != null) primary.addSuppressed(closeFailure);
                else LOG.warn("Closing a rendition read failed", closeFailure);
            } finally {
                lease.close();
            }
        }
    }

    @Override
    public void getEntryManifest(GetEntryManifestRequest request,
                                 StreamObserver<GetEntryManifestResponse> observer) {
        GrpcErrors.run(observer, () -> operations.getManifest(caller(), request));
    }

    @Override
    public void listEntries(ListEntriesRequest request,
                            StreamObserver<ListEntriesResponse> observer) {
        GrpcErrors.run(observer, () -> operations.listEntries(caller(), request));
    }

    @Override
    public void listVersions(ListVersionsRequest request,
                             StreamObserver<ListVersionsResponse> observer) {
        GrpcErrors.run(observer, () -> operations.listVersions(caller(), request));
    }

    @Override
    public void getArchiveStats(GetArchiveStatsRequest request,
                                StreamObserver<GetArchiveStatsResponse> observer) {
        GrpcErrors.run(observer, () -> operations.stats(caller(), request));
    }

    @Override
    public void classifyEntry(ClassifyEntryRequest request,
                              StreamObserver<ClassifyEntryResponse> observer) {
        GrpcErrors.run(observer, () -> operations.classifyEntry(caller(), request));
    }

    @Override
    public void bridgeEntry(BridgeEntryRequest request,
                            StreamObserver<BridgeEntryResponse> observer) {
        GrpcErrors.run(observer, () -> operations.bridgeEntry(caller(), request));
    }

    @Override
    public StreamObserver<UploadRenditionRequest> uploadRendition(
            StreamObserver<UploadRenditionResponse> observer) {
        RepositoryCaller caller = caller();
        return new StreamObserver<>() {
            private final ChunkStream chunks = new ChunkStream();
            private final java.util.concurrent.atomic.AtomicBoolean terminal =
                    new java.util.concurrent.atomic.AtomicBoolean();
            private CompletableFuture<ArchiveRepository.UploadResult> landing;
            private long received;
            private long declared;

            @Override
            public void onNext(UploadRenditionRequest frame) {
                if (terminal.get()) return;
                try {
                    if (frame.hasHeader()) {
                        if (landing != null) {
                            throw invalidArgument("the header must be the first and only"
                                    + " header frame");
                        }
                        UploadRenditionHeader header = frame.getHeader();
                        declared = header.getSizeBytes();
                        // The store writer parks on its own virtual thread
                        // while the transport keeps delivering chunks into
                        // the bounded queue.
                        landing = CompletableFuture.supplyAsync(() -> {
                            try {
                                return operations.uploadStream(caller, header.getAddress(),
                                        header.getRendition(), header.getSizeBytes(),
                                        header.getExpectedSha256(),
                                        header.hasWrittenBy() ? header.getWrittenBy() : null,
                                        null,
                                        header.hasDeclared() ? header.getDeclared() : null,
                                        header.hasOrigin() ? header.getOrigin() : null,
                                        chunks);
                            } catch (IOException e) {
                                throw new java.io.UncheckedIOException(e);
                            }
                        }, runnable -> Thread.ofVirtual().start(runnable));
                        landing.whenComplete((result, failure) -> {
                            if (failure != null) fail(failure);
                        });
                        return;
                    }
                    if (landing == null) {
                        throw invalidArgument("the first frame must be the header");
                    }
                    byte[] chunk = frame.getChunk().toByteArray();
                    received += chunk.length;
                    if (received > declared) {
                        throw invalidArgument("the stream delivered " + received
                                + " bytes against a declared size_bytes of " + declared);
                    }
                    chunks.put(chunk);
                } catch (RuntimeException e) {
                    fail(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                terminal.set(true);
                // The client went away mid-stream: abort the store writer so
                // it stops consuming; anything it landed is a staging orphan
                // the reconciler reclaims.
                chunks.abort(new IOException("upload stream cancelled by the client", t));
            }

            @Override
            public void onCompleted() {
                if (terminal.get()) return;
                try {
                    if (landing == null) {
                        throw invalidArgument("the stream carried no header");
                    }
                    if (received != declared) {
                        throw invalidArgument("the stream delivered " + received
                                + " bytes against a declared size_bytes of " + declared);
                    }
                    chunks.finish();
                    ArchiveRepository.UploadResult result = landing.get();
                    if (!terminal.compareAndSet(false, true)) return;
                    observer.onNext(UploadRenditionResponse.newBuilder()
                            .setEntryUuid(result.entryUuid())
                            .setVersion(result.version())
                            .setSha256(result.sha256())
                            .setSizeBytes(result.sizeBytes())
                            .setObjectKey(result.objectKey())
                            .setRootChecksum(result.rootChecksum())
                            .build());
                    observer.onCompleted();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof java.io.UncheckedIOException unchecked) {
                        cause = unchecked.getCause();
                    }
                    fail(cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail(e);
                } catch (RuntimeException e) {
                    fail(e);
                }
            }

            private void fail(Throwable e) {
                while ((e instanceof java.util.concurrent.CompletionException
                        || e instanceof java.io.UncheckedIOException) && e.getCause() != null) {
                    e = e.getCause();
                }
                if (!terminal.compareAndSet(false, true)) return;
                chunks.abort(new IOException("upload refused: " + e.getMessage(), e));
                observer.onError(GrpcErrors.map(e));
            }
        };
    }

    /**
     * The observer→InputStream bridge: the transport puts chunks, the store
     * writer reads them in order. Bounded, so a slow store applies
     * backpressure to the transport instead of buffering the upload.
     */
    private static final class ChunkStream extends InputStream {

        private static final byte[] END = new byte[0];

        private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(CHUNK_QUEUE_DEPTH);
        private volatile IOException aborted;
        private byte[] head = new byte[0];
        private int position;
        private boolean finished;

        void put(byte[] chunk) {
            if (chunk.length == 0) {
                return;
            }
            enqueue(chunk);
        }

        private void enqueue(byte[] chunk) {
            try {
                while (true) {
                    if (aborted != null) throw new java.io.UncheckedIOException(aborted);
                    if (queue.offer(chunk, 50, java.util.concurrent.TimeUnit.MILLISECONDS)) return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted delivering an upload chunk", e);
            }
        }

        void finish() {
            enqueue(END);
        }

        void abort(IOException cause) {
            aborted = cause;
            // Wake a parked reader; END after an abort still reads as failure.
            queue.offer(END);
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (aborted != null) {
                throw aborted;
            }
            if (finished) {
                return -1;
            }
            while (position >= head.length) {
                try {
                    head = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted reading upload chunks", e);
                }
                if (aborted != null) {
                    throw aborted;
                }
                if (head == END) {
                    finished = true;
                    return -1;
                }
                position = 0;
            }
            int n = Math.min(length, head.length - position);
            System.arraycopy(head, position, buffer, offset, n);
            position += n;
            return n;
        }
    }
}

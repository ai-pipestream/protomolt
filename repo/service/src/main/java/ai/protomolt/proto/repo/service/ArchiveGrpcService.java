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
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.InputStream;
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

    /** Bounded chunk buffer between the transport and the store writer. */
    private static final int CHUNK_QUEUE_DEPTH = 64;

    private final ArchiveRepository operations;

    ArchiveGrpcService(ArchiveRepository operations) {
        this.operations = operations;
    }

    private static RepositoryCaller caller() {
        var context = ai.protomolt.proto.authz.grpc.CallerContexts.current();
        return new RepositoryCaller(context.name(), context.unrestricted());
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

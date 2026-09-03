package ai.protomolt.proto.search.service;

import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.protomolt.proto.repo.v1.ListDocumentsRequest;
import ai.protomolt.proto.repo.v1.ListDocumentsResponse;
import ai.protomolt.proto.repo.v1.NodeAddress;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * {@link DocumentFetcher} over the repo wire contract: reads CORE and PARSED
 * parts through the {@code DocumentServiceGrpc} blocking stub. The target is
 * a {@code host:port} authority or {@code inprocess:<name>}. Every call
 * carries the configured deadline, so a hung repository fails the calling
 * workflow step (which requeues with backoff) instead of parking a worker
 * thread forever.
 */
public final class GrpcDocumentFetcher implements DocumentFetcher, DocumentLister, AutoCloseable {

    /** In-process target prefix, shared vocabulary with the composer: {@value}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    /** Call deadline applied when no explicit timeout is given. */
    public static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(30);

    private final ManagedChannel channel;
    private final Duration rpcTimeout;

    /**
     * Opens the repo channel with the {@link #DEFAULT_CALL_TIMEOUT} call
     * deadline.
     *
     * @param target a {@code host:port} authority or {@code inprocess:<name>}
     */
    public GrpcDocumentFetcher(String target) {
        this(target, DEFAULT_CALL_TIMEOUT);
    }

    /**
     * Opens the repo channel.
     *
     * @param target a {@code host:port} authority or {@code inprocess:<name>}
     * @param rpcTimeout per-call deadline; must be positive
     */
    public GrpcDocumentFetcher(String target, Duration rpcTimeout) {
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("target must not be blank");
        }
        if (rpcTimeout == null || rpcTimeout.isZero() || rpcTimeout.isNegative()) {
            throw new IllegalArgumentException("rpcTimeout must be positive");
        }
        this.rpcTimeout = rpcTimeout;
        if (target.startsWith(INPROCESS_TARGET_PREFIX)) {
            this.channel = InProcessChannelBuilder
                    .forName(target.substring(INPROCESS_TARGET_PREFIX.length()))
                    .build();
        } else {
            this.channel = NettyChannelBuilder.forTarget(target).usePlaintext().build();
        }
    }

    @Override
    public Document fetch(NodeAddress address) {
        return stub()
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(address)
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .addParts(DocumentPart.DOCUMENT_PART_PARSED)
                        .build())
                .getDocument();
    }

    @Override
    public ListDocumentsResponse list(ListDocumentsRequest request) {
        return stub().listDocuments(request);
    }

    /** One deadline-carrying stub per call: deadlines are absolute. */
    private DocumentServiceGrpc.DocumentServiceBlockingStub stub() {
        return DocumentServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(rpcTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

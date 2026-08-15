package ai.pipestream.proto.search.door;

import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;

/**
 * {@link DocumentFetcher} over the repo wire contract: reads CORE and PARSED
 * parts through the {@code DocumentServiceGrpc} blocking stub. The target is
 * a {@code host:port} authority or {@code inprocess:<name>}.
 */
public final class GrpcDocumentFetcher implements DocumentFetcher, DocumentLister, AutoCloseable {

    /** In-process target prefix, shared vocabulary with the composer: {@value}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    private final ManagedChannel channel;

    /**
     * Opens the repo channel.
     *
     * @param target a {@code host:port} authority or {@code inprocess:<name>}
     */
    public GrpcDocumentFetcher(String target) {
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("target must not be blank");
        }
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
        return DocumentServiceGrpc.newBlockingStub(channel)
                .getDocumentByReference(GetDocumentByReferenceRequest.newBuilder()
                        .setAddress(address)
                        .addParts(DocumentPart.DOCUMENT_PART_CORE)
                        .addParts(DocumentPart.DOCUMENT_PART_PARSED)
                        .build())
                .getDocument();
    }

    @Override
    public ListDocumentsResponse list(ListDocumentsRequest request) {
        return DocumentServiceGrpc.newBlockingStub(channel).listDocuments(request);
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

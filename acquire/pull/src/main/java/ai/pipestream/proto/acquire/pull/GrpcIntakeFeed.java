package ai.pipestream.proto.acquire.pull;

import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.repo.v1.Document;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link IntakeFeed} over the intake wire contract: the typed-document lane of
 * {@code IntakeService}, authenticated with the connector's API key on every call. The target
 * is a {@code host:port} authority or {@code inprocess:<name>} (the composer's shared
 * vocabulary), so a connector co-mounted with the intake role feeds it without a socket.
 */
public final class GrpcIntakeFeed implements IntakeFeed {

    /** In-process target prefix, shared vocabulary with the composer: {@value}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    private static final Metadata.Key<String> API_KEY_HEADER =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    private final ManagedChannel channel;
    private final IntakeServiceGrpc.IntakeServiceBlockingStub intake;

    /**
     * Opens the intake channel.
     *
     * @param target a {@code host:port} authority or {@code inprocess:<name>}
     * @param apiKey the intake API key the connector's identity rides
     */
    public GrpcIntakeFeed(String target, String apiKey) {
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("target must not be blank");
        }
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("apiKey must not be blank");
        }
        if (target.startsWith(INPROCESS_TARGET_PREFIX)) {
            this.channel = InProcessChannelBuilder
                    .forName(target.substring(INPROCESS_TARGET_PREFIX.length()))
                    .build();
        } else {
            this.channel = NettyChannelBuilder.forTarget(target).usePlaintext().build();
        }
        Metadata credentials = new Metadata();
        credentials.put(API_KEY_HEADER, apiKey);
        this.intake = IntakeServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(credentials));
    }

    @Override
    public IngestDocumentResponse submit(Document document, String datasourceId, String drive,
                                         Map<String, String> metadata) {
        IngestDocumentRequest.Builder request = IngestDocumentRequest.newBuilder()
                .setDocument(document)
                .setDatasourceId(datasourceId)
                .putAllMetadata(metadata);
        if (drive != null && !drive.isBlank()) {
            request.setDrive(drive);
        }
        return intake.ingestDocument(request.build());
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

package ai.protomolt.proto.acquire.pull;

import ai.protomolt.proto.intake.v1.IngestDocumentRequest;
import ai.protomolt.proto.intake.v1.IngestDocumentResponse;
import ai.protomolt.proto.intake.v1.IntakeServiceGrpc;
import ai.protomolt.proto.repo.v1.Document;
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
 *
 * <p>A {@code host:port} target uses TLS with the platform's trust roots. The API key rides
 * every call, so a plaintext channel would put the connector's credential, and the document
 * bodies it carries, on the wire in the clear for anything between the connector and intake.
 * Plaintext remains available for a trusted network or a sidecar that terminates TLS itself,
 * but it is an explicit choice rather than the default. An {@code inprocess:} target has no
 * socket and is unaffected either way.
 */
public final class GrpcIntakeFeed implements IntakeFeed {

    /** In-process target prefix, shared vocabulary with the composer: {@value}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    /**
     * Set to {@code true} to send a {@code host:port} intake target's traffic unencrypted:
     * {@value}. For a trusted network or a sidecar that terminates TLS itself. Any other
     * value, and an unset variable, keep TLS.
     */
    public static final String ENV_PLAINTEXT = "PROTOMOLT_ACQUIRE_INTAKE_PLAINTEXT";

    /** Whether {@link #ENV_PLAINTEXT} in {@code env} opts out of TLS. */
    public static boolean plaintextRequested(Map<String, String> env) {
        return env != null && Boolean.parseBoolean(env.getOrDefault(ENV_PLAINTEXT, "false"));
    }

    private static final Metadata.Key<String> API_KEY_HEADER =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    private final ManagedChannel channel;
    private final IntakeServiceGrpc.IntakeServiceBlockingStub intake;

    /**
     * Opens the intake channel, using TLS for a {@code host:port} target.
     *
     * @param target a {@code host:port} authority or {@code inprocess:<name>}
     * @param apiKey the intake API key the connector's identity rides
     */
    public GrpcIntakeFeed(String target, String apiKey) {
        this(target, apiKey, false);
    }

    /**
     * Opens the intake channel.
     *
     * @param target a {@code host:port} authority or {@code inprocess:<name>}
     * @param apiKey the intake API key the connector's identity rides
     * @param plaintext sends a {@code host:port} target's traffic unencrypted, for a trusted
     *        network or a sidecar that terminates TLS itself. The API key travels on every
     *        call, so this exposes the connector's credential to anything on the path.
     *        Ignored for an {@code inprocess:} target, which has no socket.
     */
    public GrpcIntakeFeed(String target, String apiKey, boolean plaintext) {
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
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget(target);
            this.channel = (plaintext
                    ? builder.usePlaintext()
                    : builder.useTransportSecurity()).build();
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

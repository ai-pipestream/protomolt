package ai.protomolt.proto.parse.grparse;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point: the environment-configured gRParse adapter over
 * Netty. This is the sidecar deployment — run it next to gRParse, save a
 * service profile pointing at it, and the coordinator discovers
 * {@value GrparseParserAdapter#PARSER_NAME} via
 * {@code ParserRegistry.fromProfiles}.
 */
public final class GrparseAdapterMain {

    /** Env var naming the gRParse endpoint ({@code host:port}); required. */
    public static final String ENV_TARGET = "PARSE_GRPARSE_TARGET";

    /** Env var choosing the adapter's gRPC port; defaults to {@value #DEFAULT_PORT}. */
    public static final String ENV_PORT = "PARSE_GRPARSE_PORT";

    /**
     * Env var declaring the gRParse fleet renders page images into the
     * stream ({@code true}/{@code false}, default {@code false}); a
     * deployment fact the adapter advertises as {@code emits_previews}.
     */
    public static final String ENV_EMITS_PREVIEWS = "PARSE_GRPARSE_EMITS_PREVIEWS";

    /** The default adapter gRPC port. */
    public static final int DEFAULT_PORT = 9096;

    private static final Logger LOG = LoggerFactory.getLogger(GrparseAdapterMain.class);

    private GrparseAdapterMain() {
    }

    /**
     * Boots the adapter from the environment.
     *
     * @param args unused; configuration is environment-only
     * @throws Exception when the server cannot start
     */
    public static void main(String[] args) throws Exception {
        String target = System.getenv(ENV_TARGET);
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException(
                    ENV_TARGET + " is required (the gRParse endpoint, host:port)");
        }
        int port = port(System.getenv(ENV_PORT));

        GrparseParserAdapter adapter =
                new GrparseParserAdapter(target.trim(), new GrparseAdapterOptions(
                        GrparseAdapterOptions.DEFAULT_PARSER_VERSION,
                        GrparseAdapterOptions.DEFAULT_MAX_DOCUMENT_BYTES,
                        GrparseAdapterOptions.DEFAULT_DEADLINE,
                        emitsPreviews(System.getenv(ENV_EMITS_PREVIEWS))));
        HealthStatusManager health = new HealthStatusManager();
        Server server = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(adapter)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        LOG.info(
                "grparse-adapter listening on gRPC port {} (gRParse target {})",
                server.getPort(),
                target.trim());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdownNow();
            adapter.close();
        }, "grparse-adapter-shutdown"));
        server.awaitTermination();
    }

    /** Package-private for direct testing of the port refusals. */
    static int port(String spec) {
        if (spec == null || spec.isBlank()) {
            return DEFAULT_PORT;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(spec.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(ENV_PORT + " must be a port number in 0..65535", e);
        }
        if (parsed < 0 || parsed > 65535) {
            throw new IllegalArgumentException(ENV_PORT + " must be a port number in 0..65535");
        }
        return parsed;
    }

    /**
     * The previews flag, strictly {@code true}/{@code false} (case-insensitive); unset or
     * blank is {@code false}. Package-private for direct testing.
     */
    static boolean emitsPreviews(String spec) {
        if (spec == null || spec.isBlank()) {
            return false;
        }
        if (spec.trim().equalsIgnoreCase("true")) {
            return true;
        }
        if (spec.trim().equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException(ENV_EMITS_PREVIEWS + " must be true or false");
    }
}

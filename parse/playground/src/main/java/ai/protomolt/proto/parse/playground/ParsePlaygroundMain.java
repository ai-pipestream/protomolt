package ai.protomolt.proto.parse.playground;

import ai.protomolt.proto.parse.text.TextParserService;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point. Demo mode ({@code --demo}, or no parser target
 * configured) embeds the reference text parser in-process, so
 * {@code java -jar ... --demo} is a complete zero-dependency playground;
 * otherwise {@code PARSE_PLAYGROUND_PARSER_TARGET} names any fleet parser
 * implementing the plugin contract (gRParse included).
 */
public final class ParsePlaygroundMain {

    /** Env var naming the parser endpoint ({@code host:port}). */
    public static final String ENV_PARSER_TARGET = "PARSE_PLAYGROUND_PARSER_TARGET";

    /** Env var for the HTTP port (default 8095). */
    public static final String ENV_HTTP_PORT = "PARSE_PLAYGROUND_HTTP_PORT";

    /** The playground's default HTTP port. */
    public static final int DEFAULT_HTTP_PORT = 8095;

    private static final Logger LOG = LoggerFactory.getLogger(ParsePlaygroundMain.class);

    private ParsePlaygroundMain() {
    }

    public static void main(String[] args) throws Exception {
        boolean demo = args.length > 0 && "--demo".equals(args[0]);
        String target = System.getenv(ENV_PARSER_TARGET);
        Server embedded = null;
        if (demo || target == null || target.isBlank()) {
            String name = "playground-demo-parser";
            embedded = InProcessServerBuilder.forName(name)
                    .addService(new TextParserService())
                    .build()
                    .start();
            target = ParsePlaygroundServer.INPROCESS_TARGET_PREFIX + name;
            LOG.info("demo mode: embedded text parser");
        }
        int port = port(System.getenv(ENV_HTTP_PORT));
        JsonFormat.TypeRegistry registry = JsonFormat.TypeRegistry.newBuilder()
                .add(ai.protomolt.proto.parse.document.v1.Document.getDescriptor())
                .build();
        ParsePlaygroundServer server = new ParsePlaygroundServer(port, target, registry);
        LOG.info("parser playground on http://localhost:{}/ (parser {})", server.port(), target);
        Server finalEmbedded = embedded;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            if (finalEmbedded != null) {
                finalEmbedded.shutdownNow();
            }
        }, "playground-shutdown"));
        Thread.currentThread().join();
    }

    /** Package-private for direct testing of the port refusals. */
    static int port(String spec) {
        if (spec == null || spec.isBlank()) {
            return DEFAULT_HTTP_PORT;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(spec.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(ENV_HTTP_PORT + " must be a port number in 1..65535", e);
        }
        if (parsed < 1 || parsed > 65535) {
            throw new IllegalArgumentException(ENV_HTTP_PORT + " must be a port number in 1..65535");
        }
        return parsed;
    }
}

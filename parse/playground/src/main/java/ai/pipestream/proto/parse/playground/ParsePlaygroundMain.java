package ai.pipestream.proto.parse.playground;

import ai.pipestream.proto.parse.text.TextParserService;
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
        String portEnv = System.getenv(ENV_HTTP_PORT);
        int port = portEnv == null || portEnv.isBlank() ? 8095 : Integer.parseInt(portEnv);
        JsonFormat.TypeRegistry registry = JsonFormat.TypeRegistry.newBuilder()
                .add(ai.pipestream.proto.parse.document.v1.Document.getDescriptor())
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
}

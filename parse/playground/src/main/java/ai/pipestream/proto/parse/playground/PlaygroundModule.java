package ai.pipestream.proto.parse.playground;

import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import com.google.protobuf.util.JsonFormat;

/**
 * The streaming parser playground as a mountable role. The parser it
 * watches resolves through the node's channels, by default the embedded
 * text parser role.
 */
public final class PlaygroundModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "playground";

    /** The parser role the playground watches by default. */
    public static final String DEFAULT_PARSER_ROLE = "parse-text";

    private final int port;
    private final String parserRole;
    private ParsePlaygroundServer server;

    /**
     * Creates the module.
     *
     * @param port the HTTP port (0 for ephemeral)
     * @param parserRole the parser role to watch
     */
    public PlaygroundModule(int port, String parserRole) {
        if (parserRole == null || parserRole.isBlank()) {
            throw new IllegalArgumentException("parserRole must not be blank");
        }
        this.port = port;
        this.parserRole = parserRole;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public ServiceMount wire(NodeContext context) {
        return new ServiceMount() {
            @Override
            public void start() throws Exception {
                server = new ParsePlaygroundServer(
                        port,
                        context.channels().targetOf(parserRole),
                        JsonFormat.TypeRegistry.newBuilder()
                                .add(ai.pipestream.document.v1.Document.getDescriptor())
                                .build());
            }

            @Override
            public void close() {
                if (server != null) {
                    server.close();
                }
            }
        };
    }

    /** The bound HTTP port; only valid after start. */
    public int port() {
        if (server == null) {
            throw new IllegalStateException("playground module has not started");
        }
        return server.port();
    }
}

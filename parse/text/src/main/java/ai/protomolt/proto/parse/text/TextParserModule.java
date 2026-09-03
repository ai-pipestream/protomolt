package ai.protomolt.proto.parse.text;

import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;

/**
 * The embedded reference parser as a mountable role: a fleet member that
 * happens to share the JVM. It publishes an in-process endpoint under
 * {@link #ROLE}; the parse coordinator and the playground resolve it
 * through the node's channels like any other parser target.
 */
public final class TextParserModule implements ServiceModule {

    /** The role name; the parser registers under {@link TextParserService#PARSER_NAME}. */
    public static final String ROLE = "parse-text";

    /** Creates the module. */
    public TextParserModule() {
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public ServiceMount wire(NodeContext context) throws IOException {
        String name = ROLE + "-" + context.nodeId();
        Server server = InProcessServerBuilder.forName(name)
                .addService(new TextParserService())
                .build()
                .start();
        context.channels().publishInProcess(ROLE, name);
        return ServiceMount.inert(server::shutdownNow);
    }
}

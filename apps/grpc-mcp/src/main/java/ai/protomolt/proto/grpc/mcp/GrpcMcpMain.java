package ai.protomolt.proto.grpc.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.mcp.transport.McpServer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Stdio launcher whose gRPC capability is installed through the ServiceModule SPI. */
public final class GrpcMcpMain {
    private GrpcMcpMain() {}

    public static void main(String[] args) throws IOException {
        if (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
            System.err.println("usage: protomolt-grpc-mcp\n"
                    + "Required environment: PROTOMOLT_GRPC_TARGET=host:port, "
                    + "PROTOMOLT_GRPC_TRANSPORT=tls|plaintext\n"
                    + "Exposes reflect and grpc-invoke on stdio for the configured target.");
            return;
        }
        if (args.length != 0) {
            throw new IllegalArgumentException("Unknown arguments; use --help");
        }
        run(System.getenv(), System.in, System.out);
    }

    static void run(Map<String, String> environment, InputStream input, OutputStream output)
            throws IOException {
        // Selecting a missing provider is a boot error. A bare transport must not silently
        // start in place of the requested gRPC integrator.
        try (var node = Composer.builder().environment(environment).build().boot(List.of("grpc-invoke"))) {
            var catalog = ActionCatalog.empty(ActionContext.create());
            node.context().contributions().all(ProtoAction.class).forEach(catalog::register);
            if (!catalog.names().containsAll(List.of("reflect", "grpc-invoke"))) {
                throw new IllegalStateException("grpc-invoke module did not supply its required actions");
            }
            String version = GrpcMcpMain.class.getPackage().getImplementationVersion();
            String instructions = "Read protomolt://workspace for the installed tools. "
                    + "Use reflect with target " + environment.get("PROTOMOLT_GRPC_TARGET")
                    + " and tls=" + "tls".equals(environment.get("PROTOMOLT_GRPC_TRANSPORT"))
                    + ". Pass its descriptorSetBase64 as schema.descriptorSetBase64 to grpc-invoke. "
                    + "Use the same target and tls setting, inspect the declared request fields, "
                    + "and check each operation result. Only the configured target is allowed.";
            new McpServer(catalog, null, "protomolt-grpc-mcp",
                    version == null ? "dev" : version, instructions).run(input, output);
        }
    }
}

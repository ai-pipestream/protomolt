package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;
import ai.pipestream.proto.grpc.service.ProtoMoltGrpcServer;
import ai.pipestream.proto.mcp.McpServer;
import ai.pipestream.proto.mcp.RegistryResources;
import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.registry.server.SchemaRegistryServer;
import ai.pipestream.proto.registry.server.SchemaRegistryServerConfig;
import ai.pipestream.proto.rest.ApiTokenRequirement;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import ai.pipestream.proto.server.jdk.JdkProtoRestServer;

import java.nio.file.Path;

/**
 * The one-process ProtoMolt server: {@code ProtoMoltService} over gRPC (reflection enabled),
 * the same verbs over JSON/REST with OpenAPI and Swagger UI, the MCP server on
 * streamable HTTP at {@code /mcp} (with registry resources when a registry is mounted), and
 * optionally the git-backed schema registry speaking the Confluent protocol.
 *
 * <pre>
 * protomolt-serve [--host 0.0.0.0] [--grpc-port 9090] [--http-port 8080]
 *                 [--registry-git /srv/schemas.git [--registry-port 8081]]
 * </pre>
 */
public final class ProtoMoltServe implements AutoCloseable {

    /**
     * Launcher options; a port of 0 picks a free port. A non-null {@code apiToken} guards
     * every operational surface (gRPC calls, REST verbs, the MCP endpoint) with a shared
     * secret; documentation surfaces (health, OpenAPI, Swagger UI) stay open.
     */
    public record Options(String host, int grpcPort, int httpPort,
                          Path registryGit, int registryPort, String apiToken, boolean demo,
                          Path gatherCache) {

        public Options(String host, int grpcPort, int httpPort, Path registryGit, int registryPort) {
            this(host, grpcPort, httpPort, registryGit, registryPort, null, false, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, false, null);
        }

        public Options(String host, int grpcPort, int httpPort, Path registryGit,
                       int registryPort, String apiToken, boolean demo) {
            this(host, grpcPort, httpPort, registryGit, registryPort, apiToken, demo, null);
        }

        public static Options defaults() {
            return new Options("0.0.0.0", 9090, 8080, null, 8081, null, false, null);
        }

        static Options parse(String[] args) {
            String host = "0.0.0.0";
            int grpcPort = 9090;
            int httpPort = 8080;
            Path registryGit = null;
            int registryPort = 8081;
            String apiToken = System.getenv("PROTOMOLT_API_TOKEN");
            boolean demo = false;
            String gatherCacheEnv = System.getenv("PROTOMOLT_GATHER_CACHE");
            Path gatherCache = gatherCacheEnv == null || gatherCacheEnv.isBlank()
                    ? null
                    : Path.of(gatherCacheEnv);
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--host" -> host = requireValue(args, ++i);
                    case "--grpc-port" -> grpcPort = Integer.parseInt(requireValue(args, ++i));
                    case "--http-port" -> httpPort = Integer.parseInt(requireValue(args, ++i));
                    case "--registry-git" -> registryGit = Path.of(requireValue(args, ++i));
                    case "--registry-port" -> registryPort = Integer.parseInt(requireValue(args, ++i));
                    case "--api-token" -> apiToken = requireValue(args, ++i);
                    case "--gather-cache" -> gatherCache = Path.of(requireValue(args, ++i));
                    case "--demo" -> demo = true;
                    case "--help", "-h" -> {
                        System.err.println("usage: protomolt-serve [--host <addr>] [--grpc-port <n>] "
                                + "[--http-port <n>] [--registry-git <path> [--registry-port <n>]] "
                                + "[--api-token <secret>]  (or PROTOMOLT_API_TOKEN) "
                                + "[--gather-cache <dir>]  (or PROTOMOLT_GATHER_CACHE) [--demo]");
                        System.exit(0);
                    }
                    default -> {
                        System.err.println("unknown argument: " + args[i]);
                        System.exit(2);
                    }
                }
            }
            if (apiToken != null && apiToken.isBlank()) {
                apiToken = null;
            }
            return new Options(host, grpcPort, httpPort, registryGit, registryPort, apiToken,
                    demo, gatherCache);
        }

        private static String requireValue(String[] args, int i) {
            if (i >= args.length) {
                System.err.println(args[i - 1] + " requires a value");
                System.exit(2);
            }
            return args[i];
        }
    }

    private final ProtoMoltGrpcServer grpc;
    private final JdkProtoRestServer http;
    private final GitSchemaRegistryStore registryStore;
    private final SchemaRegistryServer registry;
    private final int httpPort;
    private final int registryPort;

    private ProtoMoltServe(ProtoMoltGrpcServer grpc, JdkProtoRestServer http, int httpPort,
                           GitSchemaRegistryStore registryStore, SchemaRegistryServer registry,
                           int registryPort) {
        this.grpc = grpc;
        this.http = http;
        this.httpPort = httpPort;
        this.registryStore = registryStore;
        this.registry = registry;
        this.registryPort = registryPort;
    }

    /** Starts every configured surface; closing stops them all. */
    public static ProtoMoltServe start(Options options) {
        ActionContext context = ActionContext.create();

        ProtoMoltGrpcServer grpc = null;
        JdkProtoRestServer http = null;
        GitSchemaRegistryStore store = null;
        SchemaRegistryServer registry = null;
        try {
            Path registryGit = options.registryGit();
            if (registryGit == null && options.demo()) {
                // Demo mode always has a registry; an unnamed one lives in a temp directory.
                try {
                    registryGit = java.nio.file.Files.createTempDirectory("protomolt-demo-registry");
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("Failed to create the demo registry directory", e);
                }
            }
            if (registryGit != null) {
                store = GitSchemaRegistryStore.builder()
                        .repositoryDir(registryGit)
                        .build();
            }
            // The catalog sees the store so run-chain resolves stored chain names.
            ActionCatalog catalog = ProtoMoltCatalog.full(context, options.gatherCache(),
                    store == null ? null : chainRepository(store));
            if (options.demo()) {
                DemoSchemas.seed(context.registry(), store);
            }

            grpc = ProtoMoltGrpcServer.start(options.host(), options.grpcPort(), catalog,
                    options.apiToken());
            if (options.demo() && store != null) {
                // The demo chain composes this server's own verbs, so it needs the bound
                // gRPC port - seeded here rather than with the schemas.
                DemoSchemas.seedChain(store, grpc.port());
            }

            // The registry starts before HTTP so the console's same-origin proxy
            // (/api/protomolt) knows the port it bridges to.
            int registryPort = -1;
            if (store != null) {
                // The registry listener honors the same bind address and shared secret as
                // every other surface - one process, one security boundary.
                registry = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults()
                                .withHost(options.host())
                                .withPort(options.registryPort())
                                .withApiToken(options.apiToken()),
                        store, catalog);
                registryPort = registry.start();
            }

            ProtoRestMethodRegistry methods = new ProtoRestMethodRegistry();
            ProtoMoltRestMount.register(methods, catalog, options.apiToken() == null
                    ? null
                    : ApiTokenRequirement.apiKeyHeader("api_token"));
            ProtoToolsServerConfig config = ProtoToolsServerConfig.defaults()
                    .withHost(options.host())
                    .withPort(options.httpPort());
            ProtoRestGateway gateway = options.apiToken() == null
                    ? new ProtoRestGateway(methods, context.transcoder())
                    : new ProtoRestGateway(methods, context.transcoder(),
                            ProtoApiTokenValidator.sharedSecret(options.apiToken()));
            String version = ProtoMoltServe.class.getPackage().getImplementationVersion();
            McpServer mcp = new McpServer(catalog,
                    store != null ? new RegistryResources(store) : null,
                    "protomolt", version != null ? version : "dev");
            int boundRegistryPort = registryPort;
            int[] selfPort = {-1};
            http = new JdkProtoRestServer(config, gateway,
                    new ProtoOpenApiGenerator("ProtoMolt", version != null ? version : "dev",
                            "/", config.restPathPrefix()))
                    .withContext("/docs", new SwaggerUiHandler("/docs", config.openApiPath()))
                    .withContext("/mcp", new McpHttpHandler(mcp, options.apiToken()));
            if (options.apiToken() == null) {
                http.withContext("/console", new ConsoleHandler())
                        .withContext("/api/protomolt", new ApiProxyHandler("/api/protomolt",
                                () -> boundRegistryPort,
                                "no registry is running; start with --registry-git or --demo"))
                        .withContext("/api/serve", new ApiProxyHandler("/api/serve",
                                () -> selfPort[0], "server is still starting"));
            } else {
                // A browser cannot hold the process's shared secret, so a token-mode
                // console would be a half-open door: some calls 401, registry writes
                // silently open. Disable the whole surface with an explicit answer
                // instead of serving a partially secured interface.
                DisabledSurfaceHandler disabled = new DisabledSurfaceHandler(
                        "the console is disabled when --api-token is set; use the gRPC, "
                                + "REST, or MCP surface with the token, or run without one "
                                + "on a trusted network");
                http.withContext("/console", disabled)
                        .withContext("/api/protomolt", disabled)
                        .withContext("/api/serve", disabled);
            }
            int httpPort = http.start();
            selfPort[0] = httpPort;
            return new ProtoMoltServe(grpc, http, httpPort, store, registry, registryPort);
        } catch (RuntimeException e) {
            if (registry != null) {
                registry.close();
            }
            if (http != null) {
                http.close();
            }
            if (grpc != null) {
                grpc.close();
            }
            closeQuietly(store);
            throw e;
        }
    }

    private static ai.pipestream.proto.chain.ChainRepository chainRepository(
            GitSchemaRegistryStore store) {
        com.fasterxml.jackson.databind.ObjectMapper json =
                new com.fasterxml.jackson.databind.ObjectMapper();
        return name -> {
            try {
                return store.chain(name).map(text -> {
                    try {
                        var node = json.readTree(text);
                        return node instanceof com.fasterxml.jackson.databind.node.ObjectNode chain
                                ? chain : null;
                    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                        return null;
                    }
                });
            } catch (Exception e) {
                return java.util.Optional.empty();
            }
        };
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // best-effort teardown on a failed start
            }
        }
    }

    public int grpcPort() {
        return grpc.port();
    }

    public int httpPort() {
        return httpPort;
    }

    /** The registry port, or -1 when no registry is mounted. */
    public int registryPort() {
        return registryPort;
    }

    /** Blocks until the gRPC server terminates. */
    public void awaitTermination() throws InterruptedException {
        grpc.awaitTermination();
    }

    @Override
    public void close() {
        if (registry != null) {
            registry.close();
        }
        closeQuietly(registryStore);
        http.close();
        grpc.close();
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ProtoMoltServe serve = start(options);
        System.out.printf("""
                ProtoMolt serving:
                  gRPC  %1$s:%2$d   ai.pipestream.protomolt.v1.ProtoMoltService (reflection on)
                  REST  http://%1$s:%3$d/grpc-json/ProtoMoltService/{Method}
                  API   http://%1$s:%3$d/openapi.json
                  Docs  http://%1$s:%3$d/docs
                  MCP   http://%1$s:%3$d/mcp   (streamable HTTP)
                """, options.host(), serve.grpcPort(), serve.httpPort());
        if (serve.registryPort() >= 0) {
            System.out.printf("  Reg   http://%s:%d (Confluent protocol, git-backed)%n",
                    options.host(), serve.registryPort());
        }
        if (options.apiToken() == null) {
            System.out.printf("  UI    http://%s:%d/console%n", options.host(), serve.httpPort());
        } else {
            System.out.println("  Auth  api_token required on gRPC, REST, MCP"
                    + (serve.registryPort() >= 0 ? ", and the registry" : "")
                    + " (health, OpenAPI, and docs stay open)");
            System.out.println("  UI    console disabled in token mode (a browser cannot "
                    + "hold the shared secret)");
        }
        if (options.demo()) {
            System.out.printf("""
                    Demo schema seeded: subject %s (types demo.shop.v1.Order, Customer, ...)
                      Try: curl -s -H 'content-type: application/json' \\
                             -d '{"schema": {"type": "demo.shop.v1.Order"}}' \\
                             http://%s:%d/grpc-json/ProtoMoltService/RenderJsonSchema
                      Or open http://%s:%d/docs and call ValidateMessage on a demo.shop.v1.Order.
                    """, DemoSchemas.SHOP_SUBJECT,
                    options.host(), serve.httpPort(), options.host(), serve.httpPort());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(serve::close));
        serve.awaitTermination();
    }
}

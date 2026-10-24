package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.composer.Channels;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import io.grpc.Server;

import java.util.List;

/**
 * The document store as a mountable role. Wiring starts the in-process
 * endpoint siblings resolve through {@link Channels}; starting binds the
 * external Netty port, binds the HTTP port when
 * {@link RepoServiceConfig#httpPort()} is positive (the upload routes, the
 * console from {@link RepositoryConsole#ENV_CONSOLE_DIR} and its sign-in from the
 * {@link ai.protomolt.proto.authz.OidcConsoleSettings} variables), seeds account
 * drives, and begins the lifecycle loops.
 */
public final class RepoServiceModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "repo";

    private final RepoServiceConfig config;
    private final CallerResolver callers;
    private final AccountBindingResolver accounts;
    private RepoServices services;
    private Server netty;
    private UploadHttpServer http;

    /**
     * Creates the module with only the operator token authenticating its TCP listener.
     *
     * @param config the repo service configuration
     */
    public RepoServiceModule(RepoServiceConfig config) {
        this(config, null, AccountBindingResolver.none());
    }

    /**
     * Creates the module with scoped callers on its TCP listener.
     *
     * @param config the repo service configuration
     * @param callers resolves non-operator credentials to their principals; null when only
     *        the operator token authenticates
     * @param accounts binds scoped principals to the accounts they act for
     */
    public RepoServiceModule(RepoServiceConfig config, CallerResolver callers,
            AccountBindingResolver accounts) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (accounts == null) {
            throw new IllegalArgumentException("accounts must not be null");
        }
        this.config = config;
        this.callers = callers;
        this.accounts = accounts;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public ServiceMount wire(NodeContext context) {
        String credential = RepositoryNetworkAuthentication.requireOperatorToken(context.environment().get("PROTOMOLT_API_TOKEN"));
        // Read before any service is built so a misconfigured bundle fails the boot,
        // not the first browser request.
        RepositoryConsole console = config.httpPort() > 0
                ? RepositoryConsole.fromEnvironment(context.environment())
                : null;
        ConsoleSignIn signIn = config.httpPort() > 0
                ? ConsoleSignIn.fromEnvironment(context.environment())
                : ConsoleSignIn.none();
        services = RepoServices.build(config).withAccountBindings(accounts);
        if (config.conceptSchemePaths().isEmpty()) {
            var envConceptPaths = RepoServiceConfig.parseConceptSchemePaths(context.environment());
            if (!envConceptPaths.isEmpty()) {
                services.withConceptSchemes(RepoServices.loadConceptSchemes(envConceptPaths));
            }
        }
        String name = ROLE + "-" + context.nodeId();
        services.startInProcess(name);
        context.channels().publishInProcess(ROLE, name);
        // The in-process transport needs no credential: it has no socket, and a caller
        // already inside the JVM is past every boundary a credential could draw. The TCP
        // listener is the one reachable from elsewhere, so that is the one guarded.
        // With a co-mounted registry, register the bridge-entry workflow so
        // operators can submit bridging as a durable run by name; without one
        // the two RPCs still answer, there is just no declared envelope.
        List<GitSchemaRegistryStore> registries =
                context.contributions().all(GitSchemaRegistryStore.class);
        if (!registries.isEmpty()) {
            registries.getFirst().putWorkflow(
                    ArchiveWorkflows.BRIDGE_ENTRY_WORKFLOW,
                    ArchiveWorkflows.bridgeEntryWorkflow(
                            context.channels().targetOf(ROLE), 300_000).toString());
        }
        return new ServiceMount() {
            @Override
            public void start() {
                // All modules are wired by the time any mount starts, so co-mounted
                // contributions (the parse module's fleet document model) are visible here.
                services.withAdmittedSchemas(context.contributions().all(com.google.protobuf.Descriptors.FileDescriptor.class));
                List<ai.protomolt.proto.config.ConceptSchemeMounts> contributedSchemes =
                        context.contributions().all(ai.protomolt.proto.config.ConceptSchemeMounts.class);
                if (!contributedSchemes.isEmpty()) {
                    services.withConceptSchemes(contributedSchemes.getFirst());
                }
                netty = services.startNetty(config.grpcPort(), credential, callers);
                // Same rule as RepoServiceMain: 0 or "off" leaves the HTTP port, and
                // every route on it, unbound.
                if (console != null) {
                    http = services.startHttp(config.httpPort(), credential, console,
                            services.consoleSignInRoutes(signIn.sessions()),
                            RepoServices.requestCallers(credential, callers, signIn.sessions()));
                }
                services.seedAccountDrives();
                services.startLifecycle();
            }

            @Override
            public void close() {
                services.close();
            }
        };
    }

    /** The bound external gRPC port; only valid after start. */
    public int grpcPort() {
        if (netty == null) {
            throw new IllegalStateException("repo module has not started");
        }
        return netty.getPort();
    }

    /**
     * The bound HTTP port.
     *
     * @return the port the upload routes and the console listen on
     * @throws IllegalStateException before start, or when the HTTP port is disabled
     */
    public int httpPort() {
        if (http == null) {
            throw new IllegalStateException(netty == null
                    ? "repo module has not started"
                    : "repo module HTTP port is disabled (" + RepoServiceConfig.ENV_HTTP_PORT + ")");
        }
        return http.port();
    }
}

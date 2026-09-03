package ai.protomolt.proto.search.console;

import ai.protomolt.proto.actions.ScopeBudgets;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.composer.ServiceModule;

import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The search console as a mountable role. The service target comes from the node's channels
 * (in-process when the search role is co-mounted); the actions route is plain configuration
 * because the registry serves HTTP, not gRPC — pass blank to mount the console without the
 * operations panel.
 */
public final class SearchConsoleModule implements ServiceModule {

    /** The role name. */
    public static final String ROLE = "search-console";

    /** How long a console login lasts before the browser signs in again. */
    public static final Duration SESSION_TTL = Duration.ofHours(12);

    /**
     * Module configuration.
     *
     * @param port the HTTP port (0 for ephemeral)
     * @param actionsBaseUrl supplies the registry actions route the operations panel proxies
     *        to, e.g. {@code http://127.0.0.1:8081/protomolt/actions} — a supplier because a
     *        co-mounted registry's port is only known once it starts; a blank answer disables
     *        the panel
     * @param callers resolves login credentials to access-policy principals on a guarded
     *        node; null mounts the open, trusted-network console
     */
    public record Config(int port, Supplier<String> actionsBaseUrl, CallerResolver callers) {

        /** The open, trusted-network console. */
        public Config(int port, Supplier<String> actionsBaseUrl) {
            this(port, actionsBaseUrl, null);
        }

        /** This configuration with browser sessions bound to {@code callers} principals. */
        public Config secured(CallerResolver callers) {
            if (callers == null) {
                throw new IllegalArgumentException("callers must not be null");
            }
            return new Config(port, actionsBaseUrl, callers);
        }
    }

    private final Config config;
    private SearchConsoleServer server;

    /**
     * Creates the module.
     *
     * @param config the module configuration
     */
    public SearchConsoleModule(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        this.config = config;
    }

    @Override
    public String role() {
        return ROLE;
    }

    @Override
    public Set<String> requires() {
        return Set.of("search");
    }

    @Override
    public ServiceMount wire(NodeContext context) throws Exception {
        server = new SearchConsoleServer(
                config.port(),
                context.channels().targetOf("search"),
                config.actionsBaseUrl(),
                config.callers() == null
                        ? ConsoleSessions.open(SearchConsoleServer.COOKIE)
                        : ConsoleSessions.secured(SearchConsoleServer.COOKIE,
                                SESSION_TTL, config.callers()),
                // The console's login boundary spends the same ledger as the search
                // role it fronts: one allowance per principal, not one per door.
                context.contributions().shared(ScopeBudgets.class, ScopeBudgets::new));
        return new ServiceMount() {
            @Override
            public void start() {
                server.start();
            }

            @Override
            public void close() {
                server.close();
            }
        };
    }

    /** The bound HTTP port; only valid after start. */
    public int port() {
        if (server == null) {
            throw new IllegalStateException("search console module has not wired");
        }
        return server.port();
    }
}

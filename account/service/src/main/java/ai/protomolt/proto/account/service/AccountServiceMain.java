package ai.protomolt.proto.account.service;

import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * account-service entry point: the standalone deployment. All wiring lives
 * in {@link AccountServices} — this class only resolves the environment
 * config, starts the Netty server, installs the shutdown hook, and blocks.
 * The operator token ({@value AccountServices#ENV_API_TOKEN}) is required:
 * the listener authenticates every call with it or a credential the
 * configured caller stores name, and the repo channel presents it.
 * To embed the same services in-JVM, call {@link AccountServices#build} and
 * {@link AccountServices#startInProcess(String)} instead.
 */
public final class AccountServiceMain {

    private static final Logger LOG = LoggerFactory.getLogger(AccountServiceMain.class);

    private AccountServiceMain() {
    }

    /**
     * Boots the service from the environment and blocks until shutdown.
     *
     * @param args ignored (configuration is env-driven)
     * @throws Exception on boot failure
     */
    public static void main(String[] args) throws Exception {
        AccountCallerConfiguration callers =
                AccountCallerConfiguration.fromEnvironment(System.getenv());
        AccountServiceConfig config = AccountServiceConfig.fromEnvironment();
        AccountServices services = AccountServices.build(config, callers.apiToken())
                .withAccountBindings(callers.bindings());
        Server server = services.startNetty(config.grpcPort(), callers.apiToken(),
                callers.resolver());
        // The account-events outbox relay runs as a background loop next to
        // the server (no-op when Kafka is not configured).
        services.startLifecycle();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                services.close();
            } catch (RuntimeException e) {
                LOG.warn("Shutdown failed: {}", e.getMessage());
            }
        }, "account-service-shutdown"));
        LOG.info("account-service listening on port {}", server.getPort());
        server.awaitTermination();
    }
}

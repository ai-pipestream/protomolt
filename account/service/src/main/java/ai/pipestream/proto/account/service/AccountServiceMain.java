package ai.pipestream.proto.account.service;

import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * account-service entry point: the standalone deployment. All wiring lives
 * in {@link AccountServices} — this class only resolves the environment
 * config, starts the Netty server, installs the shutdown hook, and blocks.
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
        AccountServiceConfig config = AccountServiceConfig.fromEnvironment();
        AccountServices services = AccountServices.build(config);
        Server server = services.startNetty(config.grpcPort());
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

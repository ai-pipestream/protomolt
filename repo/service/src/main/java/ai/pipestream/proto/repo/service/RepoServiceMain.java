package ai.pipestream.proto.repo.service;

import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * repo-service entry point: the standalone deployment. All wiring lives in
 * {@link RepoServices} — this class only resolves the environment config,
 * starts the Netty server, installs the shutdown hook, and blocks. To embed
 * the same services in-JVM, call {@link RepoServices#build} and
 * {@link RepoServices#startInProcess(String)} instead.
 */
public final class RepoServiceMain {

    private static final Logger LOG = LoggerFactory.getLogger(RepoServiceMain.class);

    private RepoServiceMain() {
    }

    /**
     * Boots the service from the environment and blocks until shutdown.
     *
     * @param args ignored (configuration is env-driven)
     * @throws Exception on boot failure
     */
    public static void main(String[] args) throws Exception {
        RepoServiceConfig config = RepoServiceConfig.fromEnvironment();
        RepoServices services = RepoServices.build(config);
        // Seeded default account (DOCUMENT_PLATFORM_SEED_ACCOUNT_ID): ensure
        // the account's intake/pipeline drives exist before serving. No-op
        // when unset; embedded hosts opt in by calling it themselves.
        services.seedAccountDrives();
        Server server = services.startNetty(config.grpcPort());
        // HTTP upload route: DOCUMENT_PLATFORM_HTTP_PORT, default 8080; 0 or
        // "off" disables it (gRPC-only deployments).
        if (config.httpPort() > 0) {
            services.startHttp(config.httpPort());
        }
        // The two-phase delete's Phase B (plus sweeper and, when enabled, the
        // periodic reconcile) runs as background loops next to the servers.
        services.startLifecycle();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                services.close();
            } catch (RuntimeException e) {
                LOG.warn("Shutdown failed: {}", e.getMessage());
            }
        }, "repo-service-shutdown"));
        LOG.info("repo-service listening on port {}", server.getPort());
        server.awaitTermination();
    }
}

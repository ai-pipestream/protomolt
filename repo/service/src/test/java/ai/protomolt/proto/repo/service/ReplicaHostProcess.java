package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import java.nio.file.Files;
import java.nio.file.Path;

/** Test-only launcher of the real service assembly; no alternate repository implementation. */
public final class ReplicaHostProcess {
    public static void main(String[] args) throws Exception {
        var env = System.getenv();
        int poolSize = env.containsKey("TEST_POOL_SIZE") ? Integer.parseInt(env.get("TEST_POOL_SIZE")) : LedgerConfig.DEFAULT_POOL_SIZE;
        if (poolSize < 1) throw new IllegalArgumentException("TEST_POOL_SIZE must be positive");
        var config = new RepoServiceConfig(0,
                new LedgerConfig(env.get("TEST_JDBC"), env.get("TEST_DB_USER"), env.get("TEST_DB_PASSWORD"),
                        poolSize, LedgerConfig.DEFAULT_MIGRATION_LOCATION),
                env.get("TEST_S3_ENDPOINT"), env.get("TEST_S3_REGION"), env.get("TEST_S3_KEY"), env.get("TEST_S3_SECRET"),
                "process-host", 0, null, null, null, null, 0, 0L,
                true, 1000, 1000, false, true, 1000)
                .withManagedStorage(new ManagedStoragePolicy("process-original", "process-realm", true));
        boolean measured = "true".equals(env.get("TEST_METRICS"));
        var metrics = measured ? new ReplicaMetrics() : null;
        try (var host = measured ? new RepoServices(config, ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), metrics.providers())
                : RepoServices.build(config)) {
            if (measured) metrics.attach(host.ledgerDataSource());
            var server = host.startNetty(0, env.get("TEST_API_TOKEN"), null);
            Path ready = Path.of(args[0]);
            Path pending = ready.resolveSibling(ready.getFileName() + ".pending");
            Files.writeString(pending, Integer.toString(server.getPort()));
            Files.move(pending, ready, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            if (measured) {
                while (!server.isShutdown()) { metrics.respond(ready); Thread.sleep(10); }
            } else server.awaitTermination();
        }
    }
}

package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.engine.DocumentHistoricalReader;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Fresh-JVM reader: no writer proof, descriptor supplier, or fragment input is available here. */
public final class DocumentHistoricalReadWorker {
    public static void main(String[] args) throws Exception {
        var env = System.getenv();
        UUID revision = UUID.fromString(args[0]);
        UUID incarnation = UUID.fromString(args[1]);
        var address = NodeAddress.newBuilder().setAccountId(args[2]).setDocId(args[3])
                .setGraphId(args[4]).setGraphAddressId(args[5]).build();
        try (var database = new LedgerDatabase(new LedgerConfig(env.get("TEST_DB_URL"), env.get("TEST_DB_USER"), env.get("TEST_DB_PASSWORD")));
                var opened = BlobStores.discover().open("s3", Map.of("endpoint", env.get("TEST_ENDPOINT"), "region", env.get("TEST_REGION"),
                        "access-key", env.get("TEST_ACCESS"), "secret-key", env.get("TEST_SECRET"), "path-style", "true", "conditional-writes", "false"))) {
            var tx = new Tx(database.entityManagerFactory());
            String generation = env.get("TEST_GENERATION");
            var retained = new ManagedBackendLedger(tx).find(generation).orElseThrow();
            if (!retained.identity().equals(ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of(
                    env.get("TEST_ENDPOINT"), env.get("TEST_REGION"), true)))
                throw new IllegalStateException("Worker storage identity differs from retained backend");
            var ledger = new DocumentReadLedger(tx, incarnation);
            DocumentReadLedger.PinnedHistory history;
            try {
                history = ledger.captureHistorical(new RepositoryCaller("restart-reader", false, Set.of(address.getAccountId()), Set.of()),
                        address, revision);
            } catch (RuntimeException failure) {
                try { ledger.fence(); ledger.attestLocalQuiescence(); }
                catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            var capacity = new PayloadBudget(16_000_000);
            String receipt;
            try (var raw = new DocumentPartReader((selectedGeneration, profile) -> {
                if (!generation.equals(selectedGeneration) || !retained.equals(profile))
                    throw new IllegalStateException("Historical backend profile differs");
                return opened.store();
            }, 4, 1_000_000, capacity)) {
                try (var validated = new DocumentHistoricalReader(raw, capacity).readValidated(history, RepositoryReadControl.NONE)) {
                    receipt = "REPLAY_OK|" + validated.revision() + "|"
                            + DocumentPartCodec.sha256Hex(validated.document().toByteArray()) + "|" + validated.policySha256();
                } finally {
                    raw.close();
                    if (!raw.awaitIdle(Duration.ofSeconds(5))) throw new IllegalStateException("Provider reader did not become idle");
                }
            } finally {
                history.close();
                if (!history.awaitDrained(Duration.ofSeconds(5))) throw new IllegalStateException("Historical reader did not drain");
                history.release(); ledger.fence(); ledger.attestLocalQuiescence();
                if (capacity.reservedBytes() != 0) throw new IllegalStateException("Historical reservations leaked");
            }
            System.out.println(receipt);
        } catch (RepositoryException failure) {
            System.out.println("REPLAY_FAILURE|" + failure.code());
            throw failure;
        }
    }
}

package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Test-only privileged bootstrap; the parent must reap the crashed writer before starting the reader. */
public final class JournaledAssessmentCrashProbe {
    public static void main(String[] args) throws Exception {
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[1]), () -> {});
        var operation = UUID.fromString(System.getenv("PROTOMOLT_TEST_CRASH_OPERATION"));
        try (var provider = new AssessmentProviderProbe();
                var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                        System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")))) {
            var tx = new Tx(database.entityManagerFactory());
            if (args[0].equals("writer")) {
                AssessmentOperationReplayProbe.crash(tx, provider, new DocumentSchemaPolicies(tx).read("account", () -> {}),
                        observation, database.dataSource());
                throw new AssertionError("Writer did not halt");
            }
            if (!args[0].equals("reader")) throw new IllegalArgumentException("Expected writer or reader");
            var key = new RepositoryOperationLedger.Key("account", "principal", operation);
            // Capabilities come only from private shared SQL in this test. Never expose this as a host API.
            Object[] saved = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT o.owner_token,o.owner_generation,
                      CAST(floor(extract(epoch FROM o.lease_until)*1000000) AS bigint),
                      c.claim_token,c.claim_epoch,CAST(floor(extract(epoch FROM c.lease_until)*1000000) AS bigint),
                      encode(c.command_sha256,'hex')
                    FROM repository_operation_owners o JOIN repository_execution_claims c
                      USING(account_id,principal,operation_id)
                    WHERE o.account_id='account' AND o.principal='principal' AND o.operation_id=:op
                      AND o.lease_until>clock_timestamp() AND c.lease_until>clock_timestamp()
                    """).setParameter("op", operation).getSingleResult());
            var claim = new RepositoryExecutionClaimLedger.Claim(key, (String) saved[6], ((Number) saved[4]).longValue(),
                    (UUID) saved[3], time(saved[5]));
            var owner = new RepositoryOperationLedger.Owner(key, ((Number) saved[1]).longValue(), (UUID) saved[0],
                    time(saved[2]), Optional.of(claim));
            var budget = new PayloadBudget(128_000_000);
            var caller = new RepositoryCaller("principal", true);
            var command = new DocumentPublicationPreparationJournal(tx, budget)
                    .readCommand(caller, key, owner.generation()-1, RepositoryReadControl.NONE).orElseThrow();
            require(new DocumentPublicationReplay(tx).observe(caller, command).state() == DocumentPublicationReplay.State.PENDING,
                    "crashed writer has no terminal receipt");
            var original = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
            JournaledAssessmentProbe.resume(tx, provider, caller, owner, command, observation,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000));
            var receipt = new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow();
            require(receipt.getAssessment().getAssessmentId().equals(original.stage().assessment().toString())
                    && receipt.getAssessment().getManifestSha256().equals(original.stage().manifestSha256()),
                    "fresh reader decides original committed stage");
            require(budget.reservedBytes() == 0, "bootstrap reservation released");
        }
        System.out.println("JOURNALED_FORCED_CRASH_RECOVERY_OK");
    }
    private static Instant time(Object micros) {
        long value = ((Number) micros).longValue();
        return Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000), Math.floorMod(value, 1_000_000)*1000);
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}

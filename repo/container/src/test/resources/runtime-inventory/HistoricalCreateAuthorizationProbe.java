package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/** Revoke after initial CREATE authorization, while staging waits on the real schema-policy row. */
final class HistoricalCreateAuthorizationProbe {
    static void run(Tx tx, DataSource database, RepositoryCaller caller, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, DocumentUploadPlan.Placement placement,
            DocumentReadLedger.PinnedHistory history, Map<Integer, ByteString> fragments, PayloadBudget budget,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var key = new RepositoryOperationLedger.Key("account", caller.principalName(), command.operationId());
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
        var scopes = new DocumentPublicationScopeCalls();
        try (var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE)) {
            var registration = DocumentPublicationRegistration.historical(tx, budget, record, sources, UUID.randomUUID(),
                    scopes, new DriveLedger(tx), RepositoryReadControl.NONE);
            var modes = Map.of("a", DocumentPublicationCandidate.Mode.TYPED);
            var owner = registration.admitInitial(caller, modes, RepositoryReadControl.NONE).orElseThrow();
            try (var execution = registration.historicalExecution(caller, owner, modes, RepositoryReadControl.NONE)) {
                execution.admitUploads(caller, RepositoryReadControl.NONE);
                var started = execution.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE);
                try (var assessment = execution.prepareAssessment(caller, policy, Map.of("a", fragments), Optional.empty(),
                        (member, occurrence) -> { throw new AssertionError("Historical schema must be retained"); },
                        new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), RepositoryReadControl.NONE)) {
                    try (var workers = Executors.newVirtualThreadPerTaskExecutor(); var blocker = database.getConnection()) {
                        blocker.setAutoCommit(false);
                        blocker.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
                        int pid;
                        try (var statement = blocker.prepareStatement("""
                                SELECT pg_backend_pid() FROM document_schema_policy_current
                                WHERE account_id='account' FOR UPDATE
                                """); var rows = statement.executeQuery()) {
                            require(rows.next(), "current policy row exists"); pid = rows.getInt(1);
                        }
                        try {
                            var pending = workers.submit(() -> execution.createAssessment(caller, assessment, Map.of(), observation,
                                    new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE));
                            awaitPolicyWaiter(tx, pid);
                            require(!pending.isDone(), "CREATE remains blocked before staging authorization");
                            new RepositoryCredentialAuthorities(tx).revoke(new RepositoryCaller("operator", true),
                                    caller.credentialBinding().orElseThrow(), caller.principalName());
                            blocker.commit();
                            try {
                                pending.get(15, TimeUnit.SECONDS);
                                throw new AssertionError("Revoked credential created assessment");
                            } catch (ExecutionException failure) {
                                require(failure.getCause() instanceof RepositoryException denied
                                        && denied.code() == RepositoryException.Code.UNAUTHENTICATED,
                                        "staging reports credential revocation after policy wait");
                            }
                        } finally { blocker.rollback(); }
                    }
                    for (String table : List.of("repository_schema_artifact_claims", "document_assessment_owners")) {
                        long rows = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                                .setParameter("op", command.operationId()).getSingleResult()).longValue());
                        require(rows == 0, "revoked staging writes no rows in " + table);
                    }
                }
            }
        } finally { scopes.close(); }
        System.out.println("CLAIMED_HISTORICAL_STAGE_REVOCATION_OK");
    }

    private static void awaitPolicyWaiter(Tx tx, int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            var waiting = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pid FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))
                        AND wait_event_type='Lock' AND query LIKE '%document_schema_policy_current%'
                    """).setParameter("pid", blocker).getResultList());
            if (!waiting.isEmpty()) return;
            Thread.sleep(10);
        }
        throw new AssertionError("CREATE did not reach the held staging policy row");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}

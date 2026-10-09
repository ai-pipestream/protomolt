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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Commit wins authorization, but revocation wins delivery of its result. */
final class HistoricalCreateWinnerProbe {
    static void run(Tx tx, Tx independent, HistoricalAuthorizationCommitGate gate, boolean createWins,
            RepositoryCaller caller, DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            DocumentUploadPlan.Placement placement, DocumentReadLedger.PinnedHistory history,
            Map<Integer, ByteString> fragments, PayloadBudget budget,
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
                    require(count(independent, "repository_schema_artifact_claims", command) == 0, "no prior schema claims");
                    require(count(independent, "document_assessment_owners", command) == 0, "no prior assessment");
                    var authority = HistoricalAssessmentCreationProbe.authority(independent, key);
                    gate.arm(owner, started.assessment());
                    try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                        try {
                            var pending = workers.submit(() -> execution.createAssessment(caller, assessment, Map.of(), observation,
                                    new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE));
                            int writer = gate.awaitWriter();
                            require(!pending.isDone(), "caller waits at the selected commit");
                            var revoke = workers.submit(() -> new RepositoryCredentialAuthorities(independent).revoke(
                                    new RepositoryCaller("operator", true), caller.credentialBinding().orElseThrow(), caller.principalName()));
                            awaitRevoker(independent, writer);
                            require(!revoke.isDone(), "revoker cannot pass the writer's authorization lock");
                            gate.commit();
                            gate.awaitCommitted();
                            revoke.get(15, TimeUnit.SECONDS);
                            require(!pending.isDone(), "committed result remains held until revocation finishes");
                            gate.release();
                            try {
                                pending.get(15, TimeUnit.SECONDS);
                                throw new AssertionError("Revoked caller received CREATE success");
                            } catch (ExecutionException failure) {
                                require(failure.getCause() instanceof RepositoryException denied
                                                && denied.code() == RepositoryException.Code.UNAUTHENTICATED,
                                        "revoked caller receives UNAUTHENTICATED after the held commit: " + failure.getCause());
                            }
                        } finally { gate.release(); }
                    }
                    require(HistoricalAssessmentCreationProbe.authority(independent, key).equals(authority),
                            "commit race preserves exact owner and claim leases");
                    require(count(independent, "repository_schema_artifact_claims", command) > 0,
                            "authorized staging is durable");
                    expectDenied(() -> new DocumentAssessmentDiscovery(independent).discover(caller, owner, command, () -> {}));
                    // Host authority is explicit and retains the same operation principal. The revoked caller gains no access.
                    var host = new RepositoryCaller(caller.principalName(), true);
                    var discovered = new DocumentAssessmentDiscovery(independent).discover(host, owner, command, () -> {});
                    if (createWins) {
                        require(count(independent, "document_assessment_owners", command) == 1, "exactly one durable assessment");
                        var original = discovered.orElseThrow();
                        var stage = original.stage();
                        require(stage.assessment().equals(started.assessment()) && stage.retainUntil().equals(started.retainUntil()),
                                "discovery returns original committed start identity");
                        require(original.selections().isEmpty(), "historical-only candidate has no upload selections");
                        var reconciliation = new DocumentAssessmentReconciliation(independent);
                        expectDenied(() -> reconciliation.observeRetained(caller, owner, command, original.selections(),
                                stage.assessment(), stage.manifestSha256(), stage.retainUntil(), budget, () -> {}));
                        require(reconciliation.observeRetained(host, owner, command, original.selections(), stage.assessment(),
                                stage.manifestSha256(), stage.retainUntil(), budget, () -> {}).orElseThrow().equals(stage),
                                "authorized host reconciles durable evidence after delivery refusal");
                    } else {
                        require(discovered.isEmpty(), "staging success cannot fabricate an assessment");
                        for (String table : List.of("document_assessment_owners", "document_assessment_objects", "document_assessment_slots",
                                "document_assessment_roots", "document_assessment_artifacts", "document_assessment_slot_snapshots")) {
                            long rows = independent.readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM " + table + " WHERE assessment_id=:id")
                                    .setParameter("id", started.assessment()).getSingleResult()).longValue());
                            require(rows == 0, "stage winner leaves no rows in " + table);
                        }
                    }
                    require(count(independent, "document_revision_commits", command) == 0, "assessment does not publish a revision");
                }
            }
        } finally { scopes.close(); }
        System.out.println(createWins ? "CLAIMED_HISTORICAL_CREATE_WINS_OK" : "CLAIMED_HISTORICAL_STAGE_WINS_OK");
    }

    private static long count(Tx tx, String table, DocumentPublicationCommand command) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                .setParameter("op", command.operationId()).getSingleResult()).longValue());
    }
    private static void expectDenied(Runnable action) {
        try { action.run(); throw new AssertionError("Revoked caller was allowed to inspect committed state"); }
        catch (RepositoryException denied) { require(denied.code() == RepositoryException.Code.UNAUTHENTICATED, "exact credential denial"); }
    }
    private static void awaitRevoker(Tx tx, int writer) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            var rows = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pid FROM pg_stat_activity WHERE :writer=ANY(pg_blocking_pids(pid))
                      AND wait_event_type='Lock' AND query ILIKE '%UPDATE repository_credential_authorities%'
                    """).setParameter("writer", writer).getResultList());
            if (rows.size() == 1) return;
            require(rows.isEmpty(), "single revoker waits on the exact writer");
            Thread.sleep(10);
        }
        throw new AssertionError("Credential revocation did not block on the held writer");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}

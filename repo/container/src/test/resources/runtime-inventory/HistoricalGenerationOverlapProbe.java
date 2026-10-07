package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;

/** Holds actual historical reader ownership across a second generation's provider publication. */
final class HistoricalGenerationOverlapProbe implements AutoCloseable {
    private final Tx tx;
    private final RepositoryCaller caller;
    private final RepositoryCaller coordinator;
    private final RepositoryInstalledHistoricalAttempts attempts;
    private final DocumentPublicationCommand command;
    private final DocumentReadLedger reads;
    private DocumentReadLedger.PinnedHistory history;
    private DocumentHistoricalAssessmentSources sources;
    private DocumentHistoricalAssessmentSources.Work worker;
    private UUID oldId;
    private UUID newId;

    HistoricalGenerationOverlapProbe(Tx tx, RepositoryCaller caller, RepositoryCaller coordinator,
            RepositoryInstalledHistoricalAttempts attempts, DocumentPublicationCommand command) {
        this.tx = tx; this.caller = caller; this.coordinator = coordinator;
        this.attempts = attempts; this.command = command;
        reads = new DocumentReadLedger(tx, UUID.randomUUID());
    }

    RepositorySuccessorInstall.Plan takeOver(RepositorySuccessorInstall.Plan oldPlan,
            Map<String, DocumentPublicationCandidate.Mode> modes,
            Map<DocumentUploadPayloads.Key, PartObject> bodies, SqlTimeouts timeouts) throws Exception {
        var selector = command.intent().getMembers(0).getPartsList().stream()
                .filter(p -> p.hasHistoricalReuse()).findFirst().orElseThrow().getHistoricalReuse();
        history = reads.captureHistorical(caller, selector.getSource(), UUID.fromString(selector.getRevisionId()));
        sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
        try (var old = attempts.resume(caller, command).orElseThrow()) {
            oldId = old.identity();
            var accepted = sources.work();
            try {
                old.attachSources(sources, accepted, RepositoryReadControl.NONE);
            } catch (Exception | Error failure) { accepted.close(); throw failure; }
            old.openExecution(coordinator, RepositoryReadControl.NONE);
            worker = accepted.fork();
        }
        sources.close(); // The owner and independent worker now retain the accepted capture.
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:op
                """).setParameter("op", command.operationId()).getSingleResult());
        var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, timeouts)
                .inspect(coordinator, oldPlan.next().key(), command.sha256(), RepositoryReadControl.NONE);
        require(observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND, "actual old claim and owner expired");
        try (var next = attempts.beginSuccessor(coordinator, caller, oldId, command, modes, observed,
                Duration.ofMinutes(2), timeouts)) {
            newId = next.identity();
            require(!newId.equals(oldId), "separate generation identity");
            require(next.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                    == RepositoryHistoricalAttemptPreparation.Phase.RESERVED, "reserve successor while old Work held");
        }
        RepositorySuccessorInstall.Plan plan;
        try (var next = attempts.resume(caller, command).orElseThrow()) {
            require(next.identity().equals(newId), "retry selects new generation");
            require(next.advancePreparation(coordinator, modes, bodies, RepositoryReadControl.NONE)
                    == RepositoryHistoricalAttemptPreparation.Phase.INSTALLED, "install before fresh successor capture");
            plan = next.installedPlan(coordinator, RepositoryReadControl.NONE);
        }
        require(!history.isReleased(), "old capture remains retained after takeover");
        require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 2)), "both generations retained");
        return plan;
    }

    void verifyAndRetire() throws Exception {
        // Called only after actual provider readback and exact committed receipt verification.
        require(!history.isReleased() && worker != null, "old worker still holds history after successor publication");
        require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 2)), "publication retains both generations");
        try (var old = attempts.resumeGeneration(coordinator, caller, command, oldId).orElseThrow()) {
            require(old.retireFenced(coordinator, Duration.ZERO, RepositoryReadControl.NONE)
                    == RepositoryInstalledHistoricalAttempts.Retirement.RETAINED, "old retirement waits for actual worker drainage");
        }
        require(!history.isReleased(), "retirement attempt cannot release held capture");
        releaseWorker();
        try (var old = attempts.resumeGeneration(coordinator, caller, command, oldId).orElseThrow()) {
            require(old.retireFenced(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                    == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED, "drained old generation retires independently");
        }
        require(history.isReleased(), "old capture released after actual worker drainage");
        try (var next = attempts.resume(caller, command).orElseThrow()) {
            require(next.identity().equals(newId), "old retirement preserves successor retry identity");
        }
        System.out.println("SCOPED_HISTORICAL_GENERATION_OVERLAP_PUBLICATION_OK");
    }

    void releaseWorker() { if (worker != null) { worker.close(); worker = null; } }

    @Override public void close() throws Exception {
        releaseWorker();
        if (sources != null) sources.close();
        if (history != null) {
            history.close();
            require(history.awaitDrained(Duration.ofSeconds(1)), "overlap history truly drained before release");
            history.release();
        }
        reads.fence(); reads.attestLocalQuiescence();
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}

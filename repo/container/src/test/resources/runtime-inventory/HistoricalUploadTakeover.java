package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublishedRevision;
import java.time.Duration;
import java.util.*;

/** Successor installation during a predecessor PUT, then independent successor staging. */
final class HistoricalUploadTakeover {
    static RepositorySuccessorInstall.Plan install(Tx tx, PayloadBudget budget, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord previous, UUID incarnation) {
        var key = previous.key();
        var token = tx.readOnly(em -> (UUID) em.createNativeQuery("SELECT claim_token FROM repository_execution_claims WHERE operation_id=:o")
                .setParameter("o", key.operationId()).getSingleResult());
        var identity = new RepositoryCoordinatorDrain.Identity(key, previous.command().sha256(), 1, token, incarnation);
        var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity, UUID.randomUUID(), UUID.randomUUID(),
                Duration.ofMinutes(2), new RepositoryCoordinatorReservation.OwnerIdentity(1, previous.seeds().ownerNonce()));
        RepositoryCoordinatorExpiration.reserve(tx, coordinator, reservation, RepositoryReadControl.NONE);
        var plan = RepositorySuccessorInstall.prepare(reservation, previous, Duration.ofMinutes(2),
                Map.of("a", DocumentPublicationCandidate.Mode.TYPED));
        RepositorySuccessorInstall.install(tx, budget, coordinator, plan, RepositoryReadControl.NONE);
        return plan;
    }

    static void stage(Tx tx, PayloadBudget budget, RepositoryCaller caller, RepositoryCaller coordinator,
            DocumentPublicationPreparationRecord previous, RepositorySuccessorInstall.Plan plan,
            DocumentPublishedRevision source, DocumentUploadCoordinator uploads, Map<DocumentUploadPayloads.Key, PartObject> bodies) throws Exception {
        long before = budget.reservedBytes();
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var history = reads.captureHistorical(caller, source.getAddress(), UUID.fromString(source.getRevisionId()));
        var scopes = new DocumentPublicationScopeCalls();
        RepositoryHistoricalSuccessorActivation activation = null;
        try (var sources = DocumentHistoricalAssessmentSources.open(previous.command(), caller, List.of(history), RepositoryReadControl.NONE);
             var accepted = sources.work()) {
            activation = new RepositoryHistoricalSuccessorActivation(tx, budget, plan, previous, sources, new DriveLedger(tx));
            try (var execution = activation.openExecution(coordinator, caller, accepted, scopes, RepositoryReadControl.NONE)) {
                var staged = execution.stageUploads(caller, uploads, bodies, Map.of(), RepositoryReadControl.NONE);
                require(staged.members().size() == 1, "successor verifies one fresh attempt");
                var selected = staged.members().getFirst().selection();
                require(selected.attempt().equals(plan.next().seeds().attempts().get("a"))
                        && selected.token().equals(plan.next().seeds().uploadTokens().get("a")), "successor uses installed seeds");
                require(!selected.attempt().equals(previous.seeds().attempts().get("a"))
                        && !selected.token().equals(previous.seeds().uploadTokens().get("a")), "successor cannot adopt predecessor attempt");
                var replay = execution.stageUploads(caller, uploads, bodies, Map.of(), RepositoryReadControl.NONE);
                require(replay.members().getFirst().selection().equals(selected), "successor replay preserves selection");
            }
        } finally {
            scopes.close();
            try {
                if (activation != null) require(activation.disposeCapture(coordinator, Duration.ofSeconds(2), RepositoryReadControl.NONE).isPresent(),
                        "successor capture disposal completes");
            } finally {
                history.close(); reads.releaseDrained(16);
                require(reads.outstandingReads() == 0 && scopes.isIdle() && budget.reservedBytes() == before, "successor local resources release");
                reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}

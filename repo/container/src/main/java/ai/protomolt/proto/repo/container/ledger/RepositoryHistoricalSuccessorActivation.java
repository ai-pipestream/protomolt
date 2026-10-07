package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.*;

/** Private activation and capture only. Does not attach an execution session or release retention. */
final class RepositoryHistoricalSuccessorActivation {
    private final Tx tx;
    private final PayloadBudget budget;
    private final RepositorySuccessorInstall.Plan plan;
    private final DocumentPublicationPreparationRecord retention;
    private final DocumentHistoricalAssessmentSources sources;
    private final DriveLedger drives;
    private DocumentPreparationCaptureDrain.Capture tentativeCapture;
    private byte[] capturedDigest;

    RepositoryHistoricalSuccessorActivation(Tx tx, PayloadBudget budget, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention, DocumentHistoricalAssessmentSources sources, DriveLedger drives) {
        this.tx = Objects.requireNonNull(tx); this.budget = Objects.requireNonNull(budget);
        this.plan = Objects.requireNonNull(plan); this.retention = Objects.requireNonNull(retention);
        this.sources = Objects.requireNonNull(sources); this.drives = Objects.requireNonNull(drives);
        if (!retention.key().equals(plan.next().key())
                || !retention.command().sha256().equals(plan.next().command().sha256())
                || retention.predecessorGeneration() >= plan.next().predecessorGeneration())
            throw new IllegalArgumentException("Historical activation retention scope differs");
    }

    /** Retain this attempt across an uncertain reply. The capability can also drain rolled-back local work. */
    synchronized Optional<DocumentPreparationCaptureDrain.Capture> tentativeCapture() {
        return Optional.ofNullable(tentativeCapture);
    }

    synchronized DocumentPreparationCaptureDrain.Capture activate(RepositoryCaller coordinator,
            RepositoryCaller executionCaller, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        var next = plan.next();
        DocumentAdmissionAuthorization.requireCaller(coordinator, next.key(), next.key().account());
        DocumentAdmissionAuthorization.requireCaller(executionCaller, next.key(), next.key().account());
        if (!coordinator.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Historical activation requires private process authority");
        try (var reserved = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES
                + DocumentPreparationSourcePins.MAX_BYTES + 1024 * 1024)) {
            var sha = digest(next); var previousSha = digest(plan.previous()); var retainedSha = digest(retention);
            var modes = RepositorySuccessorInstall.encodeModes(plan);
            if (tentativeCapture != null) {
                boolean committed = tx.inTransaction(em -> { return confirms(em, sha, retainedSha, modes); });
                control.check();
                if (committed) return tentativeCapture; // Durable fact only; no new lease or borrowed source access.
            }
            if (RepositoryCoordinatorReservation.confirm(tx, coordinator, plan.reservation(), control).isEmpty()
                    || !RepositorySuccessorInstall.confirm(tx, coordinator, plan, previousSha, sha, modes, control))
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Historical successor is not installed");
            try (var work = sources.work()) {
                work.requireCaller(executionCaller);
                var references = work.references(next.command(), control::check);
                var prepared = DocumentOperationUploadAdmission.prepareHistorical(next.command(), next.placements(),
                        next.seeds().attempts(), next.lease(), next.seeds().uploadTokens(), references, control::check).plan();
                var authorization = DocumentAdmissionAuthorization.prepare(prepared, prepared.historical());
                var creation = DocumentCreationAuthorization.prepare(prepared, drives, executionCaller);
                var pins = DocumentPreparationSourcePins.prepare(next.command(), references, control::check);
                var reuse = DocumentReuseAdmission.prepare(prepared);
                var objects = prepared.members().stream().flatMap(m -> m.intent().getPartsList().stream())
                        .filter(p -> p.hasReuse() || p.hasHistoricalReuse())
                        .map(p -> UUID.fromString(p.hasReuse() ? p.getReuse().getObject().getObjectId()
                                : p.getHistoricalReuse().getObject().getObjectId()))
                        .collect(java.util.stream.Collectors.toUnmodifiableSet());
                tx.inTransaction(em -> {
                    control.check();
                    RepositorySuccessorExecution.insertExecution(em, plan, sha, modes, true);
                    DocumentAdmissionAuthorization.lockAndAuthorize(em, executionCaller, prepared, authorization, creation);
                    for (var placement : next.placements().values().stream()
                            .sorted(Comparator.comparing(p -> p.drive().id())).toList()) {
                        control.check(); placement.drive().lock(em, drives);
                        if (ManagedBackendLedger.find(em, placement.generation()).filter(placement.profile()::equals).isEmpty())
                            throw new IllegalArgumentException("Historical activation backend placement differs");
                    }
                    RepositorySuccessorExecution.insertBinding(em, plan);
                    var claim = RepositoryExecutionClaimLedger.lockLive(em, next.key(), next.command().sha256(),
                            plan.reservation().predecessor().epoch()+1, plan.reservation().successorToken());
                    lockRetention(em, retainedSha);
                    DocumentReuseAdmission.requireBoundSources(em, reuse);
                    var origins = DocumentPublicationLocks.lockIndependentOrigins(em, authorization.destinations(), objects, Set.of());
                    DocumentPublicationLocks.lockIndependentRetention(em, origins);
                    for (var source : references) DocumentHistoricalReferenceAdmission.requireBoundSources(em, source, origins, control);
                    tentativeCapture = DocumentPreparationCaptureDrain.register(tx, em, retention, pins, claim,
                            plan.reservation().successorIncarnation(), sources, work, control);
                    capturedDigest = pins.digest();
                    binding(em.createNativeQuery("""
                            INSERT INTO repository_historical_activations(account_id,principal,operation_id,claim_epoch,
                              claim_token,incarnation,predecessor_generation,preparation_sha256,command_sha256,
                              retention_generation,retention_sha256,pins_sha256)
                            VALUES(:a,:p,:o,:e,:token,:incarnation,:g,:sha,:command,:retained,:retainedSha,:pins)
                            """), sha, retainedSha).executeUpdate();
                    control.check();
                });
                control.check(); return tentativeCapture;
            }
        }
    }

    private void lockRetention(EntityManager em, byte[] retainedSha) {
        var rows = scope(em.createNativeQuery("""
                SELECT preparation_sha256 FROM repository_publication_preparations
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:retained FOR UPDATE
                """)).setParameter("retained", retention.predecessorGeneration()).getResultList();
        if (rows.size()!=1 || !MessageDigest.isEqual((byte[]) rows.getFirst(), retainedSha)) throw unavailable();
        var roots = scope(em.createNativeQuery("""
                SELECT h.predecessor_generation FROM repository_preparation_history_sets h
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:retained
                AND EXISTS(SELECT 1 FROM repository_preparation_pin_batches b
                  JOIN repository_preparation_pin_owners own USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                  WHERE b.account_id=h.account_id AND b.principal=h.principal AND b.operation_id=h.operation_id
                    AND b.predecessor_generation=h.predecessor_generation AND b.initial_capture AND b.sealed
                    AND b.creation_xid=h.creation_xid)
                FOR UPDATE OF h
                """)).setParameter("retained", retention.predecessorGeneration()).getResultList();
        if (roots.isEmpty() || DocumentPreparationHistoryRoots.coverage(em, retention, retainedSha)
                != DocumentPreparationHistoryRoots.Coverage.EXACT) throw unavailable();
    }

    private boolean confirms(EntityManager em, byte[] sha, byte[] retainedSha, String modes) {
        if (!RepositorySuccessorExecution.read(em, plan, sha, modes)) return false;
        var rows = scope(em.createNativeQuery("""
                SELECT claim_token,incarnation,predecessor_generation,preparation_sha256,command_sha256,
                  retention_generation,retention_sha256,pins_sha256
                FROM repository_historical_activations WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:e
                """)).setParameter("e", plan.reservation().predecessor().epoch()+1).getResultList();
        if (rows.isEmpty()) throw unavailable();
        var r = (Object[]) rows.getFirst();
        if (!plan.reservation().successorToken().equals(r[0]) || !plan.reservation().successorIncarnation().equals(r[1])
                || ((Number) r[2]).longValue()!=plan.next().predecessorGeneration() || !MessageDigest.isEqual(sha,(byte[]) r[3])
                || !plan.next().command().sha256().equals(HexFormat.of().formatHex((byte[]) r[4]))
                || ((Number) r[5]).longValue()!=retention.predecessorGeneration()
                || !MessageDigest.isEqual(retainedSha,(byte[]) r[6]) || !MessageDigest.isEqual(capturedDigest,(byte[]) r[7]))
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Historical activation differs from this capture");
        return true;
    }

    private Query binding(Query q, byte[] sha, byte[] retainedSha) {
        return scope(q).setParameter("e", plan.reservation().predecessor().epoch()+1)
                .setParameter("token", plan.reservation().successorToken()).setParameter("incarnation", plan.reservation().successorIncarnation())
                .setParameter("g", plan.next().predecessorGeneration()).setParameter("sha", sha)
                .setParameter("command", HexFormat.of().parseHex(plan.next().command().sha256()))
                .setParameter("retained", retention.predecessorGeneration()).setParameter("retainedSha", retainedSha).setParameter("pins", capturedDigest);
    }
    private Query scope(Query q) {
        return q.setParameter("a", retention.key().account()).setParameter("p", retention.key().principal()).setParameter("o", retention.key().operationId());
    }
    private static byte[] digest(DocumentPublicationPreparationRecord record) {
        return DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record));
    }
    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Historical activation retention binding is unavailable");
    }
}

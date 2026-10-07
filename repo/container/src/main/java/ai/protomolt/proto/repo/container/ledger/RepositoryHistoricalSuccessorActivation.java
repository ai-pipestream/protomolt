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
    private boolean closing;
    enum Disposal { NO_CAPTURE, LOCAL_ONLY, REGISTERED }

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

    /** Retain this attempt across an uncertain reply; disposal must classify its durable state. */
    synchronized Optional<DocumentPreparationCaptureDrain.Capture> tentativeCapture() {
        return Optional.ofNullable(tentativeCapture);
    }

    /** Stops this local activation permanently. NO_CAPTURE leaves preactivation resources with their caller. */
    Optional<Disposal> disposeCapture(RepositoryCaller coordinator, java.time.Duration timeout,
            RepositoryReadControl control) throws InterruptedException {
        Objects.requireNonNull(control).check();
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative capture disposal wait");
        DocumentAdmissionAuthorization.requireCaller(coordinator, plan.next().key(), plan.next().key().account());
        if (!coordinator.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Historical capture disposal requires private process authority");
        final DocumentPreparationCaptureDrain.Capture capture;
        final RepositoryHistoricalCaptureState.State state;
        synchronized (this) {
            closing = true;
            capture = tentativeCapture;
            if (capture == null) return Optional.of(Disposal.NO_CAPTURE);
            state = RepositoryHistoricalCaptureState.classify(tx, budget, plan, retention, capture.identity(), control);
        }
        // Neither the activation monitor nor the SQL claim lock spans local worker/read drainage.
        if (state == RepositoryHistoricalCaptureState.State.REGISTERED)
            return capture.complete(coordinator, timeout, control).map(ignored -> Disposal.REGISTERED);
        return capture.releaseLocal(timeout, control) ? Optional.of(Disposal.LOCAL_ONLY) : Optional.empty();
    }

    synchronized DocumentPreparationCaptureDrain.Capture activate(RepositoryCaller coordinator,
            RepositoryCaller executionCaller, RepositoryReadControl control) {
        return activateWithWork(coordinator, executionCaller, control, null);
    }

    /** Continue already accepted source work; the caller retains its permit across an uncertain reply. */
    synchronized DocumentPreparationCaptureDrain.Capture activateAccepted(RepositoryCaller coordinator,
            RepositoryCaller executionCaller, RepositoryReadControl control,
            DocumentHistoricalAssessmentSources.Work accepted) {
        try (var continuation = Objects.requireNonNull(accepted).fork()) {
            continuation.histories(sources); // Reject a permit from a different capture, even for the same command.
            continuation.requireCaller(executionCaller);
            continuation.authorize(control);
            return activateWithWork(coordinator, executionCaller, control, continuation);
        }
    }

    /** Accepted source ownership and exact local capture are required in addition to durable activation. */
    synchronized DocumentHistoricalExecution openExecution(RepositoryCaller coordinator, RepositoryCaller caller,
            DocumentHistoricalAssessmentSources.Work accepted, DocumentPublicationScopeCalls scopes,
            RepositoryReadControl control) {
        return openWithScope(coordinator, caller, accepted, control, scopes.enter());
    }

    /** Continue a live host call after new admission closes, on the same shutdown barrier. */
    synchronized DocumentHistoricalExecution openAcceptedExecution(RepositoryCaller coordinator, RepositoryCaller caller,
            DocumentHistoricalAssessmentSources.Work accepted, DocumentPublicationScopeCalls scopes,
            DocumentPublicationScopeCalls.Call acceptedCall, RepositoryReadControl control) {
        return openWithScope(coordinator, caller, accepted, control,
                Objects.requireNonNull(acceptedCall).forkAccepted(scopes));
    }

    private DocumentHistoricalExecution openWithScope(RepositoryCaller coordinator, RepositoryCaller caller,
            DocumentHistoricalAssessmentSources.Work accepted, RepositoryReadControl control,
            DocumentPublicationScopeCalls.Call scope) {
        try {
            var capture = activateAccepted(coordinator, caller, control, accepted);
            return DocumentHistoricalSuccessorExecution.open(tx, budget, plan, retention, sources, capture,
                    accepted, caller, drives, control, scope);
        } catch (RuntimeException | Error failure) {
            scope.close(); throw failure;
        }
    }

    private DocumentPreparationCaptureDrain.Capture activateWithWork(RepositoryCaller coordinator,
            RepositoryCaller executionCaller, RepositoryReadControl control,
            DocumentHistoricalAssessmentSources.Work accepted) {
        Objects.requireNonNull(control).check();
        if (closing) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Historical activation is closing");
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
            try (var work = accepted == null ? sources.work() : accepted.fork()) {
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
        DocumentHistoricalRetentionBinding.require(em, retention, retainedSha, true);
    }

    private boolean confirms(EntityManager em, byte[] sha, byte[] retainedSha, String modes) {
        var evidence = RepositoryHistoricalActivationEvidence.read(em, plan, retention, sha, retainedSha, modes);
        if (evidence.isEmpty()) return false;
        if (!evidence.orElseThrow().captureSha256().equals(HexFormat.of().formatHex(capturedDigest)))
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

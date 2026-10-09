package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;

/** Initial registration lifecycle borrowed exclusively by the retained generation owner. */
final class RepositoryInitialHistoricalAttempt {
    private final Tx tx;
    private final PayloadBudget budget;
    private final DocumentPublicationPreparationRecord record;
    private final Map<String, DocumentPublicationCandidate.Mode> modes;
    private final DocumentPublicationRegistration registration;
    private final DocumentHistoricalAssessmentSources.Work work;
    private boolean closing;

    RepositoryInitialHistoricalAttempt(Tx tx, PayloadBudget budget, DocumentPublicationPreparationRecord record,
            Map<String, DocumentPublicationCandidate.Mode> modes, UUID incarnation,
            DocumentHistoricalAssessmentSources sources, DocumentHistoricalAssessmentSources.Work work,
            DocumentPublicationScopeCalls scopes, DriveLedger drives, RepositoryReadControl control) {
        this.tx = tx; this.budget = budget; this.record = record; this.modes = Map.copyOf(modes); this.work = work;
        registration = DocumentPublicationRegistration.historicalAccepted(tx, budget, record, sources, work,
                incarnation, scopes, drives, control);
    }

    RepositoryCoordinatorDrain.Identity identity() { return registration.drainIdentity(); }

    synchronized DocumentHistoricalExecution open(RepositoryCaller coordinator, RepositoryCaller caller,
            RepositoryReadControl control) {
        if (closing) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Initial historical attempt is closing");
        requireCoordinator(coordinator, record.key(), control);
        work.requireCaller(caller); work.authorize(control);
        var capture = registration.historicalCapture();
        RepositoryOperationLedger.Owner owner;
        if (capture.isPresent() && RepositoryInitialHistoricalCaptureState.classify(tx, budget, coordinator,
                record, modes, identity(), capture.orElseThrow().identity(), control)
                == RepositoryInitialHistoricalCaptureState.State.REGISTERED) {
            // Restore only the exact live initial owner. Persisted START is never reopened here.
            owner = tx.inTransaction(em -> {
                control.check();
                var claim = RepositoryExecutionClaimLedger.lockLive(em, record.key(), record.command().sha256(),
                        identity().epoch(), identity().token());
                return RepositoryOperationLedger.lockLiveOwner(em, record.key(), 1, record.seeds().ownerNonce(), Optional.of(claim));
            });
        } else {
            owner = registration.admitInitialAccepted(caller, modes, control, work).orElseThrow(() ->
                    new RepositoryException(RepositoryException.Code.CONFLICT, "Initial historical owner is unavailable"));
        }
        control.check();
        return registration.historicalExecutionAccepted(caller, owner, modes, control, work);
    }

    /** Called after the generation has joined its calls, closed execution and released its root Work. */
    Optional<RepositoryHistoricalSuccessorActivation.Disposal> dispose(RepositoryCaller coordinator,
            Duration timeout, RepositoryReadControl control) throws InterruptedException {
        final DocumentPreparationCaptureDrain.Capture capture;
        final RepositoryInitialHistoricalCaptureState.State state;
        synchronized (this) {
            requireCoordinator(coordinator, record.key(), control);
            closing = true;
            capture = registration.historicalCapture().orElse(null);
            if (capture == null) return Optional.of(RepositoryHistoricalSuccessorActivation.Disposal.NO_CAPTURE);
            state = RepositoryInitialHistoricalCaptureState.classify(tx, budget, coordinator, record, modes,
                    identity(), capture.identity(), control);
        }
        if (state == RepositoryInitialHistoricalCaptureState.State.REGISTERED)
            return capture.complete(coordinator, timeout, control).map(ignored -> RepositoryHistoricalSuccessorActivation.Disposal.REGISTERED);
        return capture.releaseLocal(timeout, control)
                ? Optional.of(RepositoryHistoricalSuccessorActivation.Disposal.LOCAL_ONLY) : Optional.empty();
    }

    static void requireCoordinator(RepositoryCaller caller, RepositoryOperationLedger.Key key, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Initial historical ownership requires private process authority");
    }
}

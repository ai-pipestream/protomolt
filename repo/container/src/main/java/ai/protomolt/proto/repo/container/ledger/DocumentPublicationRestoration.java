package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Private, stage-only reconciliation under an explicitly supplied original live owner.
 * Does not discover claim tokens, establish coordinator death, or transfer execution.
 */
final class DocumentPublicationRestoration implements AutoCloseable {
    private final DocumentPublicationPreparationJournal.Loaded preparation;
    private final RepositoryOperationLedger.Owner owner;
    private final DocumentAssessmentStartJournal.Started started;
    // Idle, executing, closed. Closing must not release borrowed state during execution.
    private final AtomicInteger state = new AtomicInteger();

    private DocumentPublicationRestoration(DocumentPublicationPreparationJournal.Loaded preparation,
            RepositoryOperationLedger.Owner owner, DocumentAssessmentStartJournal.Started started) {
        this.preparation = preparation; this.owner = owner; this.started = started;
    }

    static DocumentPublicationRestoration restore(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(owner);
        DocumentAdmissionAuthorization.requireCaller(caller, owner.key(), owner.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Publication restoration requires private process authority");
        var claim = owner.executionClaim().orElseThrow(() -> new IllegalArgumentException("Restoration requires execution claim"));
        var loaded = new DocumentPublicationPreparationJournal(tx, budget)
                .load(caller, claim, owner.generation() - 1, control).orElseThrow(() -> missing("preparation"));
        boolean transferred = false;
        try {
            var record = loaded.record();
            if (!record.key().equals(owner.key()) || !record.seeds().ownerNonce().equals(owner.token())
                    || record.predecessorGeneration() != owner.generation() - 1
                    || !record.command().sha256().equals(claim.commandSha256()))
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Original owner differs from retained preparation");
            // load validates fixed modes, the exact command and live owner as well as V83.
            var started = new DocumentAssessmentStartJournal(tx, budget)
                    .load(caller, owner, record.command(), control).orElseThrow(() -> missing("assessment start"));
            control.check();
            var result = new DocumentPublicationRestoration(loaded, owner, started);
            transferred = true;
            return result;
        } finally { if (!transferred) loaded.close(); }
    }

    /** No bodies or resolver: this handle cannot stage, upload, admit or take over. */
    DocumentPublicationResult resume(RepositoryCaller caller, DocumentPublicationAssessmentExecution assessments,
            RepositoryReadControl control) {
        Objects.requireNonNull(assessments); Objects.requireNonNull(control).check();
        if (!state.compareAndSet(0, 1)) throw new IllegalStateException("Restoration is executing or closed");
        try {
            DocumentAdmissionAuthorization.requireCaller(caller, owner.key(), owner.key().account());
            return assessments.resume(caller, owner, preparation.record().command(), started, control);
        } finally { state.set(0); }
    }

    @Override public void close() {
        if (state.compareAndSet(0, 2)) preparation.close();
        else if (state.get() != 2) throw new IllegalStateException("Restoration is executing");
    }

    private static RepositoryException missing(String what) {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Retained publication " + what + " is absent; restoration cannot restage");
    }
}

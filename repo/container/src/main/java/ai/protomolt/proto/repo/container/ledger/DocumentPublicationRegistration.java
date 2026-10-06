package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Fixed initial-session registration; no replacement claim, provider work or authority escalation. */
final class DocumentPublicationRegistration {
    private final Tx tx;
    private final DocumentPublicationPreparationRecord preparation;
    private final UUID claimToken = UUID.randomUUID();
    private final JournalAccess access;
    private final DocumentUploadPlan.Prepared plan;
    private final DocumentAdmissionAuthorization.Prepared authorization;
    private final DocumentPublicationPreparationJournal preparations;
    private final DocumentPublicationModesJournal modes;
    private final DocumentAssessmentStartJournal starts;
    private volatile boolean mayHaveCommitted;

    DocumentPublicationRegistration(Tx tx, PayloadBudget budget, DocumentPublicationPreparationRecord preparation,
            DocumentUploadPlan.Prepared plan) {
        this.tx = Objects.requireNonNull(tx);
        this.preparation = Objects.requireNonNull(preparation);
        this.plan = Objects.requireNonNull(plan);
        if (!plan.command().sha256().equals(preparation.command().sha256())
                || !plan.command().operationId().equals(preparation.command().operationId()))
            throw new IllegalArgumentException("Registration plan differs from preparation");
        authorization = DocumentAdmissionAuthorization.prepare(plan);
        if (preparation.predecessorGeneration() != 0) throw new IllegalArgumentException("Initial registration requires no predecessor");
        access = new JournalAccess(preparation, claimToken);
        preparations = new DocumentPublicationPreparationJournal(tx, budget);
        modes = new DocumentPublicationModesJournal(tx, budget);
        starts = new DocumentAssessmentStartJournal(tx, budget);
    }

    DocumentAssessmentStartJournal.Started start(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            java.time.Duration retention, RepositoryReadControl control) {
        return starts.startOwned(access, caller, owner, preparation.command(), UUID.randomUUID(), retention, control);
    }

    void requireStart(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, RepositoryReadControl control) {
        access.requireOwner(caller, owner, preparation.command(), control);
        preflight(caller, control);
    }

    RepositoryExecutionClaimLedger.Claim register(RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> fixedModes, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, preparation.key(), preparation.key().account());
        if (fixedModes == null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Publication modes must be fixed before durable registration");
        preflight(caller, control);
        // Set before entering code that can commit; never infer absence from its exception.
        mayHaveCommitted = true;
        var claim = preparations.acquireInitial(caller, preparation, claimToken, control);
        modes.bindOwned(access, caller, claim, 0, fixedModes, control);
        control.check();
        return claim;
    }

    boolean mayHaveCommitted() { return mayHaveCommitted; }

    private void preflight(RepositoryCaller caller, RepositoryReadControl control) {
        control.check();
        DocumentAdmissionAuthorization.requireCaller(caller, preparation.key(), preparation.key().account());
        tx.inTransaction(em -> {
            DocumentAdmissionAuthorization.lockAndAuthorize(em, caller, plan, authorization);
            control.check(); return null;
        });
        control.check();
    }

    /** Host-held, nonserializable authority for this session's journal only; never a document grant. */
    static final class JournalAccess {
        private final RepositoryOperationLedger.Key key;
        private final String digest;
        private final UUID claimToken;
        private final UUID ownerNonce;

        private JournalAccess(DocumentPublicationPreparationRecord preparation, UUID claimToken) {
            key = preparation.key(); digest = preparation.command().sha256();
            ownerNonce = preparation.seeds().ownerNonce(); this.claimToken = claimToken;
        }

        void require(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim, long predecessor,
                RepositoryReadControl control) {
            Objects.requireNonNull(control).check();
            DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
            if (predecessor != 0 || !key.equals(claim.key()) || !digest.equals(claim.commandSha256())
                    || claim.epoch() != 1 || !claimToken.equals(claim.token()))
                throw new IllegalArgumentException("Assessment claim differs from registered session");
        }

        void requirePreparation(DocumentPublicationPreparationRecord preparation) {
            if (!key.equals(preparation.key()) || !digest.equals(preparation.command().sha256())
                    || preparation.predecessorGeneration() != 0 || !ownerNonce.equals(preparation.seeds().ownerNonce()))
                throw new IllegalArgumentException("Journal preparation differs from registered session");
        }

        void requireOwner(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
                ai.protomolt.proto.repo.spi.DocumentPublicationCommand command, RepositoryReadControl control) {
            require(caller, owner.executionClaim().orElseThrow(() ->
                    new IllegalArgumentException("Durable staging requires execution claim")), owner.generation()-1, control);
            if (!key.equals(owner.key()) || !ownerNonce.equals(owner.token())
                    || !digest.equals(command.sha256()) || !key.operationId().equals(command.operationId()))
                throw new IllegalArgumentException("Journal owner or command differs from registered session");
        }
        @Override public String toString() { return "JournalAccess[private]"; }
    }
}

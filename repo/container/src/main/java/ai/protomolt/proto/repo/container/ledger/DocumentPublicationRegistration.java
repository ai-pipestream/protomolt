package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Retained session journal identity for initial admission or installed successor attachment. */
final class DocumentPublicationRegistration {
    private final Tx tx;
    private final PayloadBudget budget;
    private final DocumentPublicationPreparationRecord preparation;
    private final UUID claimToken;
    private final long claimEpoch;
    private final RepositorySuccessorInstall.Plan successor;
    private final UUID coordinator;
    private final DocumentPublicationScopeCalls registrations;
    private final JournalAccess access;
    private final DocumentUploadPlan.Prepared plan;
    private final DocumentAdmissionAuthorization.Prepared authorization;
    private final DocumentPublicationModesJournal modes;
    private final DocumentAssessmentStartJournal starts;
    private volatile boolean mayHaveCommitted;

    DocumentPublicationRegistration(Tx tx, PayloadBudget budget, DocumentPublicationPreparationRecord preparation,
            DocumentUploadPlan.Prepared plan, UUID coordinator, DocumentPublicationScopeCalls registrations) {
        this(tx, budget, preparation, plan, coordinator, registrations, null);
    }

    private DocumentPublicationRegistration(Tx tx, PayloadBudget budget, DocumentPublicationPreparationRecord preparation,
            DocumentUploadPlan.Prepared plan, UUID coordinator, DocumentPublicationScopeCalls registrations,
            RepositorySuccessorInstall.Plan successor) {
        this.successor = successor;
        claimToken = successor == null ? UUID.randomUUID() : successor.reservation().successorToken();
        claimEpoch = successor == null ? 1 : successor.reservation().predecessor().epoch()+1;
        this.tx = Objects.requireNonNull(tx);
        this.budget = Objects.requireNonNull(budget);
        this.preparation = Objects.requireNonNull(preparation);
        this.plan = Objects.requireNonNull(plan);
        this.coordinator = Objects.requireNonNull(coordinator);
        this.registrations = Objects.requireNonNull(registrations);
        if (!plan.command().sha256().equals(preparation.command().sha256())
                || !plan.command().operationId().equals(preparation.command().operationId()))
            throw new IllegalArgumentException("Registration plan differs from preparation");
        authorization = DocumentAdmissionAuthorization.prepare(plan);
        if (successor == null && preparation.predecessorGeneration() != 0) throw new IllegalArgumentException("Initial registration requires no predecessor");
        access = new JournalAccess(preparation, claimToken, claimEpoch);
        mayHaveCommitted = successor != null;
        modes = new DocumentPublicationModesJournal(tx, budget);
        starts = new DocumentAssessmentStartJournal(tx, budget);
    }

    static DocumentPublicationRegistration successor(Tx tx, PayloadBudget budget, RepositorySuccessorInstall.Plan successor,
            UUID coordinator, DocumentPublicationScopeCalls registrations) {
        if (!successor.reservation().successorIncarnation().equals(coordinator))
            throw new IllegalArgumentException("Successor coordinator differs from installed incarnation");
        return new DocumentPublicationRegistration(tx, budget, successor.next(), successor.next().prepare().plan(), coordinator, registrations, successor);
    }

    RepositorySuccessorExecution.Attached attach(RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> fixedModes, RepositoryReadControl control) {
        if (successor == null) throw new IllegalStateException("Initial registration cannot attach successor");
        try (var scope = registrations.enter()) {
            if (!successor.modes().equals(fixedModes)) throw new IllegalArgumentException("Successor admission modes changed");
            var attached = RepositorySuccessorExecution.attach(tx, budget, caller, successor, control);
            var claim = attached.owner().executionClaim().orElseThrow();
            var stored = modes.loadOwned(access, caller, claim, preparation.predecessorGeneration(), control).orElseThrow(() ->
                    new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Successor modes are absent"));
            if (!stored.equals(fixedModes)) throw new IllegalArgumentException("Successor stored modes differ");
            return attached;
        }
    }

    DocumentAssessmentStartJournal.Started start(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            java.time.Duration retention, RepositoryReadControl control) {
        return starts.startOwned(access, caller, owner, preparation.command(), UUID.randomUUID(), retention, control);
    }

    void requireStart(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, RepositoryReadControl control) {
        access.requireOwner(caller, owner, preparation.command(), control);
        preflight(caller, control);
    }

    /** Initial journal and executable owner share one commit and one registration scope. */
    java.util.Optional<RepositoryOperationLedger.Owner> admitInitial(RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> fixedModes, RepositoryReadControl control) {
        if (successor != null) throw new IllegalStateException("Successor must attach installed owner");
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, preparation.key(), preparation.key().account());
        if (fixedModes == null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Publication modes must be fixed before durable registration");
        try (var scope = registrations.enter();
             var reserved = budget.reserve((long) DocumentPublicationPreparationCodec.MAX_BYTES + DocumentPublicationModesJournal.MAX_BYTES
                     + ai.protomolt.proto.repo.spi.DocumentPublicationCommand.MAX_COMMAND_BYTES)) {
            // Reject an immediately denied caller before marking the session uncertain.
            // Authorization is checked again under the claim lock before any domain writes.
            preflight(caller,control);
            var bytes = DocumentPublicationPreparationCodec.encode(preparation);
            var digest = DocumentPublicationPreparationJournal.digest(bytes);
            var encodedModes = DocumentPublicationModesJournal.encode(preparation.command(),fixedModes);
            var admission = RepositoryOperationLedger.prepareAdmission(preparation.key(),preparation.command(),
                    preparation.seeds().ownerNonce(),preparation.lease());
            control.check();
            mayHaveCommitted = true;
            var result = tx.inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireInitialInTransaction(em,preparation.key(),
                        preparation.command(),claimToken,preparation.lease());
                var claim = acquired.claim();
                RepositoryCoordinatorBinding.bindInitial(em,acquired,coordinator);
                DocumentAdmissionAuthorization.lockAndAuthorize(em,caller,plan,authorization);
                control.check();
                DocumentPublicationPreparationJournal.insert(em,claim,preparation,bytes,digest);
                DocumentPublicationModesJournal.insert(em,claim,preparation,encodedModes);
                control.check();
                var admitted = admission.apply(em,claim);
                control.check(); return admitted.owner();
            });
            control.check(); return result;
        }
    }

    boolean mayHaveCommitted() { return mayHaveCommitted; }

    RepositoryCoordinatorDrain.Identity drainIdentity() {
        return new RepositoryCoordinatorDrain.Identity(preparation.key(), preparation.command().sha256(), claimEpoch, claimToken, coordinator);
    }

    void abandon(RepositoryCaller caller, RepositoryReadControl control) {
        if (successor != null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Installed successor requires owner cancellation");
        DocumentPublicationAbandonment.abandonRetained(tx, budget, caller, claimToken, preparation, control);
    }

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
        private final long epoch;
        private final long predecessor;

        private JournalAccess(DocumentPublicationPreparationRecord preparation, UUID claimToken, long epoch) {
            this.epoch = epoch; predecessor = preparation.predecessorGeneration();
            key = preparation.key(); digest = preparation.command().sha256();
            ownerNonce = preparation.seeds().ownerNonce(); this.claimToken = claimToken;
        }

        void require(RepositoryCaller caller, RepositoryExecutionClaimLedger.Claim claim, long predecessor,
                RepositoryReadControl control) {
            Objects.requireNonNull(control).check();
            DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
            if (predecessor != this.predecessor || !key.equals(claim.key()) || !digest.equals(claim.commandSha256())
                    || claim.epoch() != epoch || !claimToken.equals(claim.token()))
                throw new IllegalArgumentException("Assessment claim differs from registered session");
        }

        void requirePreparation(DocumentPublicationPreparationRecord preparation) {
            if (!key.equals(preparation.key()) || !digest.equals(preparation.command().sha256())
                    || preparation.predecessorGeneration() != predecessor || !ownerNonce.equals(preparation.seeds().ownerNonce()))
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

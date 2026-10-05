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
    private final DocumentPublicationPreparationRecord preparation;
    private final UUID claimToken = UUID.randomUUID();
    private final RepositoryExecutionClaimLedger claims;
    private final DocumentPublicationPreparationJournal preparations;
    private final DocumentPublicationModesJournal modes;

    DocumentPublicationRegistration(Tx tx, PayloadBudget budget, DocumentPublicationPreparationRecord preparation) {
        this.preparation = Objects.requireNonNull(preparation);
        if (preparation.predecessorGeneration() != 0) throw new IllegalArgumentException("Initial registration requires no predecessor");
        claims = new RepositoryExecutionClaimLedger(tx);
        preparations = new DocumentPublicationPreparationJournal(tx, budget);
        modes = new DocumentPublicationModesJournal(tx, budget);
    }

    RepositoryExecutionClaimLedger.Claim register(RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> fixedModes, RepositoryReadControl control) {
        requireProcess(caller);
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, preparation.key(), preparation.key().account());
        if (fixedModes == null) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Publication modes must be fixed before durable registration");
        var claim = claims.acquire(preparation.key(), preparation.command(), claimToken, preparation.lease());
        control.check();
        preparations.save(caller, claim, preparation, control);
        modes.bind(caller, claim, 0, fixedModes, control);
        control.check();
        return claim;
    }

    static void requireProcess(RepositoryCaller caller) {
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Private session registration requires actual process authority");
    }
}

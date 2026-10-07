package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Preparation owned by one historical entry; never attaches ordinary publication sessions. */
final class RepositoryHistoricalAttemptPreparation implements AutoCloseable {
    enum Phase { PROPOSED, RESERVED, INSTALLED }
    private final Tx tx;
    private final PayloadBudget budget;
    private final SqlTimeouts timeouts;
    private final RepositoryCoordinatorReservation.Proposal proposal;
    private final DocumentPublicationCommand command;
    private final Map<String, DocumentPublicationCandidate.Mode> modes;
    private final Duration lease;
    private final DocumentPublicationPreparationRecord retention;
    private final byte[] retentionDigest;
    private Phase phase = Phase.PROPOSED;
    private RepositoryReservedPreparation.Loaded loaded;
    private PayloadBudget.Lease nextBytes;
    private RepositorySuccessorInstall.Plan plan;
    private boolean closed;

    RepositoryHistoricalAttemptPreparation(Tx tx, PayloadBudget budget, SqlTimeouts timeouts,
            RepositoryCoordinatorReservation.Proposal proposal, DocumentPublicationCommand command,
            Map<String, DocumentPublicationCandidate.Mode> modes, Duration lease,
            DocumentPublicationPreparationRecord retention, byte[] retentionDigest) {
        this.tx = tx.withTimeouts(timeouts); this.budget = budget; this.timeouts = timeouts;
        this.proposal = proposal; this.command = command; this.modes = Map.copyOf(modes); this.lease = lease;
        this.retention = retention; this.retentionDigest = retentionDigest.clone();
    }

    Phase phase() { requireOpen(); return phase; }
    boolean matches(Map<String, DocumentPublicationCandidate.Mode> requested, Duration lease, SqlTimeouts timeouts) {
        requireOpen(); return modes.equals(requested) && this.lease.equals(lease) && this.timeouts.equals(timeouts);
    }

    /** One phase per call; failed acknowledgments leave the exact proposal and plan retained. */
    Phase advance(RepositoryCaller coordinator, RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> requested,
            Map<DocumentUploadPayloads.Key, PartObject> payloads, RepositoryReadControl control) {
        requireOpen(); Objects.requireNonNull(control).check();
        if (!modes.equals(requested)) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                "Historical recovery modes changed");
        RepositoryCoordinatorReservation.require(coordinator, proposal, control);
        DocumentAdmissionAuthorization.requireCaller(caller, proposal.predecessor().key(), command.intent().getAccountId());
        // Validate bytes, not only caller-supplied hashes, before any reservation mutation.
        try (var snapshot = DocumentRecoveryPayloads.prepare(command, payloads, budget, control)) {
            tx.inTransaction(em -> {
                control.check(); DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
                DocumentHistoricalRetentionBinding.require(em, retention, retentionDigest, false);
                if (phase == Phase.PROPOSED && proposal instanceof RepositoryCoordinatorReservation.ExpiredUnquiesced expired)
                    DocumentPublicationModesJournal.requireBoundModes(em, expired.predecessor().key(), command,
                            expired.owner().generation(), DocumentPublicationModesJournal.encode(command, modes));
                control.check(); return null;
            });
            switch (phase) {
                case PROPOSED -> {
                    switch (proposal) {
                        case RepositoryCoordinatorReservation.ExpiredUnquiesced expired ->
                                RepositoryCoordinatorExpiration.reserve(tx, coordinator, expired, control);
                        case RepositoryCoordinatorReservation.SupersededUnactivated superseded -> {
                            new DocumentPublicationModesJournal(tx, budget)
                                    .requireSupersessionModes(coordinator, caller, command, superseded, modes, control);
                            RepositoryCoordinatorSupersession.reserve(tx, coordinator, superseded, control);
                        }
                        default -> throw new IllegalStateException("Unsupported historical recovery reservation");
                    }
                    phase = Phase.RESERVED;
                }
                case RESERVED -> {
                    if (loaded == null) loaded = new RepositoryReservedPreparation(tx, budget, timeouts).load(
                            coordinator, caller, proposal, RepositoryCoordinatorReservation.owner(proposal).orElseThrow(), control);
                    if (!loaded.modes().equals(modes)) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                            "Historical recovery modes differ from fixed modes");
                    if (plan == null) {
                        var bytes = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES);
                        try {
                            plan = RepositorySuccessorInstall.prepare(proposal, loaded.record(), lease, modes);
                            nextBytes = bytes;
                        } catch (RuntimeException | Error failure) { bytes.close(); throw failure; }
                    }
                    // install first confirms this exact plan. Never reload after an uncertain V93.
                    RepositorySuccessorInstall.install(tx, budget, coordinator, plan, control);
                    phase = Phase.INSTALLED;
                }
                case INSTALLED -> { /* No new identity, installation or activation on repeated advancement. */ }
            }
            control.check();
            return phase;
        }
    }

    RepositorySuccessorInstall.Plan installedPlan() {
        requireOpen();
        if (phase != Phase.INSTALLED) throw new IllegalStateException("Historical installation is not confirmed");
        return plan;
    }

    private void requireOpen() { if (closed) throw new IllegalStateException("Historical preparation is closed"); }
    @Override public void close() {
        if (closed) return;
        closed = true;
        if (loaded != null) loaded.close();
        if (nextBytes != null) nextBytes.close();
        loaded = null; plan = null;
    }
}

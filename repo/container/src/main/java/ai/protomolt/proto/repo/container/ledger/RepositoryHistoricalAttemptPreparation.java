package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;

/** Preparation owned by one historical entry; never attaches ordinary publication sessions. */
final class RepositoryHistoricalAttemptPreparation implements AutoCloseable {
    enum Phase { PROPOSED, RESERVED, INSTALLED }
    private final Tx tx;
    private final PayloadBudget budget;
    private final SqlTimeouts timeouts;
    private RepositoryCoordinatorReservation.Proposal proposal;
    private RepositoryCoordinatorReservation.SupersededUnactivated pending;
    private final DocumentPublicationCommand command;
    private final Map<String, DocumentPublicationCandidate.Mode> modes;
    private final Duration lease;
    private final DocumentPublicationPreparationRecord retention;
    private final byte[] retentionDigest;
    private RepositoryHistoricalRetentionLoader.Loaded recoveredRetention;
    private byte[] recoveredDigest;
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
        this.retention = Objects.requireNonNull(retention); this.retentionDigest = retentionDigest.clone();
    }

    /** Cold preparation owns metadata only; a later verified load is required before installation. */
    RepositoryHistoricalAttemptPreparation(Tx tx, PayloadBudget budget, SqlTimeouts timeouts,
            RepositoryCoordinatorReservation.Proposal proposal, DocumentPublicationCommand command,
            Map<String, DocumentPublicationCandidate.Mode> modes, Duration lease) {
        this.tx = Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts));
        this.budget = Objects.requireNonNull(budget); this.timeouts = timeouts;
        this.proposal = Objects.requireNonNull(proposal); this.command = Objects.requireNonNull(command);
        this.modes = Map.copyOf(modes); this.lease = Objects.requireNonNull(lease);
        retention = null; retentionDigest = null;
        if (!proposal.predecessor().commandSha256().equals(command.sha256())
                || !proposal.predecessor().key().operationId().equals(command.operationId())
                || !proposal.predecessor().key().account().equals(command.intent().getAccountId())
                || DocumentPreparationHistoryRoots.roots(command).isEmpty())
            throw new IllegalArgumentException("Cold historical proposal differs from command");
    }

    boolean cold() { return retention == null; }
    DocumentPublicationPreparationRecord requireResolvedRetention() {
        requireSettled();
        if (!cold()) return retention;
        if (recoveredRetention == null) throw new IllegalStateException("Historical retention anchor is unresolved");
        return recoveredRetention.record();
    }
    private void verifyRetentionIfResolved(jakarta.persistence.EntityManager em) {
        if (!cold()) DocumentHistoricalRetentionBinding.require(em, retention, retentionDigest, false);
        else if (recoveredRetention != null)
            DocumentHistoricalRetentionBinding.require(em, recoveredRetention.record(), recoveredDigest, false);
    }

    Phase phase() { requireOpen(); return phase; }
    RepositoryCoordinatorReservation.Proposal proposal() { requireOpen(); return proposal; }
    void requireSettled() {
        requireOpen();
        if (pending != null) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                "Historical supersession must be confirmed first");
    }
    List<RepositoryCoordinatorDrain.Identity> retainedClaims() {
        requireOpen();
        return pending == null ? List.of(successor(proposal)) : List.of(successor(proposal), successor(pending));
    }
    private static RepositoryCoordinatorDrain.Identity successor(RepositoryCoordinatorReservation.Proposal p) {
        var old = p.predecessor();
        return new RepositoryCoordinatorDrain.Identity(old.key(), old.commandSha256(), Math.addExact(old.epoch(), 1),
                p.successorToken(), p.successorIncarnation());
    }

    /** Replaces only an exact, expired, unactivated claim; pending identity survives an uncertain commit. */
    boolean reconcileUnactivated(RepositoryCaller coordinator, RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> requested,
            Map<DocumentUploadPayloads.Key, PartObject> payloads, RepositoryReadControl control) {
        requireOpen(); Objects.requireNonNull(control).check();
        RepositoryCoordinatorReservation.require(coordinator, proposal, control);
        DocumentAdmissionAuthorization.requireCaller(caller, proposal.predecessor().key(), command.intent().getAccountId());
        if (!modes.equals(requested)) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                "Historical recovery modes changed");
        try (var snapshot = DocumentRecoveryPayloads.prepare(command, payloads, budget, control)) {
            tx.inTransaction(em -> {
                control.check(); DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
                verifyRetentionIfResolved(em);
                control.check(); return null;
            });
            if (pending == null) {
                var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, timeouts)
                        .inspect(coordinator, proposal.predecessor().key(), command.sha256(), control);
                if (observed.unactivated().isEmpty()) return false;
                var source = observed.unactivated().orElseThrow();
                if (phase == Phase.PROPOSED && proposal.predecessor().equals(source.predecessor())) return false;
                if (!successor(proposal).equals(source.predecessor()))
                    throw new RepositoryException(RepositoryException.Code.CONFLICT,
                            "Historical successor differs from retained attempt");
                verifySource(coordinator, source, control);
                var replacement = new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),
                        UUID.randomUUID(), UUID.randomUUID(), lease, source.owner(), source.preparationSha256(), source.installation());
                new DocumentPublicationModesJournal(tx, budget)
                        .requireSupersessionModes(coordinator, caller, command, replacement, modes, control);
                pending = replacement;
            } else {
                new DocumentPublicationModesJournal(tx, budget)
                        .requireSupersessionModes(coordinator, caller, command, pending, modes, control);
            }
            RepositoryCoordinatorSupersession.reserve(tx, coordinator, pending, control);
            // Only exact commit confirmation lets us discard the old preparation metadata.
            releasePreparation();
            proposal = pending; pending = null; phase = Phase.RESERVED;
            return true;
        }
    }

    private void verifySource(RepositoryCaller coordinator,
            RepositoryCoordinatorRecoveryDiscovery.UnactivatedCandidate source, RepositoryReadControl control) {
        if (source.installation().isPresent()) {
            if (plan == null) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                    "Historical installation has no retained plan");
            try (var bytes = budget.reserve(2L * DocumentPublicationPreparationCodec.MAX_BYTES + 1024 * 1024)) {
                var oldSha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.previous()));
                var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.next()));
                if (!source.owner().equals(new RepositoryCoordinatorReservation.OwnerIdentity(
                        Math.addExact(plan.next().predecessorGeneration(), 1), plan.next().seeds().ownerNonce()))
                        || !source.preparationSha256().equals(HexFormat.of().formatHex(sha))
                        || !RepositorySuccessorInstall.confirm(tx, coordinator, plan, oldSha, sha,
                                RepositorySuccessorInstall.encodeModes(plan), control))
                    throw new RepositoryException(RepositoryException.Code.CONFLICT, "Historical installation binding differs");
            }
        } else {
            if (phase == Phase.INSTALLED || !source.owner().equals(RepositoryCoordinatorReservation.owner(proposal).orElseThrow()))
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Historical reservation owner differs");
            // Discovery binds the journal SHA to this exact owner. V98 rechecks that tuple atomically.
            // The ordinary loader requires a live lease and must not be used for expired claims.
            if (loaded == null) return;
            try (var bytes = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
                var sha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(loaded.record()));
                if (!source.preparationSha256().equals(HexFormat.of().formatHex(sha)))
                    throw new RepositoryException(RepositoryException.Code.CONFLICT, "Historical preparation binding differs");
            }
        }
    }
    boolean matches(Map<String, DocumentPublicationCandidate.Mode> requested, Duration lease, SqlTimeouts timeouts) {
        requireOpen(); return modes.equals(requested) && this.lease.equals(lease) && this.timeouts.equals(timeouts);
    }

    /** One phase per call; failed acknowledgments leave the exact proposal and plan retained. */
    Phase advance(RepositoryCaller coordinator, RepositoryCaller caller,
            Map<String, DocumentPublicationCandidate.Mode> requested,
            Map<DocumentUploadPayloads.Key, PartObject> payloads, RepositoryReadControl control) {
        requireSettled(); Objects.requireNonNull(control).check();
        if (!modes.equals(requested)) throw new RepositoryException(RepositoryException.Code.CONFLICT,
                "Historical recovery modes changed");
        RepositoryCoordinatorReservation.require(coordinator, proposal, control);
        DocumentAdmissionAuthorization.requireCaller(caller, proposal.predecessor().key(), command.intent().getAccountId());
        // Validate bytes, not only caller-supplied hashes, before any reservation mutation.
        try (var snapshot = DocumentRecoveryPayloads.prepare(command, payloads, budget, control)) {
            tx.inTransaction(em -> {
                control.check(); DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
                verifyRetentionIfResolved(em);
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
                    if (cold() && recoveredRetention == null) {
                        var anchor = new RepositoryHistoricalRetentionLoader(tx, budget, timeouts).load(
                                coordinator, caller, proposal, RepositoryCoordinatorReservation.owner(proposal).orElseThrow(),
                                loaded.record(), control);
                        try {
                            var record = anchor.record();
                            if (!record.command().canonical().equals(command.canonical())
                                    || !record.key().equals(proposal.predecessor().key()))
                                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Recovered retention command differs");
                            try (var scratch = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
                                recoveredDigest = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record));
                            }
                            recoveredRetention = anchor;
                        } catch (RuntimeException | Error failure) { anchor.close(); throw failure; }
                    }
                    requireResolvedRetention();
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
        requireSettled();
        if (phase != Phase.INSTALLED) throw new IllegalStateException("Historical installation is not confirmed");
        return plan;
    }

    private void requireOpen() { if (closed) throw new IllegalStateException("Historical preparation is closed"); }
    @Override public void close() {
        if (closed) return;
        closed = true;
        releasePreparation();
        pending = null;
    }
    private void releasePreparation() {
        if (loaded != null) loaded.close();
        if (recoveredRetention != null) recoveredRetention.close();
        if (nextBytes != null) nextBytes.close();
        loaded = null; plan = null; nextBytes = null; recoveredRetention = null; recoveredDigest = null;
    }
}

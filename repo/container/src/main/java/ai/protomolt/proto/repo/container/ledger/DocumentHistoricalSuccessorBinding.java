package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

/** Current executable preparation and original retained roots are deliberately separate. */
final class DocumentHistoricalSuccessorBinding {
    private final RepositorySuccessorInstall.Plan plan;
    private final DocumentPublicationPreparationRecord retention;
    private final DocumentPreparationCaptureDrain.Identity capture;
    private final DocumentPreparationSourcePins.Prepared pins;
    private final byte[] preparationDigest;
    private final byte[] preparationBytes;
    private final byte[] retentionDigest;
    private final String modes;
    private RepositoryHistoricalActivationEvidence.Receipt verified;

    DocumentHistoricalSuccessorBinding(RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention, DocumentPreparationCaptureDrain.Identity capture,
            DocumentPreparationSourcePins.Prepared pins) {
        this.plan = Objects.requireNonNull(plan); this.retention = Objects.requireNonNull(retention);
        this.capture = Objects.requireNonNull(capture); this.pins = Objects.requireNonNull(pins);
        var next = plan.next(); var reservation = plan.reservation();
        if (!retention.key().equals(next.key()) || !retention.command().sha256().equals(next.command().sha256())
                || retention.predecessorGeneration() >= next.predecessorGeneration()
                || capture.generation() != retention.predecessorGeneration()
                || !capture.owner().key().equals(next.key())
                || !capture.owner().commandSha256().equals(next.command().sha256())
                || capture.owner().epoch() != reservation.predecessor().epoch() + 1
                || !capture.owner().token().equals(reservation.successorToken())
                || !capture.owner().incarnation().equals(reservation.successorIncarnation())
                || !capture.pinsSha256().equals(HexFormat.of().formatHex(pins.digest())))
            throw new IllegalArgumentException("Successor execution capture differs from installed preparation");
        var encoded = DocumentPublicationPreparationCodec.encode(next);
        preparationBytes = encoded.toByteArray();
        preparationDigest = DocumentPublicationPreparationJournal.digest(encoded);
        retentionDigest = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(retention));
        modes = RepositorySuccessorInstall.encodeModes(plan);
    }

    long retainedBytes() { return preparationBytes.length; }

    /** Claim first, then immutable preparation/root rows, before origins or retention objects. */
    void lockRegistration(EntityManager em, RepositoryOperationLedger.Owner owner) {
        var claim = owner.executionClaim().orElseThrow();
        var next = plan.next();
        if (!owner.key().equals(next.key()) || owner.generation() != next.predecessorGeneration() + 1
                || !owner.token().equals(next.seeds().ownerNonce())
                || claim.epoch() != capture.owner().epoch() || !claim.token().equals(capture.owner().token()))
            throw new IllegalArgumentException("Successor execution owner differs from activation");
        RepositoryExecutionClaimLedger.lockLive(em, claim);
        var row = em.createNativeQuery("""
                SELECT owner_nonce,preparation_sha256,preparation_bytes=:bytes FROM repository_publication_preparations
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """).setParameter("a", next.key().account()).setParameter("p", next.key().principal())
                .setParameter("o", next.key().operationId()).setParameter("g", next.predecessorGeneration())
                .setParameter("bytes", preparationBytes).getResultList();
        if (row.size() != 1) throw corrupt();
        var current = (Object[]) row.getFirst();
        if (!owner.token().equals(current[0]) || !MessageDigest.isEqual(preparationDigest, (byte[]) current[1])
                || !Boolean.TRUE.equals(current[2])) throw corrupt();
        var roots = em.createNativeQuery("""
                SELECT p.preparation_sha256,h.preparation_sha256,h.command_sha256,h.sealed
                FROM repository_publication_preparations p JOIN repository_preparation_history_sets h
                  USING(account_id,principal,operation_id,predecessor_generation)
                WHERE p.account_id=:a AND p.principal=:p AND p.operation_id=:o AND p.predecessor_generation=:g
                FOR UPDATE OF p,h
                """).setParameter("a", retention.key().account()).setParameter("p", retention.key().principal())
                .setParameter("o", retention.key().operationId()).setParameter("g", retention.predecessorGeneration()).getResultList();
        if (roots.size() != 1) throw corrupt();
        var root = (Object[]) roots.getFirst();
        if (!MessageDigest.isEqual(retentionDigest, (byte[]) root[0])
                || !MessageDigest.isEqual(retentionDigest, (byte[]) root[1])
                || !retention.command().sha256().equals(HexFormat.of().formatHex((byte[]) root[2]))
                || !Boolean.TRUE.equals(root[3])) throw corrupt();
        em.createNativeQuery("""
                SELECT owner_nonce FROM repository_publication_modes
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """).setParameter("a", next.key().account()).setParameter("p", next.key().principal())
                .setParameter("o", next.key().operationId()).setParameter("g", next.predecessorGeneration()).getSingleResult();
        DocumentPublicationModesJournal.requireBoundModes(em, next.key(), next.command(), owner.generation(), modes);
        var receipt = RepositoryHistoricalActivationEvidence.read(em, plan, retention, preparationDigest, retentionDigest, modes)
                .orElseThrow(() -> new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Successor historical activation is absent"));
        if (!receipt.captureSha256().equals(capture.pinsSha256()) || !receipt.execution().equals(capture.owner())) throw corrupt();
        if (verified != null && !verified.equals(receipt)) throw corrupt();
        verified = receipt;
    }

    /** Full verification at attachment; no execution authority is conferred by this helper alone. */
    void requireCapture(EntityManager em, RepositoryOperationLedger.Owner owner, Runnable control) {
        if (verified == null) throw new IllegalStateException("Successor activation must be verified before its capture");
        if (DocumentPreparationHistoryRoots.coverage(em, retention, retentionDigest)
                != DocumentPreparationHistoryRoots.Coverage.EXACT) throw corrupt();
        DocumentPreparationSourcePins.requireSuccessor(em, retention, pins, owner.executionClaim().orElseThrow(),
                capture.owner().incarnation(), verified.activationTransaction(), control);
    }

    /** Immutable contents were verified on this handle; mutable lifetime checks remain mandatory. */
    void requireActiveCapture(EntityManager em, RepositoryOperationLedger.Owner owner, Runnable control) {
        if (verified == null) throw new IllegalStateException("Successor activation must be verified before its capture");
        DocumentPreparationSourcePins.requireActiveSuccessor(em, retention, pins, owner.executionClaim().orElseThrow(),
                capture.owner().incarnation(), verified.activationTransaction(), control);
    }

    private static RepositoryException corrupt() {

        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Successor historical execution binding differs");
    }
}

package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;
import java.util.HexFormat;

/** Private pre-owner cancellation. No takeover, provider cleanup or execution permission. */
final class DocumentPublicationAbandonment {
    private DocumentPublicationAbandonment() {}

    /** Immutable evidence only: no claim lookup, lease renewal, fencing or provider access. */
    static boolean confirm(Tx tx, PayloadBudget budget, RepositoryCaller caller, UUID token,
            DocumentPublicationPreparationRecord preparation, RepositoryReadControl control) {
        requirePrivate(caller, preparation, control);
        Objects.requireNonNull(token);
        try (var reservation = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            var expected = digest(preparation);
            control.check();
            var rows = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT a.owner_nonce,a.preparation_sha256,a.claim_epoch,a.claim_token,p.command_sha256
                    FROM repository_publication_abandonments a
                    JOIN repository_publication_preparations p USING(account_id,principal,operation_id,predecessor_generation)
                    WHERE a.account_id=:a AND a.principal=:p AND a.operation_id=:o
                    """).setParameter("a", preparation.key().account()).setParameter("p", preparation.key().principal())
                    .setParameter("o", preparation.key().operationId()).getResultList());
            control.check();
            if (rows.isEmpty()) return false;
            var row = (Object[]) rows.getFirst();
            if (!preparation.seeds().ownerNonce().equals(row[0]) || !java.util.Arrays.equals(expected, (byte[]) row[1])
                    || ((Number) row[2]).longValue() != 1 || !token.equals(row[3])
                    || !preparation.command().sha256().equals(HexFormat.of().formatHex((byte[]) row[4])))
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Abandonment marker differs from retained registration");
            return true;
        }
    }

    /** The session retains its original token even when acquisition never acknowledged. */
    static void abandonRetained(Tx tx, PayloadBudget budget, RepositoryCaller caller, UUID token,
            DocumentPublicationPreparationRecord preparation, RepositoryReadControl control) {
        if (confirm(tx, budget, caller, token, preparation, control)) return;
        control.check();
        write(tx, budget, token, preparation, control);
    }

    private static byte[] digest(DocumentPublicationPreparationRecord preparation) {
        try { return MessageDigest.getInstance("SHA-256").digest(DocumentPublicationPreparationCodec.encode(preparation).toByteArray()); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static void requirePrivate(RepositoryCaller caller, DocumentPublicationPreparationRecord preparation,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(preparation);
        DocumentAdmissionAuthorization.requireCaller(caller, preparation.key(), preparation.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Registration abandonment requires private process authority");
        if (preparation.predecessorGeneration() != 0) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                "Abandonment requires initial preparation");
    }

    /** An exception, including cancellation after commit, leaves the result uncertain; retry this exact identity. */
    static void abandon(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            RepositoryExecutionClaimLedger.Claim claim, DocumentPublicationPreparationRecord preparation,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        Objects.requireNonNull(tx); Objects.requireNonNull(budget); Objects.requireNonNull(preparation);
        DocumentAdmissionAuthorization.requireCaller(caller, claim.key(), claim.key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Registration abandonment requires private process authority");
        if (claim.epoch() != 1 || preparation.predecessorGeneration() != 0
                || !claim.key().equals(preparation.key()) || !claim.commandSha256().equals(preparation.command().sha256()))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Abandonment requires the original claim and preparation");
        write(tx, budget, claim.token(), preparation, control);
    }

    private static void write(Tx tx, PayloadBudget budget, UUID token,
            DocumentPublicationPreparationRecord preparation, RepositoryReadControl control) {
        try (var reservation = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            var digest = digest(preparation);
            control.check();
            tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, preparation.key(), preparation.command().sha256(), 1, token);
                control.check();
                em.createNativeQuery("""
                        INSERT INTO repository_publication_abandonments(account_id,principal,operation_id,
                          predecessor_generation,owner_nonce,preparation_sha256,claim_epoch,claim_token)
                        VALUES(:a,:p,:o,0,:nonce,:digest,1,:token)
                        ON CONFLICT(account_id,principal,operation_id) DO NOTHING
                        """).setParameter("a", preparation.key().account()).setParameter("p", preparation.key().principal())
                        .setParameter("o", preparation.key().operationId()).setParameter("nonce", preparation.seeds().ownerNonce())
                        .setParameter("digest", digest).setParameter("token", token).executeUpdate();
                control.check(); return null;
            });
            control.check();
        }
    }
}

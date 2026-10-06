package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Private pre-owner cancellation. No takeover, provider cleanup or execution permission. */
final class DocumentPublicationAbandonment {
    private DocumentPublicationAbandonment() {}

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
        try (var reservation = budget.reserve(DocumentPublicationPreparationCodec.MAX_BYTES)) {
            byte[] digest;
            try { digest = MessageDigest.getInstance("SHA-256").digest(DocumentPublicationPreparationCodec.encode(preparation).toByteArray()); }
            catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
            control.check();
            tx.inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                control.check();
                em.createNativeQuery("""
                        INSERT INTO repository_publication_abandonments(account_id,principal,operation_id,
                          predecessor_generation,owner_nonce,preparation_sha256,claim_epoch,claim_token)
                        VALUES(:a,:p,:o,0,:nonce,:digest,1,:token)
                        ON CONFLICT(account_id,principal,operation_id) DO NOTHING
                        """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                        .setParameter("o", claim.key().operationId()).setParameter("nonce", preparation.seeds().ownerNonce())
                        .setParameter("digest", digest).setParameter("token", claim.token()).executeUpdate();
                control.check(); return null;
            });
            control.check();
        }
    }
}

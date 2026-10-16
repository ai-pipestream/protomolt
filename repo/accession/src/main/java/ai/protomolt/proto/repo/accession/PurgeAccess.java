package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.Objects;

/** Purge-only checks performed before any catalog mutation or outcome lookup. */
final class PurgeAccess {
    private final PurgeAuthorization authorization;

    PurgeAccess(PurgeAuthorization authorization) {
        this.authorization = Objects.requireNonNull(authorization, "purgeAuthorization");
    }

    void require(RepositoryCaller caller, String accountId) {
        Objects.requireNonNull(caller, "caller");
        if (caller.processAuthority())
            throw WriterAuthority.denied("Process authority cannot purge documents");
        // Keep the common binding rules while returning the purge refusal detail.
        try {
            DocumentReads.requireBinding(caller);
            DocumentReads.requireAccount(caller, accountId);
        } catch (AccessionException denied) {
            throw new AccessionException(denied.code(), denied.getMessage(),
                    DocumentSaves.rejection(ai.protomolt.proto.repo.v2.RejectionReason.REJECTION_REASON_WRITE_DENIED,
                            denied.getMessage()), denied);
        }
        var decision = Objects.requireNonNull(authorization.evaluate(caller, accountId),
                "purge authorization decision");
        if (!decision.administrator() || !decision.accountWriteAllowed())
            throw WriterAuthority.denied("Purge requires administrator authority and account write access");
    }
}

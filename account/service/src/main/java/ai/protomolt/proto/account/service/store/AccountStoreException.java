package ai.protomolt.proto.account.service.store;

/**
 * The store layer's failure type. Carries a {@link Kind} for the failures
 * that are part of the wire contract (unknown account, duplicate id) so the
 * gRPC layer can map them to statuses without string matching; everything
 * else is an unclassified store failure and lands INTERNAL.
 */
public final class AccountStoreException extends RuntimeException {

    /** The wire-relevant failure classifications. */
    public enum Kind {
        /** No classification (plain store failure → INTERNAL). */
        NONE,
        /** The named account does not exist → NOT_FOUND. */
        NOT_FOUND,
        /** The named account id is taken → ALREADY_EXISTS. */
        CONFLICT
    }

    private final Kind kind;

    private AccountStoreException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    /**
     * A plain store failure (no wire-contract classification).
     *
     * @param message the failure detail
     * @param cause the underlying cause (e.g. SQLException)
     * @return the exception
     */
    public static AccountStoreException wrap(String message, Throwable cause) {
        return new AccountStoreException(Kind.NONE, message, cause);
    }

    /**
     * The named account does not exist.
     *
     * @param accountId the missing account
     * @return the exception
     */
    public static AccountStoreException notFound(String accountId) {
        return new AccountStoreException(Kind.NOT_FOUND,
                "account not found: " + accountId, null);
    }

    /**
     * The named account id is already taken.
     *
     * @param accountId the conflicting account id
     * @return the exception
     */
    public static AccountStoreException conflict(String accountId) {
        return new AccountStoreException(Kind.CONFLICT,
                "account already exists: " + accountId, null);
    }

    /**
     * The failure classification the gRPC layer maps on.
     *
     * @return the kind
     */
    public Kind kind() {
        return kind;
    }
}

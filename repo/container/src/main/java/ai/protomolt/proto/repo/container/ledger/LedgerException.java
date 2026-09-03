package ai.protomolt.proto.repo.container.ledger;

/**
 * Runtime wrapper for failures surfacing from the ledger's persistence
 * plumbing.
 * <p>
 * {@link Tx} rethrows {@link RuntimeException}s intact (constraint
 * violations, lock timeouts and the like keep their original
 * {@code PersistenceException} type so callers can react to them); only
 * checked failures that cannot propagate through the functional signatures
 * are wrapped in this type. The original exception is always the cause —
 * nothing is swallowed.
 */
public class LedgerException extends RuntimeException {

    public LedgerException(String message) {
        super(message);
    }

    public LedgerException(String message, Throwable cause) {
        super(message, cause);
    }
}

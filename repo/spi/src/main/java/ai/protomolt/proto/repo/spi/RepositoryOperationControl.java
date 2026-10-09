package ai.protomolt.proto.repo.spi;

/**
 * Thread-safe, nonblocking cancellation/deadline signal supplied by the host.
 * A write checks this before publication. Cancellation during a commit or after
 * its acknowledgement is lost does not establish whether publication occurred.
 */
public interface RepositoryOperationControl {
    RepositoryOperationControl NONE = new RepositoryOperationControl() {
        @Override public boolean isCancelled() { return false; }
        @Override public long remainingNanos() { return Long.MAX_VALUE; }
    };

    boolean isCancelled();
    /** Remaining monotonic time, or Long.MAX_VALUE for no deadline. */
    long remainingNanos();

    default void check() {
        if (remainingNanos() <= 0)
            throw new RepositoryException(RepositoryException.Code.DEADLINE_EXCEEDED, "Repository operation deadline exceeded");
        if (isCancelled() || Thread.currentThread().isInterrupted())
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Repository operation cancelled");
    }
}

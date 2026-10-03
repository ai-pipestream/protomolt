package ai.protomolt.proto.repo.spi;

/**
 * Host-supplied cancellation and monotonic deadline for a read. Implementations
 * must be thread-safe and nonblocking; they carry no authorization decisions.
 */
public interface RepositoryReadControl {
    RepositoryReadControl NONE = new RepositoryReadControl() {
        @Override public boolean isCancelled() { return false; }
        @Override public long remainingNanos() { return Long.MAX_VALUE; }
    };

    boolean isCancelled();

    /** Remaining monotonic time; Long.MAX_VALUE means no deadline. */
    long remainingNanos();

    default void check() {
        if (remainingNanos() <= 0)
            throw new RepositoryException(RepositoryException.Code.DEADLINE_EXCEEDED, "Document read deadline exceeded");
        if (isCancelled() || Thread.currentThread().isInterrupted())
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Document read cancelled");
    }
}

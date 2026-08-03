package ai.pipestream.proto.jobs.service.store;

/**
 * The jobs store layer's failure type. A plain runtime exception: every
 * store operation fails loud, so the worker's loop and the verbs see the
 * failure with the SQL cause intact instead of a swallowed default.
 */
public final class ChainJobStoreException extends RuntimeException {

    private ChainJobStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * A store failure with the underlying cause (e.g. SQLException).
     *
     * @param message the failure detail
     * @param cause the underlying cause
     * @return the exception
     */
    public static ChainJobStoreException wrap(String message, Throwable cause) {
        return new ChainJobStoreException(message, cause);
    }

    /**
     * The named job does not exist.
     *
     * @param jobId the missing job id
     * @return the exception
     */
    public static ChainJobStoreException notFound(Object jobId) {
        return new ChainJobStoreException("chain job not found: " + jobId, null);
    }
}

package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.CANCELLED;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED;

/**
 * Engine-boundary classification of failures raised while reading retained historical
 * content from its original backend. Provider and resource failures become repository
 * domain codes with constant messages; the raising exception stays on the cause chain.
 * Failures that are not provider or resource conditions (SQL, broken invariants, malformed
 * arguments) keep their type so that an outage or a bug is never reported as a provider
 * classification.
 */
final class HistoricalReadFailures {
    private HistoricalReadFailures() {}

    /** The repository failure for a historical read; a RepositoryException returns unchanged. */
    static RuntimeException translate(RuntimeException failure) {
        Objects.requireNonNull(failure);
        return switch (failure) {
            case RepositoryException repository -> repository;
            case BlobStoreException provider -> RepositoryErrors.historicalProvider(provider);
            case BlobStore.BlobNotFoundException missing ->
                    new RepositoryException(DATA_LOSS, "Published document part is missing from its original backend", missing);
            case BlobStore.BlobReadLimitException oversized ->
                    new RepositoryException(DATA_LOSS, "Document part exceeds its recorded size", oversized);
            case UnsupportedOperationException unsupported ->
                    new RepositoryException(FAILED_PRECONDITION, "Original document backend does not support the retained read", unsupported);
            case CancellationException cancelled -> new RepositoryException(CANCELLED, "Historical document read cancelled", cancelled);
            case PayloadBudget.CapacityExceededException exhausted ->
                    new RepositoryException(RESOURCE_EXHAUSTED, "Document payload capacity exhausted", exhausted);
            default -> failure;
        };
    }

    /**
     * Closes one owned resource after a failed read. A close failure is attached to the
     * primary failure as suppressed, so the classification and the cleanup fact both survive;
     * without a primary failure the close failure propagates.
     */
    static void release(Throwable primary, AutoCloseable resource) {
        if (resource == null) return;
        try { resource.close(); }
        catch (Exception | Error closeFailure) {
            if (primary == null) {
                if (closeFailure instanceof RuntimeException runtime) throw runtime;
                if (closeFailure instanceof Error error) throw error;
                throw new IllegalStateException("Historical read resource close failed", closeFailure);
            }
            if (primary != closeFailure) primary.addSuppressed(closeFailure);
        }
    }
}

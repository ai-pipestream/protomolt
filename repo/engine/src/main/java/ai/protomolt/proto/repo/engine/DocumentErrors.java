package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.ledger.DocumentAttemptWriter;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentPartAttemptLedger;
import ai.protomolt.proto.repo.container.ledger.RawObjectLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Supplier;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.*;

/**
 * Domain failures of the v1 document, raw-object and part-storage paths. {@link #call} translates
 * the v1 ledger and part-storage exceptions first and leaves everything else to
 * {@link RepositoryErrors#call}, so the v1 paths keep their mapping while the archive does not
 * depend on this class.
 */
final class DocumentErrors {
    private DocumentErrors() {}
    static RepositoryException revisionConflict() {
        var cause = new DocumentLedger.RevisionConflictException();
        return new RepositoryException(CONFLICT, cause.getMessage(), cause);
    }
    static <T> T call(Supplier<T> operation) {
        return RepositoryErrors.call(() -> {
            try { return operation.get(); }
            catch (DocumentAttemptWriter.WriteFailure failure) {
                throw managedFailure(failure.attemptId(), failure.phase(), failure);
            }
            catch (DocumentLedger.RevisionConflictException conflict) {
                throw new RepositoryException(CONFLICT, conflict.getMessage(), conflict);
            }
            catch (PartStorage.PartObjectMissingException failure) {
                throw new RepositoryException(FAILED_PRECONDITION, failure.getMessage(), failure);
            }
        });
    }

    /**
     * Historical reads of retained content from its original backend. The caller is already
     * authorized by the ledger and the request is built from retained coordinates, so a provider
     * refusal is a host or backend condition: a refused credential is a precondition failure rather
     * than the caller's permission, a provider-side timeout is backend unavailability rather than the
     * caller's deadline, and a missing or damaged physical version is data loss, never a retry
     * promise. Messages are constants; the provider failure stays on the cause chain.
     */
    static RepositoryException historicalProvider(BlobStoreException failure) {
        return switch (failure.code()) {
            case PERMISSION_DENIED, UNAUTHENTICATED ->
                    new RepositoryException(FAILED_PRECONDITION, "Original document backend refused the historical read", failure);
            case UNAVAILABLE -> new RepositoryException(UNAVAILABLE, "Original document backend is unreachable", failure);
            case DEADLINE_EXCEEDED -> new RepositoryException(UNAVAILABLE, "Original document backend did not answer in time", failure);
            case RESOURCE_EXHAUSTED ->
                    new RepositoryException(RESOURCE_EXHAUSTED, "Original document backend read capacity is exhausted", failure);
            case CANCELLED -> new RepositoryException(CANCELLED, "Historical document read cancelled", failure);
            case NOT_FOUND -> new RepositoryException(DATA_LOSS, "Published document part is missing from its original backend", failure);
            case DATA_LOSS -> new RepositoryException(DATA_LOSS, "Historical document part is damaged at its original backend", failure);
            case FAILED_PRECONDITION ->
                    new RepositoryException(FAILED_PRECONDITION, "Original document backend cannot serve the retained revision", failure);
            case INVALID_ARGUMENT ->
                    new RepositoryException(FAILED_PRECONDITION, "Original document backend rejected the retained object coordinates", failure);
            case UNIMPLEMENTED -> new RepositoryException(UNSUPPORTED, "Original document backend does not support historical reads", failure);
            case ABORTED -> new RepositoryException(CONFLICT, "Original document backend aborted the historical read", failure);
            case INTERNAL -> new RepositoryException(INTERNAL, "Original document backend failed internally", failure);
            case UNKNOWN, ALREADY_EXISTS, OUT_OF_RANGE ->
                    new RepositoryException(UNKNOWN, "Original document backend rejected the historical read", failure);
        };
    }

    /** Preserve the operation identity and original failure chain; classification never authorizes a retry. */
    static RepositoryException managedFailure(UUID attemptId, String phase, Throwable failure) {
        var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        var code = UNKNOWN;
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof RepositoryException domain) { code = domain.code(); break; }
            if (cause instanceof BlobStoreException provider) { code = RepositoryErrors.providerCode(provider); break; }
            if (cause instanceof DocumentLedger.RevisionConflictException) { code = CONFLICT; break; }
            if (cause instanceof DocumentPartAttemptLedger.FenceException || cause instanceof RawObjectLedger.FenceException) {
                // Generic fences also represent invalid state/profile/identity, not just races.
                code = FAILED_PRECONDITION; break;
            }
            if (cause instanceof PayloadBudget.CapacityExceededException) { code = RESOURCE_EXHAUSTED; break; }
            if (cause instanceof CancellationException) { code = CANCELLED; break; }
            if (cause instanceof BlobStore.BlobReadLimitException) { code = DATA_LOSS; break; }
            if (cause instanceof BlobStore.BlobNotFoundException) { code = DATA_LOSS; break; }
            if (cause instanceof UnsupportedOperationException) { code = FAILED_PRECONDITION; break; }
            if (cause instanceof IllegalArgumentException) { code = INVALID_ARGUMENT; break; }
        }
        return new RepositoryException(code, "Document attempt " + attemptId + " failed during " + phase
                + "; reconcile its outcome", failure);
    }
}

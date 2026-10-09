package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.*;

/** Domain failures shared by document operations; wire translation belongs to adapters. */
final class RepositoryErrors {
    private RepositoryErrors() {}
    static void requireProcessAuthority(RepositoryCaller caller) {
        if (caller == null || !caller.processAuthority())
            throw new RepositoryException(PERMISSION_DENIED, "Repository account policy is not configured for this principal");
    }
    static RepositoryException invalidArgument(String message) { return new RepositoryException(INVALID_ARGUMENT, message); }
    static RepositoryException notFound(String message) { return new RepositoryException(NOT_FOUND, message); }
    static RepositoryException failedPrecondition(String message) { return new RepositoryException(FAILED_PRECONDITION, message); }
    static RepositoryException unavailable(String message) { return new RepositoryException(UNAVAILABLE, message); }
    static RepositoryException aborted(String message) { return new RepositoryException(CONFLICT, message); }
    static RepositoryException revisionConflict() {
        var cause = new ai.protomolt.proto.repo.container.ledger.DocumentLedger.RevisionConflictException();
        return new RepositoryException(CONFLICT, cause.getMessage(), cause);
    }
    static RepositoryException alreadyExists(String message) { return new RepositoryException(ALREADY_EXISTS, message); }
    static <T> T call(java.util.function.Supplier<T> operation) {
        try { return operation.get(); }
        catch (ai.protomolt.proto.repo.container.ledger.DocumentAttemptWriter.WriteFailure failure) {
            throw managedFailure(failure.attemptId(), failure.phase(), failure);
        }
        catch (ai.protomolt.proto.repo.container.ledger.DocumentLedger.RevisionConflictException conflict) {
            throw new RepositoryException(CONFLICT, conflict.getMessage(), conflict);
        }
        catch (ai.protomolt.proto.repo.container.archive.ArchiveLedger.VersionConflictException conflict) {
            throw new RepositoryException(CONFLICT, conflict.getMessage(), conflict);
        }
        catch (ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger.FenceException conflict) {
            throw new RepositoryException(CONFLICT, conflict.getMessage(), conflict);
        }
        catch (ai.protomolt.proto.repo.blob.spi.BlobStoreException failure) {
            var code = switch (failure.code()) {
                case ABORTED -> CONFLICT;
                case UNIMPLEMENTED -> UNSUPPORTED;
                default -> RepositoryException.Code.valueOf(failure.code().name());
            };
            throw new RepositoryException(code, failure.getMessage(), failure);
        } catch (PartStorage.PartObjectMissingException failure) {

            throw new RepositoryException(FAILED_PRECONDITION, failure.getMessage(), failure);
        } catch (BlobStore.BlobNotFoundException failure) {
            throw new RepositoryException(NOT_FOUND, failure.getMessage(), failure);
        } catch (IllegalArgumentException failure) {
            throw new RepositoryException(INVALID_ARGUMENT, failure.getMessage(), failure);
        }
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
    static RepositoryException managedFailure(java.util.UUID attemptId, String phase, Throwable failure) {
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        var code = UNKNOWN;
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause instanceof RepositoryException domain) { code = domain.code(); break; }
            if (cause instanceof ai.protomolt.proto.repo.blob.spi.BlobStoreException provider) {
                code = switch (provider.code()) {
                    case ABORTED -> CONFLICT;
                    case UNIMPLEMENTED -> UNSUPPORTED;
                    default -> RepositoryException.Code.valueOf(provider.code().name());
                };
                break;
            }
            if (cause instanceof ai.protomolt.proto.repo.container.ledger.DocumentLedger.RevisionConflictException) {
                code = CONFLICT; break;
            }
            if (cause instanceof ai.protomolt.proto.repo.container.ledger.DocumentPartAttemptLedger.FenceException
                    || cause instanceof ai.protomolt.proto.repo.container.ledger.RawObjectLedger.FenceException) {
                // Generic fences also represent invalid state/profile/identity, not just races.
                code = FAILED_PRECONDITION; break;
            }
            if (cause instanceof ai.protomolt.proto.repo.blob.spi.PayloadBudget.CapacityExceededException) {
                code = RESOURCE_EXHAUSTED; break;
            }
            if (cause instanceof java.util.concurrent.CancellationException) { code = CANCELLED; break; }
            if (cause instanceof BlobStore.BlobReadLimitException) { code = DATA_LOSS; break; }
            if (cause instanceof BlobStore.BlobNotFoundException) { code = DATA_LOSS; break; }
            if (cause instanceof UnsupportedOperationException) { code = FAILED_PRECONDITION; break; }
            if (cause instanceof IllegalArgumentException) { code = INVALID_ARGUMENT; break; }
        }
        return new RepositoryException(code, "Document attempt " + attemptId + " failed during " + phase
                + "; reconcile its outcome", failure);
    }
}

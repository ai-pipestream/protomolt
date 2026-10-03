package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
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
}

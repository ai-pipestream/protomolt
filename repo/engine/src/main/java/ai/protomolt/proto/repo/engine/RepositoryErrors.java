package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.function.Supplier;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.*;

/**
 * Domain failures shared by the archive, drive and document operations; wire translation belongs
 * to adapters. This class names only archive, drive and blob failures so the frozen archive
 * closure stays free of the v1 document ledger; v1 document failures are {@link DocumentErrors}.
 */
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
    static RepositoryException alreadyExists(String message) { return new RepositoryException(ALREADY_EXISTS, message); }
    static <T> T call(Supplier<T> operation) {
        try { return operation.get(); }
        catch (ArchiveLedger.VersionConflictException conflict) {
            throw new RepositoryException(CONFLICT, conflict.getMessage(), conflict);
        }
        catch (ArchiveUploadLedger.FenceException conflict) {
            throw new RepositoryException(CONFLICT, conflict.getMessage(), conflict);
        }
        catch (BlobStoreException failure) {
            throw new RepositoryException(providerCode(failure), failure.getMessage(), failure);
        } catch (BlobStore.BlobNotFoundException failure) {
            throw new RepositoryException(NOT_FOUND, failure.getMessage(), failure);
        } catch (IllegalArgumentException failure) {
            throw new RepositoryException(INVALID_ARGUMENT, failure.getMessage(), failure);
        }
    }

    /** The provider's own code, except ABORTED and UNIMPLEMENTED, which have repository names. */
    static RepositoryException.Code providerCode(BlobStoreException failure) {
        return switch (failure.code()) {
            case ABORTED -> CONFLICT;
            case UNIMPLEMENTED -> UNSUPPORTED;
            default -> RepositoryException.Code.valueOf(failure.code().name());
        };
    }
}

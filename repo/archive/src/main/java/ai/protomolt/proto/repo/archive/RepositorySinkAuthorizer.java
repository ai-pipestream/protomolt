package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import java.util.function.Supplier;

/**
 * Host authorization hooks invoked by {@link RepositorySink} around source reads, job inspection,
 * renderer policy checks, and repository commits.
 *
 * <p>Downstream v2 {@code DocumentService} reads and uploads always enforce document-level ACLs
 * and writer grants as the authoritative gate; these hooks allow a host to bind caller credentials
 * on background execution threads and re-evaluate live account/writer bindings immediately before
 * commit so grants revoked during long conversions fail cleanly before staging or committing bytes.
 */
public interface RepositorySinkAuthorizer {
    /** Permissive no-op hook when caller context is already carried directly on the channel. */
    RepositorySinkAuthorizer PASS_THROUGH = new RepositorySinkAuthorizer() {};

    /**
     * Verifies that the current caller is bound to and permitted to inspect {@code accountId}.
     *
     * @param accountId target account identifier
     */
    default void checkAccountAccess(String accountId) {}

    /**
     * Verifies at admission time that the current caller currently holds writer authority for
     * {@code destination} before admitting or enqueuing a publication job.
     *
     * @param destination target archive entry address
     * @param datasourceId archive datasource identifier
     * @param writtenBy requested write attribution
     */
    default void checkWriteAccess(
            EntryAddress destination, String datasourceId, WriteAttribution writtenBy) {}

    /**
     * Captures the current caller's context on the admitting request thread so background/async
     * execution and pre-commit re-authorization run as the originating caller rather than with
     * ambient or elevated authority.
     *
     * @return a captured caller handle bound to the admitting request's caller
     */
    default CapturedCaller captureCaller() {
        return CapturedCaller.CURRENT_CONTEXT;
    }

    /** Captured caller execution handle for synchronous and asynchronous sink phases. */
    interface CapturedCaller {
        /** Default handle running directly in the ambient gRPC context. */
        CapturedCaller CURRENT_CONTEXT = new CapturedCaller() {};

        /** Runs {@code action} under the captured caller's read context. */
        default <T> T callForRead(String accountId, Supplier<T> action) {
            return action.get();
        }

        /**
         * Re-evaluates the captured caller's live writer bindings for {@code destination} and runs
         * {@code commitAction} under the refreshed commit context.
         */
        default <T> T callForCommit(
                EntryAddress destination,
                String datasourceId,
                WriteAttribution writtenBy,
                Supplier<T> commitAction) {
            return commitAction.get();
        }
    }
}

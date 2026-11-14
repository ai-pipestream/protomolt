package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import java.util.Objects;
import java.util.Optional;

/**
 * Persistence contract for durable {@link RepositorySinkJob} records keyed by
 * {@code (account_id, operation_id)}.
 *
 * <p>Implementations must execute short, bounded reads and updates without holding database
 * transactions open across source reads, renderer execution, or streamed uploads.
 */
public interface RepositorySinkJobStore {

    /**
     * Durable job record bundling the public {@link RepositorySinkJob} state, the admitted
     * {@link PublishRepositorySinkRequest}, and optional pre-commit {@link RenditionProvenance}
     * used to reconcile uncertain commit outcomes.
     *
     * @param job current job state and receipt
     * @param request admitted publication request
     * @param pendingProvenance provenance prepared immediately prior to repository commit, or null
     */
    record StoredJob(
            RepositorySinkJob job,
            PublishRepositorySinkRequest request,
            RenditionProvenance pendingProvenance) {
        /** Validates required record fields. */
        public StoredJob {
            Objects.requireNonNull(job, "job");
            Objects.requireNonNull(request, "request");
        }
    }

    /**
     * Looks up a persisted sink job by owning account and operation ID.
     *
     * @param accountId owning account identifier
     * @param operationId sink operation identifier
     * @return the persisted job record when present
     */
    Optional<StoredJob> find(String accountId, String operationId);

    /**
     * Atomically inserts {@code candidate} if no row exists for
     * {@code (candidate.job.account_id, candidate.job.operation_id)}, or returns the existing row
     * unchanged when one is already recorded.
     *
     * @param candidate newly admitted job record
     * @return the inserted record, or the pre-existing record for the same key
     */
    StoredJob insertOrGet(StoredJob candidate);

    /**
     * Persists an updated state transition for an existing sink job.
     *
     * @param updated updated job record
     */
    void save(StoredJob updated);
}

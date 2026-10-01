package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateResponse;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationFailure;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import java.io.IOException;
import java.util.Optional;

/**
 * Coordinator-owned preparation ledger, not a public author API. Reservations bind both
 * the exact task/attempt/revision tuple and a globally unique preparation UUID. Stored
 * shape and digest validity do not establish current delegation authority.
 */
public interface WorkflowPreparationRepository {
    /**
     * Reads a validated snapshot. Corrupt or unsupported records are errors, never absence.
     * The snapshot does not grant execution ownership and can become stale immediately.
     */
    Optional<WorkflowPreparationRecord> find(String taskId, int attempt, int revision)
            throws IOException;

    /**
     * Holds exclusive execution ownership of the exact tuple across threads/processes for
     * the callback's lifetime, including external calls. Does not reserve an intent itself.
     * Implementations release ownership on every exit. Callbacks must not acquire another
     * session or use a session asynchronously; no lock is held over unrelated task execution.
     * The handler still checks current delegation authority before each execution phase.
     */
    <T> T withExclusiveIntent(String taskId, int attempt, int revision, Work<T> work)
            throws Exception;

    /** Bounded work performed while one tuple's execution lock is held. */
    @FunctionalInterface
    interface Work<T> {
        T execute(Session session) throws Exception;
    }

    /** Valid only inside its owning callback; implementations reject use after return. */
    interface Session {
        /** Current validated record, if reserved. */
        Optional<WorkflowPreparationRecord> current() throws IOException;

        /**
         * Atomically reserves the tuple and UUID or returns the exact existing intent's
         * current state. Different intent or UUID reuse by another tuple conflicts. Verify
         * native rules, bounded canonical bytes and source/offer/policy digests before writing.
         * Caller preflight and transcript membership remain handler obligations.
         */
        WorkflowPreparationRecord reserveOrMatch(WorkflowPreparationIntent intent)
                throws IOException;

        /**
         * Atomically changes pending to this bound response. Exact terminal retry returns
         * the existing record; different outcome conflicts. Never mutates the intent.
         */
        WorkflowPreparationRecord complete(PrepareWorkflowCandidateResponse response)
                throws IOException;

        /** Same transition rules as complete, for a stable terminal failure. */
        WorkflowPreparationRecord fail(WorkflowPreparationFailure failure) throws IOException;
    }
}

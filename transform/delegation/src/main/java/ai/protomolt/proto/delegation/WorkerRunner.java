package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.v1.Checkpoint;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.RevisionRequested;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import ai.protomolt.proto.delegation.v1.WorkerHello;

import java.util.Objects;
import java.util.Optional;

/** Provider-neutral boundary between the delegation stream and an LLM worker. */
public interface WorkerRunner {

    /** Returns the identity and capabilities advertised during admission. */
    WorkerHello hello();

    /**
     * Executes or revises one leased task.
     *
     * @param task task and optional revision feedback
     * @param events progress, checkpoint, and liveness callbacks
     * @return a reviewable completion candidate
     * @throws Exception when the provider or adapter cannot finish the task
     */
    CompletionCandidate run(WorkerTask task, WorkerEvents events) throws Exception;

    /** One invocation of a worker, including feedback when revising. */
    record WorkerTask(String taskId, TaskOffer offer,
                      Optional<RevisionRequested> revision) {
        public WorkerTask {
            Objects.requireNonNull(taskId, "taskId");
            Objects.requireNonNull(offer, "offer");
            revision = Objects.requireNonNull(revision, "revision");
        }

        /** Revision number the next candidate must carry. */
        public int expectedRevision() {
            return revision.map(value -> value.getRevision() + 1).orElse(1);
        }
    }

    /** Runtime callbacks available to a provider adapter. */
    interface WorkerEvents {

        /** Reports one monotonic progress update. */
        void progress(String message);

        /** Records one resumable checkpoint. */
        void checkpoint(Checkpoint checkpoint);

        /** Sends a lease heartbeat. */
        void heartbeat(String note);

        /** Returns whether the coordinator cancelled this attempt. */
        boolean cancelled();
    }
}

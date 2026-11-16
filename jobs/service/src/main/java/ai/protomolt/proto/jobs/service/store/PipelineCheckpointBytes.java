package ai.protomolt.proto.jobs.service.store;

import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoint;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoints;
import com.google.protobuf.InvalidProtocolBufferException;

/** What both stores share when they read and compare a pipeline run's stored checkpoints. */
final class PipelineCheckpointBytes {

    private PipelineCheckpointBytes() {
    }

    /** The stored checkpoints; an absent column is a run with no checkpoint yet. */
    static PipelineRunCheckpoints parse(byte[] stored) {
        if (stored == null) {
            return PipelineRunCheckpoints.getDefaultInstance();
        }
        try {
            return PipelineRunCheckpoints.parseFrom(stored);
        } catch (InvalidProtocolBufferException e) {
            throw WorkflowRunStoreException.wrap("stored pipeline checkpoints are not readable", e);
        }
    }

    /**
     * The outcome when {@code stepName} already has a checkpoint: {@code AlreadyDone} for
     * identical content, {@code Conflict} for different content, and null when the step has
     * none yet.
     */
    static ParkedCompletion alreadySettled(PipelineRunCheckpoints checkpoints, String stepName,
                                           PipelineRunCheckpoint candidate, String status) {
        for (PipelineRunCheckpoint existing : checkpoints.getCheckpointsList()) {
            if (stepName.equals(existing.getOutcome().getStep())) {
                return existing.equals(candidate)
                        ? new ParkedCompletion.AlreadyDone(status)
                        : new ParkedCompletion.Conflict(status);
            }
        }
        return null;
    }
}

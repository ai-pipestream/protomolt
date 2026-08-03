package ai.pipestream.proto.jobs.service.store;

/**
 * The verdict of {@link ChainJobStore#completeParkedStep}: the state machine
 * gate for the human-in-the-loop lane, decided atomically inside the store's
 * transaction (the row is locked {@code FOR UPDATE} while it is made).
 */
public sealed interface ParkedCompletion {

    /**
     * The job was WAITING on exactly this step: the checkpoint entry was
     * appended and the job is QUEUED again for its next segment.
     */
    record Completed() implements ParkedCompletion {
    }

    /**
     * The step's checkpoint is already persisted — an idempotent redelivery
     * of complete-step. Nothing changed.
     *
     * @param currentStatus the job's status as found
     */
    record AlreadyDone(String currentStatus) implements ParkedCompletion {
    }

    /**
     * The job is not parked on this step.
     *
     * @param currentStatus the job's status as found
     * @param outstandingStep the step the job is actually parked on, or null
     */
    record WrongState(String currentStatus, String outstandingStep) implements ParkedCompletion {
    }
}

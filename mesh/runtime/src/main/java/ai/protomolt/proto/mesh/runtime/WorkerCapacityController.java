package ai.protomolt.proto.mesh.runtime;

/** Constant-time effective-credit accounting for one admitted worker session. */
public final class WorkerCapacityController {

    private final int coordinatorLimit;

    public WorkerCapacityController(int coordinatorLimit) {
        if (coordinatorLimit < 1 || coordinatorLimit > 100_000) {
            throw new IllegalArgumentException(
                    "coordinatorLimit must be between 1 and 100000");
        }
        this.coordinatorLimit = coordinatorLimit;
    }

    public int effectivePermits(
            int explicitCredit,
            int directoryCapacity,
            int workerMaxInFlight,
            int workerInFlight,
            int localQueueDepth,
            boolean draining) {
        if (draining || explicitCredit <= 0 || directoryCapacity <= 0
                || workerMaxInFlight <= 0 || workerInFlight >= workerMaxInFlight) {
            return 0;
        }
        int workerFree = workerMaxInFlight - workerInFlight;
        int queuePressure = Math.max(0, workerFree - Math.max(0, localQueueDepth));
        return Math.max(0, Math.min(
                Math.min(explicitCredit, directoryCapacity),
                Math.min(queuePressure, coordinatorLimit)));
    }
}

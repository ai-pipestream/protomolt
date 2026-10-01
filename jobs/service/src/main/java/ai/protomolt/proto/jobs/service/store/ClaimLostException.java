package ai.protomolt.proto.jobs.service.store;

/** A worker no longer owns a live attempt; no job or outbox mutation was committed. */
public final class ClaimLostException extends RuntimeException {
    public ClaimLostException(WorkerClaim claim) {
        super("workflow claim is no longer live: " + claim.jobId() + " attempt " + claim.attempt());
    }
}

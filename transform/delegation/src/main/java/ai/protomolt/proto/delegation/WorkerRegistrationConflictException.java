package ai.protomolt.proto.delegation;

/** Connected worker identity is already owned by another delegation stream. */
public final class WorkerRegistrationConflictException extends RuntimeException {
    public WorkerRegistrationConflictException() {
        super("worker identity already has a connected session");
    }
}

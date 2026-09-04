package ai.protomolt.proto.mesh.runtime;

/** Optimistic lifecycle update failed because a newer durable revision exists. */
public final class LifecycleConflictException extends IllegalStateException {

    public LifecycleConflictException(String message) {
        super(message);
    }
}

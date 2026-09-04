package ai.protomolt.proto.mesh.runtime;

/** A requested durable workflow, deployment, or run does not exist. */
public final class LifecycleNotFoundException extends RuntimeException {

    public LifecycleNotFoundException(String message) {
        super(message);
    }
}

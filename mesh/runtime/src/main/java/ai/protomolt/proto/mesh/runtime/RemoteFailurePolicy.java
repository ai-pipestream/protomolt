package ai.protomolt.proto.mesh.runtime;

/** Decides whether a remote processor exception may consume another durable attempt. */
@FunctionalInterface
public interface RemoteFailurePolicy {

    boolean retryable(Throwable failure);

    static RemoteFailurePolicy retryAll() {
        return ignored -> true;
    }
}

package ai.protomolt.proto.mesh.runtime;

/** Terminal failure reported by a demand-driven processor delivery. */
public final class RemoteProcessorException extends RuntimeException {

    private final String code;

    RemoteProcessorException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}

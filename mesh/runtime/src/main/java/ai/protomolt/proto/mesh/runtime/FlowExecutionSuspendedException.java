package ai.protomolt.proto.mesh.runtime;

/** Execution stopped without a terminal transition so another coordinator can resume it. */
final class FlowExecutionSuspendedException extends RuntimeException {

    FlowExecutionSuspendedException(String message) {
        super(message);
    }

    FlowExecutionSuspendedException(String message, Throwable cause) {
        super(message, cause);
    }
}

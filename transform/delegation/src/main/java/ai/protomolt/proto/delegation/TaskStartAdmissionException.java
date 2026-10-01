package ai.protomolt.proto.delegation;

/** A new create-once task could not select a currently admitted, connected worker. */
public final class TaskStartAdmissionException extends IllegalStateException {
    public TaskStartAdmissionException(String message, Throwable cause) {
        super(message, cause);
    }
}

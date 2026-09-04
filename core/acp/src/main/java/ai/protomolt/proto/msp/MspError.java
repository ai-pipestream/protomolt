package ai.protomolt.proto.msp;

/**
 * An MSP failure: a JSON-RPC error the session host answered, a turn that ended in the
 * {@code failed} terminal, or a local failure of the connection (timeout, host exit).
 * {@link #code()} follows the JSON-RPC 2.0 code space; a turn failure carries
 * {@link #TURN_FAILED}.
 */
public final class MspError extends RuntimeException {

    /** The local code for a turn whose {@code turn/completed} terminal was {@code failed}. */
    public static final int TURN_FAILED = -1;

    private final int code;
    private final boolean retryable;

    public MspError(int code, String message, boolean retryable) {
        super(message);
        this.code = code;
        this.retryable = retryable;
    }

    public MspError(int code, String message) {
        this(code, message, false);
    }

    public int code() {
        return code;
    }

    /** The host's judgment that the same request may succeed if resubmitted. */
    public boolean retryable() {
        return retryable;
    }
}

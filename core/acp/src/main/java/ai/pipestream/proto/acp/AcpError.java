package ai.pipestream.proto.acp;

/**
 * A JSON-RPC protocol error: the error object carried on a response, or a local failure of the
 * connection itself (timeout, stream closed). {@link #code()} follows the JSON-RPC 2.0 code
 * space; codes the peer sends are passed through unchanged.
 */
public final class AcpError extends RuntimeException {

    private final int code;

    public AcpError(int code, String message) {
        super(message);
        this.code = code;
    }

    public int code() {
        return code;
    }
}

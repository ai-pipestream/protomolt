package ai.pipestream.proto.rest;

/**
 * Base exception for REST/JSON gateway failures.
 */
public class ProtoRestException extends RuntimeException {
    public ProtoRestException(String message) {
        super(message);
    }

    public ProtoRestException(String message, Throwable cause) {
        super(message, cause);
    }
}

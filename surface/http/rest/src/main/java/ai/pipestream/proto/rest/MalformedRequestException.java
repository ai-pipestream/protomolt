package ai.pipestream.proto.rest;

/**
 * Thrown when the raw HTTP request is syntactically invalid before it reaches the
 * gateway (for example malformed percent-encoding in the query string).
 * Maps to {@code 400 Bad Request}.
 */
public class MalformedRequestException extends ProtoRestException {

    public MalformedRequestException(String message) {
        super(message);
    }

    public MalformedRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}

package ai.pipestream.proto.rest;

/**
 * Thrown when a required API token is missing or invalid.
 */
public class UnauthorizedProtoRestException extends ProtoRestException {
    public UnauthorizedProtoRestException(String message) {
        super(message);
    }
}

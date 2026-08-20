package ai.pipestream.proto.http.rest;

/**
 * Thrown when an authenticated caller does not hold the scope an operation requires
 * (mapped to 403; 401 keeps meaning "not authenticated").
 */
public class ForbiddenProtoRestException extends ProtoRestException {
    public ForbiddenProtoRestException(String message) {
        super(message);
    }

    public ForbiddenProtoRestException(String message, Throwable cause) {
        super(message, cause);
    }
}

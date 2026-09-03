package ai.protomolt.proto.http.rest;

/**
 * Thrown when a required API token is missing or invalid.
 */
public final class UnauthorizedProtoRestException extends ProtoRestException {
    public UnauthorizedProtoRestException(String message) {
        super(message);
    }
}

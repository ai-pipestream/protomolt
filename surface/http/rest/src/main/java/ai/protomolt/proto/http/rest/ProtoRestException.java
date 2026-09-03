package ai.protomolt.proto.http.rest;

/**
 * Base exception for REST/JSON gateway failures.
 *
 * <p>Sealed because the set of subtypes <em>is</em> the status-code contract: every host maps
 * these to an HTTP status through {@code ProtoRestHttpSupport.statusFor}, and anything it does
 * not name becomes a 500. An outside subclass would silently take that 500 instead of the status
 * it meant; sealing turns adding one into a deliberate edit of the mapping.
 */
public sealed class ProtoRestException extends RuntimeException
        permits ForbiddenProtoRestException,
                HttpMethodNotAllowedException,
                MalformedRequestException,
                MethodNotFoundException,
                ProtoRestInvocationException,
                RequestTooLargeException,
                ServiceNotFoundException,
                UnauthorizedProtoRestException {
    public ProtoRestException(String message) {
        super(message);
    }

    public ProtoRestException(String message, Throwable cause) {
        super(message, cause);
    }
}

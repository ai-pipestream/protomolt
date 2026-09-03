package ai.protomolt.proto.http.rest;

public final class ProtoRestInvocationException extends ProtoRestException {
    public ProtoRestInvocationException(String message) {
        super(message);
    }

    public ProtoRestInvocationException(String message, Throwable cause) {
        super(message, cause);
    }
}

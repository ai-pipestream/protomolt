package ai.pipestream.proto.http.rest;

public class ProtoRestInvocationException extends ProtoRestException {
    public ProtoRestInvocationException(String message) {
        super(message);
    }

    public ProtoRestInvocationException(String message, Throwable cause) {
        super(message, cause);
    }
}

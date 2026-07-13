package ai.pipestream.proto.rest;

import java.util.List;

/**
 * Thrown when a request uses an HTTP verb the target method does not declare.
 * Maps to {@code 405 Method Not Allowed}; hosts should emit an {@code Allow}
 * header listing {@link #allowedMethods()}.
 */
public class HttpMethodNotAllowedException extends ProtoRestException {

    private final List<String> allowedMethods;

    public HttpMethodNotAllowedException(String httpMethod, List<String> allowedMethods) {
        super("HTTP method " + httpMethod + " not allowed; allowed: " + String.join(", ", allowedMethods));
        this.allowedMethods = List.copyOf(allowedMethods);
    }

    /** Uppercase HTTP verbs the route accepts, for the {@code Allow} response header. */
    public List<String> allowedMethods() {
        return allowedMethods;
    }
}

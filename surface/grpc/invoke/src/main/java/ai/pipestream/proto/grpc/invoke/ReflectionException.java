package ai.pipestream.proto.grpc.invoke;

/** A gRPC server-reflection failure: an error response, a stream failure, or a timeout. */
public class ReflectionException extends Exception {

    public ReflectionException(String message) {
        super(message);
    }

    public ReflectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

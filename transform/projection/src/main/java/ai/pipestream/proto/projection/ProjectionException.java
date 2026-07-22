package ai.pipestream.proto.projection;

/** Raised when a projection cannot be built or a source message cannot be projected. */
public class ProjectionException extends RuntimeException {

    public ProjectionException(String message) {
        super(message);
    }

    public ProjectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

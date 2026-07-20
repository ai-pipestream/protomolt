package ai.pipestream.proto.connector;

/** A streaming source failed: the transport errored, a payload would not parse, or the like. */
public class SourceException extends RuntimeException {

    public SourceException(String message) {
        super(message);
    }

    public SourceException(String message, Throwable cause) {
        super(message, cause);
    }
}

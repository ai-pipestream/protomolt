package ai.pipestream.proto.gather;

/**
 * Gathering of {@code .proto} sources failed — missing inputs, unreadable files, conflicting
 * content across sources, or a transport failure (git, jar, repository).
 */
public class GatherException extends Exception {

    public GatherException(String message) {
        super(message);
    }

    public GatherException(String message, Throwable cause) {
        super(message, cause);
    }
}

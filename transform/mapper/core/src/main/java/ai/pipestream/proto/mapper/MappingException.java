package ai.pipestream.proto.mapper;

/**
 * Thrown when a mapping rule or path operation cannot be applied.
 */
public class MappingException extends Exception {

    public MappingException(String message, String rule) {
        super(message + (rule != null ? " (Rule: '" + rule + "')" : ""));
    }

    public MappingException(String message, Throwable cause, String rule) {
        super(message + (rule != null ? " (Rule: '" + rule + "')" : ""), cause);
    }
}

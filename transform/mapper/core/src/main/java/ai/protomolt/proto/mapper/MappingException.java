package ai.protomolt.proto.mapper;

/**
 * Thrown when a mapping rule or path operation cannot be applied.
 */
public class MappingException extends Exception {

    /** Classifies mapping failures that callers may handle without parsing messages. */
    public enum Category {
        GENERAL,
        ABSENT_INTERMEDIATE
    }

    private final Category category;

    public MappingException(String message, String rule) {
        this(message, rule, Category.GENERAL);
    }

    public MappingException(String message, Throwable cause, String rule) {
        this(message, cause, rule, Category.GENERAL);
    }

    /** Returns the machine-readable class of this mapping failure. */
    public Category category() {
        return category;
    }

    /** Creates the absence signal used when a path's intermediate message is unset. */
    static MappingException absentIntermediate(String message, String rule) {
        return new MappingException(message, rule, Category.ABSENT_INTERMEDIATE);
    }

    private MappingException(String message, String rule, Category category) {
        super(message + (rule != null ? " (Rule: '" + rule + "')" : ""));
        this.category = category;
    }

    private MappingException(String message, Throwable cause, String rule, Category category) {
        super(message + (rule != null ? " (Rule: '" + rule + "')" : ""), cause);
        this.category = category;
    }
}

package ai.pipestream.proto.compat;

import java.util.List;

/**
 * Thrown by {@link CompatibilityResult#throwIfIncompatible()} when a check found violations.
 * The message enumerates every violation ({@code ruleId path: message}, one per line) and the
 * structured {@link #violations()} remain available for programmatic handling.
 */
public class IncompatibleSchemaException extends Exception {

    private final transient List<SchemaChange> violations;
    private final CompatibilityMode mode;

    IncompatibleSchemaException(CompatibilityMode mode, List<SchemaChange> violations) {
        super(buildMessage(mode, violations));
        this.mode = mode;
        this.violations = List.copyOf(violations);
    }

    /** The violations that failed the check, in diff order. */
    public List<SchemaChange> violations() {
        return violations;
    }

    /** The mode the failing check ran under. */
    public CompatibilityMode mode() {
        return mode;
    }

    private static String buildMessage(CompatibilityMode mode, List<SchemaChange> violations) {
        StringBuilder message = new StringBuilder("Schema is incompatible under ")
                .append(mode).append(" (").append(violations.size()).append(" violation")
                .append(violations.size() == 1 ? "" : "s").append("):");
        for (SchemaChange violation : violations) {
            message.append(System.lineSeparator()).append("  - ").append(violation);
        }
        return message.toString();
    }
}

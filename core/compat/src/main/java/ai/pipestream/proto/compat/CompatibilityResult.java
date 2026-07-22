package ai.pipestream.proto.compat;

import java.util.List;

/**
 * Outcome of a {@link CompatibilityChecker} check: the full diff plus the subset that violates
 * the requested {@link CompatibilityMode} under the checker's rule configuration.
 *
 * @param mode       the mode the check ran under
 * @param changes    every detected {@link SchemaChange}, informational ones included
 * @param violations the changes that break the requested mode; empty means compatible
 */
public record CompatibilityResult(CompatibilityMode mode,
                                  List<SchemaChange> changes,
                                  List<SchemaChange> violations) {

    public CompatibilityResult {
        changes = List.copyOf(changes);
        violations = List.copyOf(violations);
    }

    /** Whether the new schema passes: no violations under the requested mode. */
    public boolean isCompatible() {
        return violations.isEmpty();
    }

    /**
     * Fails loudly when incompatible — the intended write-gate idiom.
     *
     * @throws IncompatibleSchemaException enumerating every violation
     */
    public void throwIfIncompatible() throws IncompatibleSchemaException {
        if (!violations.isEmpty()) {
            throw new IncompatibleSchemaException(mode, violations);
        }
    }
}

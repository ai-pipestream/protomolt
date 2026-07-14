package ai.pipestream.proto.compat;

import java.util.Set;

/**
 * A single difference between two schema versions, produced by {@link SchemaDiff}. Changes are
 * pure observations — whether one is a <em>violation</em> is decided later by
 * {@link CompatibilityChecker} from the {@link Impact}s and the requested mode.
 *
 * @param ruleId  stable SCREAMING_SNAKE identifier of the rule that fired
 *                (e.g. {@code FIELD_TYPE_CHANGED})
 * @param path    protobuf-style location of the change: {@code example.Person.email} for a
 *                field, {@code example.Status.STATUS_OLD} for an enum value,
 *                {@code example.Doc.attrs (map value)} for a map entry component
 * @param before  human-readable snippet of the old declaration; may be empty (e.g. additions)
 * @param after   human-readable snippet of the new declaration; may be empty (e.g. removals)
 * @param message one clear sentence describing the change
 * @param impacts the compatibility dimensions this change breaks; empty for informational
 *                changes
 */
public record SchemaChange(String ruleId,
                           String path,
                           String before,
                           String after,
                           String message,
                           Set<Impact> impacts) {

    public SchemaChange {
        impacts = Set.copyOf(impacts);
    }

    /** Whether this change breaks nothing — reported for visibility only. */
    public boolean isInformational() {
        return impacts.isEmpty();
    }

    @Override
    public String toString() {
        return ruleId + " " + path + ": " + message;
    }
}

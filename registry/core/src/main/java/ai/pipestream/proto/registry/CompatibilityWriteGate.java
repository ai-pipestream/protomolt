package ai.pipestream.proto.registry;

import ai.pipestream.proto.compat.CompatibilityChecker;
import ai.pipestream.proto.compat.CompatibilityException;
import ai.pipestream.proto.compat.CompatibilityMode;
import ai.pipestream.proto.compat.CompatibilityResult;
import ai.pipestream.proto.compat.SchemaChange;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link SchemaRegistryStore.WriteGate} backed by {@code protomolt-compat}: registrations are
 * checked against the subject's history under its effective compatibility mode, and violations
 * reject the write with the rule, path and reason.
 *
 * <p>Non-transitive modes check the candidate against the latest version only; the
 * {@code *_TRANSITIVE} modes check every version in the history. Candidate and historical
 * schemas are resolved with their transitive references from the store, so cross-subject
 * imports participate in the comparison. Wire rules always apply; JSON and source rule layers
 * follow the flags given at construction (see
 * {@link ai.pipestream.proto.compat.CompatibilityChecker.Builder}).</p>
 *
 * <p>A candidate that fails to resolve or compile is reported as a violation rather than an
 * error; the store's own compile verification then produces the typed failure.</p>
 */
public final class CompatibilityWriteGate implements SchemaRegistryStore.WriteGate {

    private final CompatibilityChecker checker;

    /** A gate enforcing wire rules only, matching Confluent's protobuf checking. */
    public CompatibilityWriteGate() {
        this(CompatibilityChecker.create());
    }

    /** A gate using the given checker (e.g. with JSON or source rules enabled). */
    public CompatibilityWriteGate(CompatibilityChecker checker) {
        this.checker = checker;
    }

    @Override
    public List<String> validate(String subject, String mode, List<StoredSchema> history,
                                 String schemaText, List<SchemaReference> references,
                                 SchemaRegistryStore store) {
        if (history.isEmpty()) {
            return List.of();
        }
        CompatibilityMode compatMode = CompatibilityMode.valueOf(mode);
        List<StoredSchema> against = switch (compatMode) {
            case BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE -> history;
            default -> List.of(history.getLast());
        };

        StoredSchemaSources.Resolved candidate;
        try {
            candidate = StoredSchemaSources.resolve(store, subject, schemaText, references);
        } catch (RegistryStoreException e) {
            return List.of("candidate does not resolve: " + e.getMessage());
        }

        List<String> violations = new ArrayList<>();
        for (StoredSchema previous : against) {
            try {
                StoredSchemaSources.Resolved old = StoredSchemaSources.resolve(store, previous);
                CompatibilityResult result =
                        checker.check(old.sources(), candidate.sources(), compatMode);
                for (SchemaChange change : result.violations()) {
                    violations.add("v" + previous.version() + ": " + change.ruleId()
                            + " at " + change.path() + ": " + change.message());
                }
            } catch (CompatibilityException | RegistryStoreException e) {
                violations.add("v" + previous.version() + ": comparison failed: " + e.getMessage());
            }
        }
        return List.copyOf(violations);
    }
}

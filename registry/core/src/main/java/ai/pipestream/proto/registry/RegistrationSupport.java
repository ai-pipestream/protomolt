package ai.pipestream.proto.registry;

import ai.pipestream.proto.registry.SchemaRegistryStore.WriteGate;
import ai.pipestream.proto.sources.ProtoCompilationException;
import ai.pipestream.proto.sources.ProtoSourceCompiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The registration pipeline shared by every {@link SchemaRegistryStore} implementation:
 * reference verification, write-gate enforcement and compile verification. Callers invoke the
 * steps under their own write lock, in the documented order.
 */
final class RegistrationSupport {

    private static final ProtoSourceCompiler COMPILER = new ProtoSourceCompiler();

    private RegistrationSupport() {
    }

    static String requireSubject(String subject) {
        Objects.requireNonNull(subject, "subject");
        if (subject.isBlank()) {
            throw new IllegalArgumentException("subject must not be blank");
        }
        return subject;
    }

    /** Every reference must already exist in the store. */
    static void verifyReferences(SchemaRegistryStore store, List<SchemaReference> references)
            throws ReferenceNotFoundException {
        for (SchemaReference reference : references) {
            if (store.version(reference.subject(), reference.version()).isEmpty()) {
                throw new ReferenceNotFoundException(reference);
            }
        }
    }

    /** The subject's full history, ascending by version. */
    static List<StoredSchema> history(SchemaRegistryStore store, String subject) {
        List<StoredSchema> history = new ArrayList<>();
        for (int version : store.versions(subject)) {
            store.version(subject, version).ifPresent(history::add);
        }
        return List.copyOf(history);
    }

    /**
     * Runs the write gate with the subject's effective mode, unless the gate is absent or the
     * mode is {@code NONE}.
     *
     * @throws IncompatibleRegistrationException carrying the gate's violations
     */
    static void enforceWriteGate(WriteGate gate, SchemaRegistryStore store, String subject,
                                 List<StoredSchema> history, String schemaText,
                                 List<SchemaReference> references)
            throws IncompatibleRegistrationException {
        if (gate == null) {
            return;
        }
        String mode = store.compatibilityMode(subject).orElseGet(store::globalCompatibilityMode);
        if (CompatibilityModes.NONE.equals(mode)) {
            return;
        }
        List<String> violations = gate.validate(subject, mode, history, schemaText, references, store);
        if (!violations.isEmpty()) {
            throw new IncompatibleRegistrationException(subject, violations);
        }
    }

    /**
     * Compiles the candidate together with its resolved transitive reference texts, rejecting
     * syntactically invalid or unlinkable schemas.
     *
     * @throws InvalidSchemaException carrying the compiler's message
     */
    static void compileCandidate(SchemaRegistryStore store, String subject, String schemaText,
                                 List<SchemaReference> references)
            throws InvalidSchemaException, ReferenceNotFoundException {
        StoredSchemaSources.Resolved resolved =
                StoredSchemaSources.resolve(store, subject, schemaText, references);
        try {
            COMPILER.compile(resolved.sources());
        } catch (ProtoCompilationException e) {
            throw new InvalidSchemaException(subject, e.getMessage(), e);
        }
    }
}

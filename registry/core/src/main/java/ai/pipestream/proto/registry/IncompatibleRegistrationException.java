package ai.pipestream.proto.registry;

import java.util.List;

/**
 * A registration was rejected by the store's {@link SchemaRegistryStore.WriteGate}: the
 * candidate schema violates the subject's effective compatibility mode. Carries the gate's
 * violation strings verbatim.
 */
public class IncompatibleRegistrationException extends RegistryStoreException {

    private final List<String> violations;

    public IncompatibleRegistrationException(String subject, List<String> violations) {
        super("Schema for subject " + subject + " is incompatible with earlier versions: "
                + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    /** The write gate's violation messages, in the order the gate returned them. */
    public List<String> violations() {
        return violations;
    }
}

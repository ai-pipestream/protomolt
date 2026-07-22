package ai.pipestream.proto.registry;

import java.util.Set;

/**
 * The Confluent compatibility-mode vocabulary. Modes are kept as opaque validated strings —
 * the store never interprets them beyond {@code NONE} (which disables the write gate); the
 * semantics belong to whatever {@link SchemaRegistryStore.WriteGate} is plugged in.
 */
public final class CompatibilityModes {

    /** Compatibility checking disabled: the write gate is skipped entirely. */
    public static final String NONE = "NONE";

    /** Default global mode of a fresh store, matching Confluent's default. */
    public static final String DEFAULT_GLOBAL = "BACKWARD";

    private static final Set<String> VALID = Set.of(
            "NONE", "BACKWARD", "FORWARD", "FULL",
            "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE");

    private CompatibilityModes() {
    }

    /** Whether {@code mode} is one of the seven Confluent compatibility levels. */
    public static boolean isValid(String mode) {
        return mode != null && VALID.contains(mode);
    }

    /**
     * Returns {@code mode} unchanged when valid.
     *
     * @throws IllegalArgumentException for anything outside the Confluent vocabulary
     */
    public static String requireValid(String mode) {
        if (!isValid(mode)) {
            throw new IllegalArgumentException("Invalid compatibility mode: " + mode
                    + " (valid: " + String.join(", ", VALID.stream().sorted().toList()) + ")");
        }
        return mode;
    }
}

package ai.pipestream.proto.compat;

/**
 * Compatibility policies matching Confluent Schema Registry's modes.
 *
 * <p>Directions use the reader/writer phrasing documented on {@link Impact}: BACKWARD requires
 * that a consumer of the NEW schema can read data written with the OLD schema; FORWARD requires
 * that a consumer of the OLD schema can read data written with the NEW schema; FULL requires
 * both. The {@code _TRANSITIVE} variants apply the same direction against every historical
 * version instead of only the latest (see
 * {@link CompatibilityChecker#checkAgainstHistory}).</p>
 */
public enum CompatibilityMode {

    /** No compatibility enforced: every change is reported, none is a violation. */
    NONE(false, false, false),

    /** A consumer of the NEW schema must be able to read data written with the OLD schema. */
    BACKWARD(true, false, false),

    /** A consumer of the OLD schema must be able to read data written with the NEW schema. */
    FORWARD(false, true, false),

    /** Both {@link #BACKWARD} and {@link #FORWARD}. */
    FULL(true, true, false),

    /** {@link #BACKWARD}, checked against every historical version. */
    BACKWARD_TRANSITIVE(true, false, true),

    /** {@link #FORWARD}, checked against every historical version. */
    FORWARD_TRANSITIVE(false, true, true),

    /** {@link #FULL}, checked against every historical version. */
    FULL_TRANSITIVE(true, true, true);

    private final boolean checksBackward;
    private final boolean checksForward;
    private final boolean transitive;

    CompatibilityMode(boolean checksBackward, boolean checksForward, boolean transitive) {
        this.checksBackward = checksBackward;
        this.checksForward = checksForward;
        this.transitive = transitive;
    }

    /** Whether this mode enforces the backward direction. */
    public boolean checksBackward() {
        return checksBackward;
    }

    /** Whether this mode enforces the forward direction. */
    public boolean checksForward() {
        return checksForward;
    }

    /** Whether history checks cover every version rather than only the latest. */
    public boolean isTransitive() {
        return transitive;
    }
}

package ai.protomolt.proto.agenthost;

/** The delegation authority assigned to an attached agent process. */
public enum AgentRole {
    WORKER,
    COORDINATOR;

    static AgentRole parse(String value) {
        try {
            return valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("role must be 'worker' or 'coordinator'");
        }
    }
}

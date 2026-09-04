package ai.protomolt.proto.agenthost;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** One resumable model session driven by event packets from the delegation coordinator. */
public interface AgentProvider extends AutoCloseable {

    /** Provider name recorded with the worker advertisement and local state. */
    String name();

    /** Provider-owned session id after initialization, or an empty string before it exists. */
    String sessionId();

    /** Runs one turn and returns the final response text. */
    String prompt(String prompt);

    /**
     * Receives the output schema the host expects the next replies to follow. Providers
     * with enforced structured output install it; a provider that can only be asked in the
     * prompt ignores it. Called before the first prompt and whenever the deliverable
     * contracts the host knows change the schema.
     */
    default void outputSchema(ObjectNode schema) {
    }

    @Override
    void close();
}

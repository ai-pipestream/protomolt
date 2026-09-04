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
     * Token counters for the provider session so far: prompt and output tokens as the
     * provider reports them, summed over every turn of this host's session. Empty when the
     * provider exposes no usage. The marketplace settles tokens at the end of a task from
     * these numbers; they are never an estimate.
     */
    default java.util.Optional<Usage> usage() {
        return java.util.Optional.empty();
    }

    /** Cumulative prompt and output tokens of one provider session. */
    record Usage(long promptTokens, long outputTokens) {
        public Usage plus(long prompt, long output) {
            return new Usage(promptTokens + prompt, outputTokens + output);
        }

        public long total() {
            return promptTokens + outputTokens;
        }
    }

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

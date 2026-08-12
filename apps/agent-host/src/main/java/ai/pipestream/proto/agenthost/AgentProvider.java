package ai.pipestream.proto.agenthost;

/** One resumable model session driven by event packets from the delegation coordinator. */
public interface AgentProvider extends AutoCloseable {

    /** Provider name recorded with the worker advertisement and local state. */
    String name();

    /** Provider-owned session id after initialization, or an empty string before it exists. */
    String sessionId();

    /** Runs one turn and returns the final response text. */
    String prompt(String prompt);

    @Override
    void close();
}

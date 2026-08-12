package ai.pipestream.proto.agenthost;

/** A bounded host, provider, state, or MCP failure safe to report without secret material. */
public final class AgentHostException extends RuntimeException {

    public AgentHostException(String message) {
        super(message);
    }

    public AgentHostException(String message, Throwable cause) {
        super(message, cause);
    }
}

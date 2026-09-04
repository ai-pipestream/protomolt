package ai.protomolt.proto.agenthost;

/**
 * A bounded host, provider, state, or MCP failure safe to report without secret material.
 * {@link ModelReplyException} is the one subclass: a rejected model reply, which {@link
 * AgentHost#run()} tracks and caps separately from every other failure here.
 */
public sealed class AgentHostException extends RuntimeException
        permits ModelReplyException {

    public AgentHostException(String message) {
        super(message);
    }

    public AgentHostException(String message, Throwable cause) {
        super(message, cause);
    }
}

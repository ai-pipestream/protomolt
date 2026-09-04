package ai.protomolt.proto.agenthost;

/**
 * A model's reply was rejected: it failed to parse as the required JSON turn, or it failed
 * the required-action checks in {@code AgentHost.validateRequiredActions}. This is distinct
 * from a transport, MCP, or provider-process failure, which stays a plain
 * {@link AgentHostException}: a rejected reply is retried on a minutes-scale backoff and is
 * the only failure kind that can make the host give up on a batch, because retrying it changes
 * nothing about why the model produced a bad reply.
 */
public final class ModelReplyException extends AgentHostException {

    public ModelReplyException(String message) {
        super(message);
    }

    public ModelReplyException(String message, Throwable cause) {
        super(message, cause);
    }
}

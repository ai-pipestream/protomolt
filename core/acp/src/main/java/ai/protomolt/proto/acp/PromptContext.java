package ai.protomolt.proto.acp;

/**
 * The channel a prompt turn streams back on: each call becomes one {@code session/update}
 * notification carrying a text content block, messages as {@code agent_message_chunk} and
 * thoughts as {@code agent_thought_chunk}. The agent hands an implementation to its
 * {@link PromptHandler} for the duration of one turn.
 */
public interface PromptContext {

    /** Streams one chunk of the reply the user reads, as an {@code agent_message_chunk}. */
    void sendMessage(String text);

    /** Streams one chunk of working narrative, as an {@code agent_thought_chunk}. */
    void sendThought(String text);
}

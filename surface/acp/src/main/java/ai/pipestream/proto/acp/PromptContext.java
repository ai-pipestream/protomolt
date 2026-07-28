package ai.pipestream.proto.acp;

/**
 * The channel a prompt turn streams back on: each call becomes one {@code session/update}
 * notification carrying a text content block, messages as {@code agent_message_chunk} and
 * thoughts as {@code agent_thought_chunk}.
 */
interface PromptContext {

    void sendMessage(String text);

    void sendThought(String text);
}

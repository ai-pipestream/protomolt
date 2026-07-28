package ai.pipestream.proto.acp;

/**
 * What one prompt turn does. The joined text of the prompt's text content blocks comes in and
 * the handler streams its output back through the {@link PromptContext}. Each turn runs on its
 * own virtual thread, so a handler that blocks (running a compiler, calling a service) parks
 * rather than pins and never stalls the connection's read loop. A thrown exception answers the
 * prompt with a JSON-RPC internal error; the session survives.
 */
@FunctionalInterface
public interface PromptHandler {

    /**
     * Runs one prompt turn.
     *
     * @param promptText the prompt's text content blocks joined by newlines
     * @param context the channel to stream message and thought chunks back on
     */
    void run(String promptText, PromptContext context);
}

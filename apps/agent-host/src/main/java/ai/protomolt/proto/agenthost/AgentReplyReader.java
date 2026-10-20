package ai.protomolt.proto.agenthost;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Finds the JSON value in a provider reply. Operates on reply text only:
 * nothing here sees credentials, provider processes or session state.
 */
final class AgentReplyReader {

    /** Rejects text after the JSON value, so a candidate object is the whole candidate. */
    private static final ObjectReader STRICT = new ObjectMapper().reader()
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private AgentReplyReader() {
    }

    /**
     * Reads the JSON value in a provider reply. Providers that join every message chunk of
     * a turn (the ACP provider) hand over the model's narration between tool calls as well
     * as its final answer, and some models wrap the answer in a Markdown fence. The reply
     * is taken as a whole when it parses; otherwise the turn is the last complete JSON
     * object in it: the earliest opening brace from which the text up to the final closing
     * brace parses with no trailing tokens. An earlier, abandoned object followed by more
     * text therefore never wins over the answer that ends the reply.
     */
    static JsonNode readResponse(String text) {
        String trimmed = text.strip();
        JsonProcessingException whole;
        try {
            return STRICT.readTree(trimmed);
        } catch (JsonProcessingException e) {
            whole = e;
        }
        String completed = closeTrailingOpeners(trimmed);
        if (completed != null) {
            try {
                return STRICT.readTree(completed);
            } catch (JsonProcessingException incomplete) {
                // the missing closers were not the only defect
            }
        }
        int end = trimmed.lastIndexOf('}');
        int start = trimmed.indexOf('{');
        while (start >= 0 && start < end) {
            try {
                JsonNode candidate = STRICT.readTree(trimmed.substring(start, end + 1));
                if (candidate.isObject() && candidate.has("handledEventCursors")) {
                    return candidate;
                }
            } catch (JsonProcessingException partial) {
                // not a complete object from this brace; try the next one
            }
            start = trimmed.indexOf('{', start + 1);
        }
        throw new ModelReplyException("agent response is not JSON: "
                + whole.getOriginalMessage() + " at line " + whole.getLocation().getLineNr()
                + " column " + whole.getLocation().getColumnNr());
    }

    /**
     * Returns the text with the closers its unclosed objects and arrays still need appended,
     * or null when nothing is open at the end or the text ends inside a string. Models
     * regularly stop one brace short of a well-formed reply; nothing else is repaired.
     */
    private static String closeTrailingOpeners(String text) {
        StringBuilder closers = new StringBuilder();
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> inString = true;
                case '{' -> closers.append('}');
                case '[' -> closers.append(']');
                case '}', ']' -> {
                    if (closers.isEmpty() || closers.charAt(closers.length() - 1) != c) {
                        return null;
                    }
                    closers.setLength(closers.length() - 1);
                }
                default -> {
                    // other characters do not affect nesting
                }
            }
        }
        if (inString || closers.isEmpty()) {
            return null;
        }
        return text + closers.reverse();
    }
}

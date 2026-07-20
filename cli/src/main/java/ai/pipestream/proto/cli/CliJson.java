package ai.pipestream.proto.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** JSON parsing shared by the CLI and the console. */
final class CliJson {

    private CliJson() {
    }

    /**
     * Parses verb input. Every verb takes a JSON object, so an array, string or number is
     * rejected here with the shape it was given rather than as a cast failure further in.
     *
     * @throws JsonProcessingException when the text is not JSON
     * @throws IllegalArgumentException when the text is JSON but not an object
     */
    static ObjectNode readObject(ObjectMapper mapper, String json) throws JsonProcessingException {
        JsonNode node = mapper.readTree(json);
        if (!node.isObject()) {
            throw new IllegalArgumentException(
                    "Input must be a JSON object, got " + node.getNodeType().name().toLowerCase());
        }
        return (ObjectNode) node;
    }
}

package ai.pipestream.proto.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

/** One independently mountable collection of MCP resources. */
public interface McpResources {

    /** Lists the resources served by this collection. */
    ArrayNode list(ObjectMapper mapper);

    /** Reads one URI, or empty when this collection does not own it. */
    Optional<ObjectNode> read(ObjectMapper mapper, String uri);
}

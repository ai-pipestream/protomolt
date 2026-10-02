package ai.protomolt.proto.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;

/** Bridges the original resource API without changing its public Page record type. */
final class ResourceAdapters {
    private ResourceAdapters() {}
    static ai.protomolt.proto.mcp.transport.McpResources toTransport(McpResources source) {
        if (source == null) return null;
        return new ai.protomolt.proto.mcp.transport.McpResources() {
            @Override public ArrayNode list(ObjectMapper mapper) { return source.list(mapper); }
            @Override public ArrayNode templates(ObjectMapper mapper) { return source.templates(mapper); }
            @Override public Page page(ObjectMapper mapper, String cursor, int size) {
                McpResources.Page page = source.page(mapper, cursor, size);
                return new Page(page.resources(), page.nextCursor());
            }
            @Override public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
                return source.read(mapper, uri);
            }
        };
    }
}

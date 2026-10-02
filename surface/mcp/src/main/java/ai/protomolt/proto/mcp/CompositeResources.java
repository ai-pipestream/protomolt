package ai.protomolt.proto.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Compatibility view of the transport's ordered resource composition. */
public final class CompositeResources implements McpResources {
    private final ai.protomolt.proto.mcp.transport.CompositeResources delegate;
    public CompositeResources(List<McpResources> resources) {
        delegate = new ai.protomolt.proto.mcp.transport.CompositeResources(
                List.copyOf(resources).stream().map(ResourceAdapters::toTransport).toList());
    }
    public static McpResources of(McpResources... resources) {
        List<McpResources> present = Arrays.stream(resources).filter(Objects::nonNull).toList();
        return present.isEmpty() ? null
                : present.size() == 1 ? present.getFirst() : new CompositeResources(present);
    }
    @Override public ArrayNode list(ObjectMapper mapper) { return delegate.list(mapper); }
    @Override public ArrayNode templates(ObjectMapper mapper) { return delegate.templates(mapper); }
    @Override public Page page(ObjectMapper mapper, String cursor, int size) {
        var page = delegate.page(mapper, cursor, size);
        return new Page(page.resources(), page.nextCursor());
    }
    @Override public Optional<ObjectNode> read(ObjectMapper mapper, String uri) { return delegate.read(mapper, uri); }
}

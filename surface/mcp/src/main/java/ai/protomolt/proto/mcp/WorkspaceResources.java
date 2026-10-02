package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;

/** Compatibility view of the transport's live workspace inventory. */
public final class WorkspaceResources implements McpResources {
    public static final String URI = ai.protomolt.proto.mcp.transport.WorkspaceResources.URI;
    private final ai.protomolt.proto.mcp.transport.WorkspaceResources delegate;
    public WorkspaceResources(ActionCatalog catalog, String name, String version, String instructions) {
        delegate = new ai.protomolt.proto.mcp.transport.WorkspaceResources(catalog, name, version, instructions);
    }
    @Override public ArrayNode list(ObjectMapper mapper) { return delegate.list(mapper); }
    @Override public Optional<ObjectNode> read(ObjectMapper mapper, String uri) { return delegate.read(mapper, uri); }
    static ObjectNode toolCatalog(ActionCatalog catalog, ObjectMapper mapper) {
        return ai.protomolt.proto.mcp.transport.WorkspaceResources.toolCatalog(catalog, mapper);
    }
    static ObjectNode toolCatalog(ArrayNode manifest, ObjectMapper mapper) {
        return ai.protomolt.proto.mcp.transport.WorkspaceResources.toolCatalog(manifest, mapper);
    }
    static String fingerprint(ArrayNode manifest) {
        return ai.protomolt.proto.mcp.transport.WorkspaceResources.fingerprint(manifest);
    }
}

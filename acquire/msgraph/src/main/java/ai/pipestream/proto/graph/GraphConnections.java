package ai.pipestream.proto.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The Copilot connectors (external connections) API — the same door Microsoft's
 * closed-source, Windows-only Graph Connector Agent walks through, spoken directly. This is
 * the roll-your-own-agent lane: create a connection, register its schema (rendered from
 * indexing hints by {@link GraphSchemas}), and PUT external items; Microsoft 365 Search and
 * Copilot take it from there. Needs {@code ExternalConnection.ReadWrite.OwnedBy} and
 * {@code ExternalItem.ReadWrite.OwnedBy} application permissions with admin consent.
 */
public final class GraphConnections {

    private final GraphClient graph;

    public GraphConnections(GraphClient graph) {
        this.graph = Objects.requireNonNull(graph, "graph");
    }

    public JsonNode list() throws IOException, InterruptedException {
        return graph.get("/external/connections");
    }

    /** Creates a connection; {@code id} is 3-32 alphanumeric characters, tenant-unique. */
    public JsonNode create(String id, String name, String description)
            throws IOException, InterruptedException {
        ObjectNode body = GraphClient.object();
        body.put("id", id);
        body.put("name", name);
        body.put("description", description);
        return graph.post("/external/connections", body);
    }

    public JsonNode get(String connectionId) throws IOException, InterruptedException {
        return graph.get("/external/connections/" + connectionId);
    }

    public void delete(String connectionId) throws IOException, InterruptedException {
        graph.delete("/external/connections/" + connectionId);
    }

    /**
     * Registers the connection's schema and waits for Graph's async provisioning to
     * complete (it can take minutes on a fresh connection). The schema comes from
     * {@link GraphSchemas#connectionSchema} — indexing hints declared once in the proto.
     */
    public void registerSchema(String connectionId, ObjectNode schema, Duration timeout)
            throws IOException, InterruptedException {
        Optional<String> operation = graph.postAsync(
                "/external/connections/" + connectionId + "/schema", schema);
        if (operation.isPresent()) {
            graph.awaitOperation(operation.get(), timeout);
        }
    }

    public JsonNode schema(String connectionId) throws IOException, InterruptedException {
        return graph.get("/external/connections/" + connectionId + "/schema");
    }

    /**
     * Creates or replaces one external item: {@code properties} matching the registered
     * schema, optional text {@code content} for full-text search and Copilot grounding, and
     * an ACL (use {@link #everyoneAcl()} for tenant-visible items).
     */
    public JsonNode putItem(String connectionId, String itemId, ObjectNode properties,
                            String content, JsonNode acl)
            throws IOException, InterruptedException {
        ObjectNode item = GraphClient.object();
        item.set("acl", acl);
        item.set("properties", properties);
        if (content != null) {
            ObjectNode contentNode = item.putObject("content");
            contentNode.put("value", content);
            contentNode.put("type", "text");
        }
        return graph.put("/external/connections/" + connectionId + "/items/" + itemId, item);
    }

    public void deleteItem(String connectionId, String itemId)
            throws IOException, InterruptedException {
        graph.delete("/external/connections/" + connectionId + "/items/" + itemId);
    }

    /** The simplest ACL: visible to everyone in the tenant. */
    public static JsonNode everyoneAcl() {
        ObjectNode grant = GraphClient.object();
        grant.put("type", "everyone");
        grant.put("value", "everyone");
        grant.put("accessType", "grant");
        return GraphClient.object().arrayNode().add(grant);
    }
}

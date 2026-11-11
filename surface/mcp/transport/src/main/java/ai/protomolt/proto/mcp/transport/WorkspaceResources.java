package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.Caller;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/** Compact, always-present MCP bootstrap for the live server and its exact tool inventory. */
public final class WorkspaceResources implements McpResources {

    /** Stable URI clients can read immediately after initialization. */
    public static final String URI = "protomolt://workspace";

    private final ActionCatalog catalog;
    private final String serverName;
    private final String serverVersion;
    private final String instructions;

    public WorkspaceResources(ActionCatalog catalog, String serverName, String serverVersion,
                              String instructions) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.serverName = Objects.requireNonNull(serverName, "serverName");
        this.serverVersion = Objects.requireNonNull(serverVersion, "serverVersion");
        this.instructions = Objects.requireNonNull(instructions, "instructions");
    }

    @Override
    public ArrayNode list(ObjectMapper mapper) {
        ArrayNode resources = mapper.createArrayNode();
        ObjectNode workspace = resources.addObject();
        workspace.put("uri", URI);
        workspace.put("name", "workspace");
        workspace.put("description",
                "Live ProtoMolt server identity, workflow, and exact tool-catalog fingerprint");
        workspace.put("mimeType", "application/json");
        return resources;
    }

    /**
     * The workspace only describes the server to its caller — identity, guidance, and
     * the caller's own tool inventory — so it stays behind authentication alone.
     */
    @Override
    public boolean authenticatedOnly() {
        return true;
    }

    @Override
    public boolean owns(String uri) {
        return URI.equals(uri);
    }

    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri) {
        return read(mapper, uri, Caller.operator());
    }

    @Override
    public Optional<ObjectNode> read(ObjectMapper mapper, String uri, Caller caller) {
        if (!URI.equals(uri)) {
            return Optional.empty();
        }
        ObjectNode document = mapper.createObjectNode();
        ObjectNode server = document.putObject("server");
        server.put("name", serverName);
        server.put("version", serverVersion);
        server.put("protocolVersion", McpServer.PROTOCOL_VERSION);
        // The inventory is the caller's own view, exactly as tools/list serves it:
        // names of tools outside the caller's scopes are not the caller's to learn.
        document.set("toolCatalog", toolCatalog(catalog.list(caller), mapper));
        document.put("instructions", instructions);
        document.put("refreshGuidance", "Compare the live tool count and fingerprint with the "
                + "client-visible catalog; reconnect the MCP client when they differ.");

        ObjectNode contents = mapper.createObjectNode();
        contents.put("uri", URI);
        contents.put("mimeType", "application/json");
        contents.put("text", document.toString());
        return Optional.of(contents);
    }

    public static ObjectNode toolCatalog(ActionCatalog catalog, ObjectMapper mapper) {
        return toolCatalog(catalog.list(), mapper);
    }

    public static ObjectNode toolCatalog(ArrayNode manifest, ObjectMapper mapper) {
        ObjectNode summary = mapper.createObjectNode();
        summary.put("count", manifest.size());
        summary.put("fingerprint", fingerprint(manifest));
        ArrayNode names = summary.putArray("names");
        manifest.forEach(tool -> names.add(tool.path("name").asText()));
        return summary;
    }

    public static String fingerprint(ArrayNode manifest) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return "sha256:" + HexFormat.of().formatHex(
                    digest.digest(manifest.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}

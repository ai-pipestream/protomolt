package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.Caller;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Full-distribution compatibility facade over the independently reusable MCP transport.
 * Existing constructor and session types are retained; protocol behavior lives in transport.
 */
public final class McpServer {
    public static final String PROTOCOL_VERSION = ai.protomolt.proto.mcp.transport.McpServer.PROTOCOL_VERSION;
    public static final String DEFAULT_INSTRUCTIONS =
            "ProtoMolt: read `protomolt://workspace`; reconnect if tool count differs. Use "
                    + "`service-register`, `service-inspect`, then `service-invoke`; `reflect`/"
                    + "`grpc-invoke` for ad hoc calls. Use descriptor-defined fields. "
                    + "Compose: `suggest-mappings`, `check-workflow`, `compile-workflow`, "
                    + "`record-workflow-run`, `replay-workflow`, `promote-workflow`. "
                    + "Receipts: `export-work-record`, `verify-work-record`. "
                    + "Mesh: read `mesh-snapshot`; renew leases. `generate-stubs` for native "
                    + "clients. Check `ok`; never guess payloads.";

    private final ai.protomolt.proto.mcp.transport.McpServer delegate;

    public McpServer(ActionCatalog catalog, McpResources resources, String name, String version) {
        this(catalog, resources, name, version, DEFAULT_INSTRUCTIONS);
    }
    public McpServer(ActionCatalog catalog, RegistryResources resources, String name, String version) {
        this(catalog, (McpResources) resources, name, version, DEFAULT_INSTRUCTIONS);
    }
    public McpServer(ActionCatalog catalog, McpResources resources, String name, String version,
            String instructions) {
        delegate = new ai.protomolt.proto.mcp.transport.McpServer(catalog,
                ResourceAdapters.toTransport(resources), name, version, instructions);
    }
    public static boolean supportsProtocolVersion(String version) {
        return ai.protomolt.proto.mcp.transport.McpServer.supportsProtocolVersion(version);
    }
    public void run(InputStream in, OutputStream out) throws IOException { delegate.run(in, out); }
    public Optional<ObjectNode> handle(JsonNode message) { return delegate.handle(message); }
    public Optional<ObjectNode> handle(JsonNode message, Caller caller) { return delegate.handle(message, caller); }
    public Session openSession() { return new Session(delegate.openSession()); }
    public Session openSession(Caller caller) { return new Session(delegate.openSession(caller)); }
    static boolean isMoonshotClient(String name) {
        return ai.protomolt.proto.mcp.transport.McpServer.isMoonshotClient(name);
    }
    static ArrayNode sanitizeForMoonshot(ArrayNode manifest) {
        return ai.protomolt.proto.mcp.transport.McpServer.sanitizeForMoonshot(manifest);
    }
    static void sanitizeSchemaForMoonshot(JsonNode node) {
        ai.protomolt.proto.mcp.transport.McpServer.sanitizeSchemaForMoonshot(node);
    }

    /** Retains the original nested session API for existing HTTP and stdio hosts. */
    public final class Session implements AutoCloseable {
        public enum State { NEW, INITIALIZED, READY, CLOSED }
        private final ai.protomolt.proto.mcp.transport.McpServer.Session session;
        private Session(ai.protomolt.proto.mcp.transport.McpServer.Session session) { this.session = session; }
        public State state() { return State.valueOf(session.state().name()); }
        public boolean isMoonshotDialect() { return session.isMoonshotDialect(); }
        public void setMoonshotDialect(boolean enabled) { session.setMoonshotDialect(enabled); }
        public String negotiatedProtocolVersion() { return session.negotiatedProtocolVersion(); }
        public Optional<ObjectNode> handle(JsonNode message) { return session.handle(message); }
        public boolean isToolCallReady(JsonNode message) { return session.isToolCallReady(message); }
        public Future<Optional<ObjectNode>> submit(JsonNode message) { return session.submit(message); }
        public Future<Optional<ObjectNode>> submit(JsonNode message, Consumer<Optional<ObjectNode>> completion) {
            return session.submit(message, completion);
        }
        public void dispatch(JsonNode message, Consumer<Optional<ObjectNode>> completion) {
            session.dispatch(message, completion);
        }
        public void awaitInFlight(long timeoutMillis) { session.awaitInFlight(timeoutMillis); }
        @Override public void close() { session.close(); }
    }
}

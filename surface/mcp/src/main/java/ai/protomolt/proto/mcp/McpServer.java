package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * A Model Context Protocol server over an {@link ActionCatalog}: every action becomes an MCP
 * tool (the catalog's manifest already carries the name, tool-use description, and JSON Schema
 * input MCP requires), and a {@link McpResources} optionally exposes a schema registry's
 * subjects as MCP resources.
 *
 * <p>The transport is the protocol's stdio framing: one JSON-RPC 2.0 message per line, requests
 * answered as requests complete, notifications consumed silently. {@link #handle(JsonNode)} is the pure
 * message-in/message-out core, so tests and alternative transports (an HTTP mount, a framework
 * adapter) can drive the server without streams. Nothing here is framework-aware; Spring and
 * Quarkus MCP hosts can register the same catalog through their own programmatic APIs.</p>
 */
public final class McpServer {

    private static final Logger LOG = LoggerFactory.getLogger(McpServer.class);

    /** Latest protocol revision this server implements. */
    public static final String PROTOCOL_VERSION = "2025-06-18";

    /**
     * Guidance shown to an MCP client during initialization when no custom instructions are
     * supplied. The opening is deliberately self-contained so clients that truncate
     * initialization metadata still get the complete gRPC workflow.
     */
    public static final String DEFAULT_INSTRUCTIONS =
            "ProtoMolt: read `protomolt://workspace`; reconnect if tool count differs. Use "
                    + "`service-register`, `service-inspect`, then `service-invoke`; `reflect`/"
                    + "`grpc-invoke` for ad hoc calls. Use descriptor-defined fields. "
                    + "Compose: `suggest-mappings`, `check-workflow`, `compile-workflow`, "
                    + "`record-workflow-run`, `replay-workflow`, `promote-workflow`. "
                    + "Receipts: `export-work-record`, `verify-work-record`. "
                    + "Mesh: read `mesh-snapshot`; renew leases. `generate-stubs` for native "
                    + "clients. Check `ok`; never guess payloads.";

    private static final List<String> SUPPORTED_VERSIONS =
            List.of("2025-06-18", "2025-03-26", "2024-11-05");
    private static final int RESOURCE_PAGE_SIZE = 100;
    private static final int MAX_IN_FLIGHT_PER_SESSION = 64;

    private final ActionCatalog catalog;
    private final McpResources resources;
    private final String serverName;
    private final String serverVersion;
    private final String instructions;
    private final ObjectMapper mapper = new ObjectMapper();

    public static boolean supportsProtocolVersion(String version) {
        return version != null && SUPPORTED_VERSIONS.contains(version);
    }

    /**
     * @param catalog   tools; every catalog action is exposed
     * @param resources registry-backed resources, or {@code null} to serve tools only
     */
    public McpServer(ActionCatalog catalog, McpResources resources,
                     String serverName, String serverVersion) {
        this(catalog, resources, serverName, serverVersion, DEFAULT_INSTRUCTIONS);
    }

    /**
     * Binary-compatible constructor retained for consumers compiled against the original
     * registry-only resource surface.
     */
    public McpServer(ActionCatalog catalog, RegistryResources resources,
                     String serverName, String serverVersion) {
        this(catalog, (McpResources) resources, serverName, serverVersion,
                DEFAULT_INSTRUCTIONS);
    }

    /**
     * Creates an MCP server with explicit client guidance returned in {@code initialize}.
     * Passing an empty string omits the optional MCP {@code instructions} member.
     *
     * @param catalog tools; every catalog action is exposed
     * @param resources registry-backed resources, or {@code null} to serve tools only
     * @param serverName server identity exposed during initialization
     * @param serverVersion server version exposed during initialization
     * @param instructions guidance for an MCP client, or an empty string to omit it
     */
    public McpServer(ActionCatalog catalog, McpResources resources,
                     String serverName, String serverVersion, String instructions) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.serverName = Objects.requireNonNull(serverName, "serverName");
        this.serverVersion = Objects.requireNonNull(serverVersion, "serverVersion");
        this.instructions = Objects.requireNonNull(instructions, "instructions");
        this.resources = CompositeResources.of(
                new WorkspaceResources(catalog, serverName, serverVersion, instructions),
                resources);
    }

    /**
     * Reads newline-delimited JSON-RPC messages from {@code in} until end of stream, writing
     * one response line per request. Malformed JSON is answered with a parse error rather than
     * terminating the session.
     */
    public void run(InputStream in, OutputStream out) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        Object writeLock = new Object();
        try (Session session = openSession()) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode message;
                try {
                    message = mapper.readTree(line);
                } catch (JsonProcessingException e) {
                    synchronized (writeLock) {
                        write(writer, JsonRpc.error(mapper, null, JsonRpc.PARSE_ERROR, "Parse error"));
                    }
                    continue;
                }
                session.dispatch(message, response -> response.ifPresent(value -> {
                    synchronized (writeLock) {
                        write(writer, value);
                    }
                }));
            }
            session.awaitInFlight(30_000);
        }
    }

    /** Opens an isolated MCP lifecycle for a stdio connection. HTTP mounts should not retain it. */
    public Session openSession() {
        return new Session(Caller.operator());
    }

    /**
     * Opens a lifecycle whose every dispatch runs as {@code caller}: the transport resolves
     * the credential once, at initialization, and the caller rides the session from then on.
     */
    public Session openSession(Caller caller) {
        return new Session(Objects.requireNonNull(caller, "caller"));
    }

    /**
     * Dispatches one JSON-RPC message with process authority. Requests produce a response;
     * notifications and client-side responses produce none.
     */
    public Optional<ObjectNode> handle(JsonNode message) {
        return handle(message, Caller.operator());
    }

    /** Dispatches one JSON-RPC message as {@code caller}. */
    public Optional<ObjectNode> handle(JsonNode message, Caller caller) {
        if (!message.isObject()) {
            return Optional.of(JsonRpc.error(mapper, null, JsonRpc.INVALID_REQUEST, "Invalid request"));
        }
        if (!message.has("method")) {
            if (message.has("result") || message.has("error")) {
                // A response to a server-initiated request; this server never sends any.
                return Optional.empty();
            }
            return Optional.of(JsonRpc.error(mapper, message.get("id"), JsonRpc.INVALID_REQUEST,
                    "Invalid request"));
        }
        if (JsonRpc.isNotification(message)) {
            return Optional.empty();
        }
        JsonNode id = message.get("id");
        String method = message.get("method").asText();
        JsonNode params = message.has("params") ? message.get("params") : mapper.createObjectNode();
        try {
            return switch (method) {
                case "initialize" -> Optional.of(JsonRpc.result(mapper, id,
                        initialize(params, caller)));
                case "ping" -> Optional.of(JsonRpc.result(mapper, id, mapper.createObjectNode()));
                case "tools/list" -> Optional.of(JsonRpc.result(mapper, id, listTools(caller)));
                case "tools/call" -> Optional.of(JsonRpc.result(mapper, id,
                        callTool(params, caller)));
                case "resources/list" -> Optional.of(JsonRpc.result(mapper, id, listResources(params)));
                case "resources/templates/list" -> Optional.of(JsonRpc.result(mapper, id,
                        listResourceTemplates(params)));
                case "resources/read" -> readResource(params)
                        .map(contents -> JsonRpc.result(mapper, id, contents))
                        .or(() -> Optional.of(JsonRpc.error(mapper, id, JsonRpc.RESOURCE_NOT_FOUND,
                                "Unknown resource: " + params.path("uri").asText())));
                default -> Optional.of(JsonRpc.error(mapper, id, JsonRpc.METHOD_NOT_FOUND,
                        "Method not found: " + method));
            };
        } catch (IllegalArgumentException e) {
            return Optional.of(JsonRpc.error(mapper, id, JsonRpc.INVALID_PARAMS, e.getMessage()));
        } catch (Exception e) {
            // Exception class names and messages can leak paths, targets, or upstream
            // detail; the wire gets a correlation id, the log gets the stack trace.
            String correlationId = java.util.UUID.randomUUID().toString();
            LOG.error("MCP request '{}' failed, correlation id {}", method, correlationId, e);
            return Optional.of(JsonRpc.error(mapper, id, JsonRpc.INTERNAL_ERROR,
                    "Internal error (correlation id " + correlationId + ")"));
        }
    }

    private ObjectNode initialize(JsonNode params, Caller caller) {
        if (params == null || !params.isObject()) {
            throw new IllegalArgumentException("initialize params must be an object");
        }
        String requested = params.path("protocolVersion").asText(PROTOCOL_VERSION);
        ObjectNode result = mapper.createObjectNode();
        result.put("protocolVersion",
                supportsProtocolVersion(requested) ? requested : PROTOCOL_VERSION);
        ObjectNode capabilities = result.putObject("capabilities");
        capabilities.putObject("tools").put("listChanged", false);
        capabilities.putObject("resources")
                .put("subscribe", false)
                .put("listChanged", false);
        ObjectNode serverInfo = result.putObject("serverInfo");
        serverInfo.put("name", serverName);
        serverInfo.put("version", serverVersion);
        addToolCatalogMetadata(result, catalog.list(caller));
        if (!instructions.isEmpty()) {
            result.put("instructions", instructions);
        }
        return result;
    }

    private ObjectNode listTools(Caller caller) {
        ObjectNode result = mapper.createObjectNode();
        // The catalog manifest entries ({name, description, inputSchema}) are already the
        // MCP tool shape; inputSchema is JSON Schema in both worlds. The manifest is the
        // caller's view: only tools whose scope the caller holds.
        ArrayNode manifest = catalog.list(caller);
        result.set("tools", manifest);
        addToolCatalogMetadata(result, manifest);
        return result;
    }

    private void addToolCatalogMetadata(ObjectNode result, ArrayNode manifest) {
        ObjectNode toolCatalog = WorkspaceResources.toolCatalog(manifest, mapper);
        ObjectNode metadata = result.putObject("_meta");
        metadata.put("ai.pipestream.protomolt/toolCatalogFingerprint",
                toolCatalog.path("fingerprint").asText());
        metadata.put("ai.pipestream.protomolt/toolCount", toolCatalog.path("count").asInt());
        metadata.put("ai.pipestream.protomolt/workspace", WorkspaceResources.URI);
    }

    private ObjectNode callTool(JsonNode params, Caller caller) {
        String name = params.path("name").asText(null);
        if (name == null) {
            throw new IllegalArgumentException("tools/call requires params.name");
        }
        JsonNode arguments = params.path("arguments");
        if (arguments.isMissingNode() || arguments.isNull()) {
            arguments = mapper.createObjectNode();
        }
        if (!arguments.isObject()) {
            throw new IllegalArgumentException("tools/call arguments must be an object");
        }
        try {
            ObjectNode output = catalog.execute(name, (ObjectNode) arguments, caller);
            return toolResult(output, false);
        } catch (ActionException e) {
            // Tool execution failures are results with isError, not protocol errors, so the
            // calling model sees the structured envelope and can repair its input.
            return toolResult(e.toJson(mapper), true);
        }
    }

    private ObjectNode toolResult(ObjectNode payload, boolean isError) {
        ObjectNode result = mapper.createObjectNode();
        ArrayNode content = result.putArray("content");
        ObjectNode text = content.addObject();
        text.put("type", "text");
        text.put("text", payload.toString());
        result.set("structuredContent", payload);
        result.put("isError", isError);
        return result;
    }

    private ObjectNode listResources(JsonNode params) {
        if (params == null || !params.isObject()) {
            throw new IllegalArgumentException("resources/list params must be an object");
        }
        JsonNode cursor = params.get("cursor");
        String cursorValue = null;
        if (cursor != null && !cursor.isNull()) {
            if (!cursor.isTextual() || cursor.asText().isBlank()) {
                throw new IllegalArgumentException("resources/list cursor must be a non-empty string");
            }
            cursorValue = cursor.asText();
        }
        if (resources == null && cursorValue != null) {
            throw new IllegalArgumentException("resource cursor is invalid");
        }
        McpResources.Page page = resources == null
                ? new McpResources.Page(mapper.createArrayNode(), null)
                : resources.page(mapper, cursorValue, RESOURCE_PAGE_SIZE);
        ObjectNode result = mapper.createObjectNode();
        result.set("resources", page.resources());
        if (page.nextCursor() != null) {
            result.put("nextCursor", page.nextCursor());
        }
        return result;
    }

    private ObjectNode listResourceTemplates(JsonNode params) {
        if (params == null || !params.isObject()) {
            throw new IllegalArgumentException("resources/templates/list params must be an object");
        }
        int offset = cursorOffset(params.get("cursor"), "resource template");
        ArrayNode all = resources.templates(mapper);
        if (offset > all.size()) {
            throw new IllegalArgumentException("resource template cursor is invalid");
        }
        int end = Math.min(offset + RESOURCE_PAGE_SIZE, all.size());
        ArrayNode page = mapper.createArrayNode();
        for (int i = offset; i < end; i++) {
            page.add(all.get(i));
        }
        ObjectNode result = mapper.createObjectNode();
        result.set("resourceTemplates", page);
        if (end < all.size()) {
            result.put("nextCursor", Integer.toString(end));
        }
        return result;
    }

    private static int cursorOffset(JsonNode cursor, String label) {
        if (cursor == null || cursor.isNull()) {
            return 0;
        }
        if (!cursor.isTextual() || cursor.asText().isBlank()) {
            throw new IllegalArgumentException(label + " cursor must be a non-empty string");
        }
        try {
            int offset = Integer.parseInt(cursor.asText());
            if (offset < 0) {
                throw new NumberFormatException();
            }
            return offset;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " cursor is invalid");
        }
    }

    /** Per-connection MCP lifecycle used by stdio and the streamable HTTP adapter. */
    public final class Session implements AutoCloseable {

        public enum State {
            NEW, INITIALIZED, READY, CLOSED
        }

        private final Caller caller;
        private volatile State state = State.NEW;
        private volatile String negotiatedProtocolVersion;

        private Session(Caller caller) {
            this.caller = caller;
        }
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final ConcurrentMap<String, FutureTask<Optional<ObjectNode>>> inFlight =
                new ConcurrentHashMap<>();
        private final java.util.concurrent.Semaphore inFlightSlots =
                new java.util.concurrent.Semaphore(MAX_IN_FLIGHT_PER_SESSION);

        public State state() {
            return state;
        }

        public String negotiatedProtocolVersion() {
            return negotiatedProtocolVersion;
        }

        public Optional<ObjectNode> handle(JsonNode message) {
            if (message == null || !message.isObject()) {
                return Optional.of(JsonRpc.error(mapper, null, JsonRpc.INVALID_REQUEST,
                        "Invalid request"));
            }
            if (!message.has("method")) {
                return McpServer.this.handle(message, caller);
            }
            String method = message.get("method").asText();
            if (JsonRpc.isNotification(message)) {
                handleNotification(method, message.path("params"));
                return Optional.empty();
            }
            JsonNode id = message.get("id");
            if ("initialize".equals(method)) {
                if (state != State.NEW) {
                    return Optional.of(JsonRpc.error(mapper, id, JsonRpc.INVALID_REQUEST,
                            "initialize must be the first request"));
                }
                ObjectNode result;
                try {
                    result = initialize(message.has("params")
                            ? message.get("params") : mapper.createObjectNode(), caller);
                } catch (IllegalArgumentException e) {
                    return Optional.of(JsonRpc.error(mapper, id, JsonRpc.INVALID_PARAMS,
                            e.getMessage()));
                }
                negotiatedProtocolVersion = result.path("protocolVersion").asText(PROTOCOL_VERSION);
                state = State.INITIALIZED;
                return Optional.of(JsonRpc.result(mapper, id, result));
            }
            if (state == State.NEW) {
                return Optional.of(lifecycleError(id, "initialize is required before " + method));
            }
            if (state == State.INITIALIZED) {
                return Optional.of(lifecycleError(id,
                        "notifications/initialized is required before " + method));
            }
            if (state == State.CLOSED) {
                return Optional.of(lifecycleError(id, "MCP session is closed"));
            }
            return McpServer.this.handle(message, caller);
        }

        private void handleNotification(String method, JsonNode params) {
            switch (method) {
                case "notifications/initialized" -> {
                    if (state == State.INITIALIZED) {
                        state = State.READY;
                    }
                }
                case "notifications/cancelled" -> {
                    JsonNode requestId = params == null ? null : params.get("requestId");
                    if (requestId != null && !requestId.isNull()) {
                        Future<?> future = inFlight.remove(idKey(requestId));
                        if (future != null) {
                            future.cancel(true);
                        }
                    }
                }
                default -> {
                    // Unknown notifications are intentionally consumed without a response.
                }
            }
        }

        private ObjectNode lifecycleError(JsonNode id, String message) {
            return JsonRpc.error(mapper, id, JsonRpc.INVALID_REQUEST, message);
        }

        public boolean isToolCallReady(JsonNode message) {
            return message != null && message.isObject() && message.has("method")
                    && "tools/call".equals(message.get("method").asText())
                    && !JsonRpc.isNotification(message) && state == State.READY;
        }

        /** Runs a ready tool request without blocking the reader or its transport. */
        public Future<Optional<ObjectNode>> submit(JsonNode message) {
            return submit(message, null);
        }

        /** Runs a ready tool request and invokes the completion callback unless cancelled. */
        public Future<Optional<ObjectNode>> submit(JsonNode message,
                                                   Consumer<Optional<ObjectNode>> completion) {
            if (!isToolCallReady(message)) {
                throw new IllegalArgumentException("tool request is not valid in this session state");
            }
            if (!inFlightSlots.tryAcquire()) {
                throw new java.util.concurrent.RejectedExecutionException(
                        "MCP session in-flight tool limit is exhausted");
            }
            String key = idKey(message.get("id"));
            FutureTask<Optional<ObjectNode>> task = new FutureTask<>(() -> {
                try {
                    Optional<ObjectNode> response = McpServer.this.handle(message, caller);
                    if (completion != null && inFlight.containsKey(key)
                            && !Thread.currentThread().isInterrupted()) {
                        completion.accept(response);
                    }
                    return response;
                } finally {
                    // Release the id before the task completes. A FutureTask wakes its
                    // waiters before it runs done(), so a caller that reuses the id for
                    // its next request, which is correct when the previous one has
                    // answered, would otherwise race the cleanup and be refused as a
                    // duplicate. Removing by key alone is safe here: nothing can claim
                    // the key while this task still holds it.
                    inFlight.remove(key);
                }
            }) {
                @Override
                protected void done() {
                    // The body's own release covers the normal path. This covers a task
                    // cancelled before it ever ran, and owns the slot either way.
                    inFlight.remove(key, this);
                    inFlightSlots.release();
                }
            };
            if (inFlight.putIfAbsent(key, task) != null) {
                inFlightSlots.release();
                throw new IllegalArgumentException("duplicate request id in this session");
            }
            try {
                executor.execute(task);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                inFlight.remove(key, task);
                task.cancel(false);
                throw new IllegalArgumentException("MCP session is closed");
            }
            return task;
        }

        /** Dispatches a stream message, asynchronously tracking ready tool calls. */
        public void dispatch(JsonNode message, Consumer<Optional<ObjectNode>> completion) {
            if (isToolCallReady(message)) {
                try {
                    submit(message, completion);
                } catch (java.util.concurrent.RejectedExecutionException e) {
                    completion.accept(Optional.of(JsonRpc.error(mapper, message.get("id"),
                            -32000, e.getMessage())));
                }
            } else {
                completion.accept(handle(message));
            }
        }

        /** Gives already-received stdio requests a bounded chance to publish their responses. */
        public void awaitInFlight(long timeoutMillis) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            for (FutureTask<Optional<ObjectNode>> task : inFlight.values()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }
                try {
                    task.get(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (TimeoutException e) {
                    return;
                } catch (java.util.concurrent.ExecutionException | CancellationException ignored) {
                    // The completion callback already handled the wire response.
                }
            }
        }

        private void cancelInFlight() {
            inFlight.values().forEach(future -> future.cancel(true));
            inFlight.clear();
        }

        private String idKey(JsonNode id) {
            return id == null ? "null" : id.toString();
        }

        @Override
        public void close() {
            state = State.CLOSED;
            cancelInFlight();
            executor.shutdownNow();
        }
    }

    private Optional<ObjectNode> readResource(JsonNode params) {
        String uri = params.path("uri").asText(null);
        if (uri == null) {
            throw new IllegalArgumentException("resources/read requires params.uri");
        }
        if (resources == null) {
            return Optional.empty();
        }
        return resources.read(mapper, uri).map(contents -> {
            ObjectNode result = mapper.createObjectNode();
            ArrayNode list = result.putArray("contents");
            list.add(contents);
            return result;
        });
    }

    private void write(BufferedWriter writer, ObjectNode response) {
        try {
            writer.write(response.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}

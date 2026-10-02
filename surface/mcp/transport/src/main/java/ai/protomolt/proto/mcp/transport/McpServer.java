package ai.protomolt.proto.mcp.transport;

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
import java.util.Locale;
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
     * supplied. Names only the workspace resource present in every transport assembly;
     * optional capabilities are described by the supplied catalog.
     */
    public static final String DEFAULT_INSTRUCTIONS =
            "Read protomolt://workspace for the installed tool inventory. Use the declared "
                    + "input schemas and inspect operation results before proceeding.";

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
        return handle(message, caller, false);
    }

    /**
     * Dispatches one JSON-RPC message as {@code caller}. When {@code moonshotDialect} is set,
     * tool schemas in {@code initialize} and {@code tools/list} are rendered in the
     * Moonshot-sanitized form.
     */
    private Optional<ObjectNode> handle(JsonNode message, Caller caller, boolean moonshotDialect) {
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
                        initialize(params, caller, moonshotDialect)));
                case "ping" -> Optional.of(JsonRpc.result(mapper, id, mapper.createObjectNode()));
                case "tools/list" -> Optional.of(JsonRpc.result(mapper, id, listTools(caller, moonshotDialect)));
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

    private ObjectNode initialize(JsonNode params, Caller caller, boolean moonshotDialect) {
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
        ArrayNode manifest = catalog.list(caller);
        if (moonshotDialect) {
            manifest = sanitizeForMoonshot(manifest);
        }
        addToolCatalogMetadata(result, manifest);
        if (!instructions.isEmpty()) {
            result.put("instructions", instructions);
        }
        return result;
    }

    private ObjectNode listTools(Caller caller, boolean moonshotDialect) {
        ObjectNode result = mapper.createObjectNode();
        // The catalog manifest entries ({name, description, inputSchema}) are already the
        // MCP tool shape; inputSchema is JSON Schema in both worlds. The manifest is the
        // caller's view: only tools whose scope the caller holds.
        ArrayNode manifest = catalog.list(caller);
        if (moonshotDialect) {
            manifest = sanitizeForMoonshot(manifest);
        }
        result.set("tools", manifest);
        addToolCatalogMetadata(result, manifest);
        return result;
    }

    /**
     * Determines whether the given MCP client info name represents a Moonshot/Kimi client.
     */
    public static boolean isMoonshotClient(String clientName) {
        if (clientName == null || clientName.isBlank()) {
            return false;
        }
        String normalized = clientName.toLowerCase(Locale.ROOT);
        return normalized.contains("kimi") || normalized.contains("moonshot");
    }

    /**
     * Sanitizes a tool manifest for strict function-calling validators like Moonshot Flavored
     * JSON Schema (MFJS). Strips parent {@code type} (and numeric/string bounds) when {@code anyOf}
     * or {@code oneOf} is present, ensuring variant subschemas carry their own {@code type}.
     * The input is not modified; returns a sanitized deep copy, or {@code null} for {@code null}.
     */
    public static ArrayNode sanitizeForMoonshot(ArrayNode manifest) {
        if (manifest == null) {
            return null;
        }
        ArrayNode copy = manifest.deepCopy();
        for (JsonNode tool : copy) {
            if (tool.isObject() && tool.has("inputSchema")) {
                sanitizeSchemaForMoonshot(tool.get("inputSchema"));
            }
        }
        return copy;
    }

    /**
     * Applies the Moonshot schema sanitization in place: at every object carrying an
     * {@code anyOf} or {@code oneOf} union, removes the parent {@code type} (pushing it into
     * variant subschemas that lack one when it is a single textual type) and drops the
     * {@code pattern}, {@code minimum}, {@code maximum}, {@code exclusiveMinimum}, and
     * {@code exclusiveMaximum} keywords the strict validator refuses beside a union.
     */
    public static void sanitizeSchemaForMoonshot(JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            boolean hasAnyOf = obj.has("anyOf");
            boolean hasOneOf = obj.has("oneOf");
            if (hasAnyOf || hasOneOf) {
                JsonNode parentType = obj.remove("type");
                if (parentType != null && parentType.isTextual()) {
                    String typeStr = parentType.asText();
                    Consumer<JsonNode> ensureType = branch -> {
                        if (branch.isObject() && !branch.has("type")) {
                            ((ObjectNode) branch).put("type", typeStr);
                        }
                    };
                    if (hasAnyOf && obj.get("anyOf").isArray()) {
                        obj.get("anyOf").forEach(ensureType);
                    }
                    if (hasOneOf && obj.get("oneOf").isArray()) {
                        obj.get("oneOf").forEach(ensureType);
                    }
                }
                obj.remove("pattern");
                obj.remove("minimum");
                obj.remove("maximum");
                obj.remove("exclusiveMinimum");
                obj.remove("exclusiveMaximum");
            }
            obj.properties().forEach(entry -> sanitizeSchemaForMoonshot(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(McpServer::sanitizeSchemaForMoonshot);
        }
    }

    private void addToolCatalogMetadata(ObjectNode result, ArrayNode manifest) {
        ObjectNode toolCatalog = WorkspaceResources.toolCatalog(manifest, mapper);
        ObjectNode metadata = result.putObject("_meta");
        metadata.put("ai.protomolt/toolCatalogFingerprint",
                toolCatalog.path("fingerprint").asText());
        metadata.put("ai.protomolt/toolCount", toolCatalog.path("count").asInt());
        metadata.put("ai.protomolt/workspace", WorkspaceResources.URI);
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
        private final java.util.concurrent.atomic.AtomicReference<Throwable> asynchronousFailure =
                new java.util.concurrent.atomic.AtomicReference<>();
        private final java.util.concurrent.Semaphore inFlightSlots =
                new java.util.concurrent.Semaphore(MAX_IN_FLIGHT_PER_SESSION);

        private volatile boolean moonshotDialect = false;

        public State state() {
            return state;
        }

        /** Whether this session renders tool schemas in the Moonshot-sanitized dialect. */
        public boolean isMoonshotDialect() {
            return moonshotDialect;
        }

        /** Enables or disables the Moonshot tool-schema dialect for this session. */
        public void setMoonshotDialect(boolean moonshotDialect) {
            this.moonshotDialect = moonshotDialect;
        }

        public String negotiatedProtocolVersion() {
            return negotiatedProtocolVersion;
        }

        public Optional<ObjectNode> handle(JsonNode message) {
            checkAsynchronousFailure();
            if (message == null || !message.isObject()) {
                return Optional.of(JsonRpc.error(mapper, null, JsonRpc.INVALID_REQUEST,
                        "Invalid request"));
            }
            if (!message.has("method")) {
                return McpServer.this.handle(message, caller, moonshotDialect);
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
                JsonNode initParams = message.has("params")
                        ? message.get("params") : mapper.createObjectNode();
                String clientName = initParams.path("clientInfo").path("name").asText("");
                if (isMoonshotClient(clientName)) {
                    setMoonshotDialect(true);
                }
                ObjectNode result;
                try {
                    result = initialize(initParams, caller, moonshotDialect);
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
            return McpServer.this.handle(message, caller, moonshotDialect);
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
            checkAsynchronousFailure();
            if (!isToolCallReady(message)) {
                throw new IllegalArgumentException("tool request is not valid in this session state");
            }
            if (!inFlightSlots.tryAcquire()) {
                throw new java.util.concurrent.RejectedExecutionException(
                        "MCP session in-flight tool limit is exhausted");
            }
            String key = idKey(message.get("id"));
            var taskIdentity = new java.util.concurrent.atomic.AtomicReference<FutureTask<Optional<ObjectNode>>>();
            FutureTask<Optional<ObjectNode>> task = new FutureTask<>(() -> {
                Optional<ObjectNode> response = McpServer.this.handle(message, caller, moonshotDialect);
                if (completion != null && inFlight.get(key) == taskIdentity.get()
                        && !Thread.currentThread().isInterrupted()) {
                    completion.accept(response);
                }
                return response;
            }) {
                @Override
                protected void set(Optional<ObjectNode> response) {
                    // Release before waking get() callers, but never remove a replacement
                    // request that reused the id after this request was cancelled.
                    inFlight.remove(key, this);
                    super.set(response);
                }

                @Override
                protected void setException(Throwable failure) {
                    asynchronousFailure.compareAndSet(null, failure);
                    LOG.error("MCP asynchronous response failed for request {}", key, failure);
                    inFlight.remove(key, this);
                    super.setException(failure);
                }

                @Override
                protected void done() {
                    // Also release cancelled tasks that never started.
                    inFlight.remove(key, this);
                    inFlightSlots.release();
                }
            };
            taskIdentity.set(task);
            if (inFlight.putIfAbsent(key, task) != null) {
                inFlightSlots.release();
                throw new IllegalArgumentException("duplicate request id in this session");
            }
            try {
                executor.execute(task);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                inFlight.remove(key, task);
                task.cancel(false);
                throw new IllegalArgumentException("MCP session is closed", e);
            }
            return task;
        }

        /** Dispatches a stream message, asynchronously tracking ready tool calls. */
        public void dispatch(JsonNode message, Consumer<Optional<ObjectNode>> completion) {
            checkAsynchronousFailure();
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

        /**
         * Waits for received requests to publish their responses. Timeout, interruption and
         * unexpected asynchronous failures throw; explicit request cancellation is normal.
         * Interrupt status is preserved. Failures remain visible after their tasks finish.
         */
        public void awaitInFlight(long timeoutMillis) {
            if (timeoutMillis < 0) {
                throw new IllegalArgumentException("timeoutMillis must not be negative");
            }
            checkAsynchronousFailure();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            for (FutureTask<Optional<ObjectNode>> task : inFlight.values()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("Timed out awaiting MCP responses");
                }
                try {
                    task.get(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted awaiting MCP responses", e);
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Timed out awaiting MCP responses", e);
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new IllegalStateException("MCP asynchronous response failed", e.getCause());
                } catch (CancellationException cancelled) {
                    // Explicit cancellation has no response to publish.
                    checkAsynchronousFailure();
                }
            }
            checkAsynchronousFailure();
        }

        private void checkAsynchronousFailure() {
            Throwable failure = asynchronousFailure.get();
            if (failure != null) {
                throw new IllegalStateException("MCP asynchronous response failed", failure);
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
            checkAsynchronousFailure();
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

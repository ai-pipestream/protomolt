package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.Caller;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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
    private static final int MAX_IN_FLIGHT_PER_SESSION = 64;

    private final ObjectMapper mapper = new ObjectMapper();
    private final McpMethodDispatcher dispatcher;

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
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(serverName, "serverName");
        Objects.requireNonNull(serverVersion, "serverVersion");
        Objects.requireNonNull(instructions, "instructions");
        McpResources composed = CompositeResources.of(
                new WorkspaceResources(catalog, serverName, serverVersion, instructions),
                resources);
        this.dispatcher = new McpMethodDispatcher(
                catalog, composed, serverName, serverVersion, instructions, mapper);
    }

    /**
     * Reads newline-delimited JSON-RPC messages from {@code in} until end of stream, writing
     * one response line per request. Malformed JSON is answered with a parse error rather than
     * terminating the session.
     */
    public void run(InputStream in, OutputStream out) throws IOException {
        StdioLineTransport.run(mapper, openSession(), in, out);
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
        return dispatcher.handle(message, caller, false);
    }

    /**
     * Determines whether the given MCP client info name represents a Moonshot/Kimi client.
     */
    public static boolean isMoonshotClient(String clientName) {
        return MoonshotDialect.isMoonshotClient(clientName);
    }

    /**
     * Sanitizes a tool manifest for strict function-calling validators like Moonshot Flavored
     * JSON Schema (MFJS). Strips parent {@code type} (and numeric/string bounds) when {@code anyOf}
     * or {@code oneOf} is present, ensuring variant subschemas carry their own {@code type}.
     * The input is not modified; returns a sanitized deep copy, or {@code null} for {@code null}.
     */
    public static ArrayNode sanitizeForMoonshot(ArrayNode manifest) {
        return MoonshotDialect.sanitizeForMoonshot(manifest);
    }

    /**
     * Applies the Moonshot schema sanitization in place: at every object carrying an
     * {@code anyOf} or {@code oneOf} union, removes the parent {@code type} (pushing it into
     * variant subschemas that lack one when it is a single textual type) and drops the
     * {@code pattern}, {@code minimum}, {@code maximum}, {@code exclusiveMinimum}, and
     * {@code exclusiveMaximum} keywords the strict validator refuses beside a union.
     */
    public static void sanitizeSchemaForMoonshot(JsonNode node) {
        MoonshotDialect.sanitizeSchemaForMoonshot(node);
    }

    /** Per-connection MCP lifecycle used by stdio and the streamable HTTP adapter. */
    public final class Session implements AutoCloseable {

        public enum State {
            NEW, INITIALIZED, READY, CLOSED
        }

        private final Caller caller;
        private volatile State state = State.NEW;
        private volatile String negotiatedProtocolVersion;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final ConcurrentMap<String, FutureTask<Optional<ObjectNode>>> inFlight =
                new ConcurrentHashMap<>();
        private final AtomicReference<Throwable> asynchronousFailure =
                new AtomicReference<>();
        private final Semaphore inFlightSlots =
                new Semaphore(MAX_IN_FLIGHT_PER_SESSION);
        private volatile boolean moonshotDialect = false;

        private Session(Caller caller) {
            this.caller = caller;
        }

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
                return dispatcher.handle(message, caller, moonshotDialect);
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
                    result = dispatcher.initialize(initParams, caller, moonshotDialect);
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
            return dispatcher.handle(message, caller, moonshotDialect);
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
                throw new RejectedExecutionException(
                        "MCP session in-flight tool limit is exhausted");
            }
            String key = idKey(message.get("id"));
            var taskIdentity = new AtomicReference<FutureTask<Optional<ObjectNode>>>();
            FutureTask<Optional<ObjectNode>> task = new FutureTask<>(() -> {
                Optional<ObjectNode> response = dispatcher.handle(message, caller, moonshotDialect);
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
            } catch (RejectedExecutionException e) {
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
                } catch (RejectedExecutionException e) {
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
                } catch (ExecutionException e) {
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
}

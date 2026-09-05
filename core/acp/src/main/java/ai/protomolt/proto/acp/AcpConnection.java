package ai.protomolt.proto.acp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Newline-delimited JSON-RPC 2.0 over a pair of byte streams, the framing ACP and MSP use on stdio:
 * one JSON object per line, UTF-8, no Content-Length headers. One virtual thread runs the read
 * loop; every write passes through a single lock so a notification emitted by a request handler
 * can never interleave with another message on the wire. Incoming requests are each dispatched
 * to a fresh virtual thread, so a long-running request (a prompt turn) never blocks the loop
 * that reads the next message; notifications run inline on that loop so wire order is delivery
 * order. Responses are correlated to requests by id with a {@link CompletableFuture}.
 */
public final class AcpConnection implements AutoCloseable {

    public static final int PARSE_ERROR = -32700;
    public static final int METHOD_NOT_FOUND = -32601;
    public static final int INTERNAL_ERROR = -32603;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = Logger.getLogger(AcpConnection.class.getName());

    /** Handles an incoming request; the return value becomes the response's {@code result}. */
    public interface RequestHandler {
        JsonNode handle(String method, JsonNode params) throws Exception;
    }

    /** Handles an incoming notification; failures are logged, never answered. */
    public interface NotificationHandler {
        void handle(String method, JsonNode params);
    }

    private final InputStream rawIn;
    private final BufferedReader in;
    private final OutputStream out;
    private final Object writeLock = new Object();
    private final Map<String, CompletableFuture<JsonNode>> pending = new ConcurrentHashMap<>();
    private final ExecutorService handlers = Executors.newVirtualThreadPerTaskExecutor();
    private final CountDownLatch eof = new CountDownLatch(1);
    private final AtomicLong ids = new AtomicLong();

    private volatile RequestHandler requestHandler = (method, params) -> {
        throw new AcpError(METHOD_NOT_FOUND, "unknown method: " + method);
    };
    private volatile NotificationHandler notificationHandler = (method, params) -> {
    };

    private AcpConnection(InputStream in, OutputStream out) {
        this.rawIn = in;
        this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        this.out = out;
    }

    public static AcpConnection over(InputStream in, OutputStream out) {
        return new AcpConnection(in, out);
    }

    /** Starts the read loop on a virtual thread. */
    public AcpConnection start() {
        Thread.ofVirtual().name("acp-reader").start(this::readLoop);
        return this;
    }

    public void onRequest(RequestHandler handler) {
        this.requestHandler = handler;
    }

    public void onNotification(NotificationHandler handler) {
        this.notificationHandler = handler;
    }

    /** Sends a request and returns a future completed by its response's {@code result}. */
    public CompletableFuture<JsonNode> request(String method, JsonNode params) {
        String id = "req-" + ids.incrementAndGet();
        ObjectNode message = MAPPER.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", id);
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        CompletableFuture<JsonNode> future = new CompletableFuture<>();
        pending.put(key(TextNode.valueOf(id)), future);
        write(message);
        return future;
    }

    /** Sends a notification; fire-and-forget, no response is expected or read. */
    public void notify(String method, JsonNode params) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        write(message);
    }

    /** Blocks until the peer closes the stream. */
    public void awaitEnd() throws InterruptedException {
        eof.await();
    }

    private void readLoop() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                if (!line.isBlank()) {
                    dispatch(line);
                }
            }
        } catch (IOException e) {
            // The stream was closed, by the peer or by close(); either way the loop ends.
        } finally {
            failAll(new EOFException("the peer closed the stream"));
            eof.countDown();
        }
    }

    private void dispatch(String line) {
        JsonNode message;
        try {
            message = MAPPER.readTree(line);
        } catch (Exception e) {
            write(errorMessage(null, PARSE_ERROR, "not JSON: " + e.getMessage()));
            return;
        }
        if (!message.isObject()) {
            write(errorMessage(null, PARSE_ERROR, "a JSON-RPC message must be an object"));
            return;
        }
        JsonNode id = message.get("id");
        JsonNode method = message.get("method");
        if (method != null && method.isTextual()) {
            if (id != null) {
                // Requests (a prompt turn can run for minutes) get their own virtual thread
                // so the read loop is never blocked by a handler.
                handlers.execute(() -> answer(id, method.asText(), message.get("params")));
            } else {
                // Notifications are handled inline: the peer's messages arrive in wire order,
                // so every update preceding a response is delivered before that response
                // completes its future. Listeners must be fast; ours append to a buffer.
                try {
                    notificationHandler.handle(method.asText(), message.get("params"));
                } catch (Exception e) {
                    // Notifications are never answered; a failing listener must not kill
                    // the connection, so the failure is logged and dropped.
                    LOG.log(Level.WARNING, e,
                            () -> "acp: notification handler failed for " + method.asText());
                }
            }
        } else if (id != null && (message.has("result") || message.has("error"))) {
            CompletableFuture<JsonNode> future = pending.remove(key(id));
            if (future != null) {
                JsonNode error = message.get("error");
                if (error != null && error.isObject()) {
                    future.completeExceptionally(new AcpError(
                            error.path("code").asInt(INTERNAL_ERROR),
                            error.path("message").asText("unknown error")));
                } else {
                    future.complete(message.get("result"));
                }
            }
        }
        // Anything else (a response with an id we never sent, a stray result) is ignored.
    }

    private void answer(JsonNode id, String method, JsonNode params) {
        try {
            JsonNode result = requestHandler.handle(method, params);
            ObjectNode response = MAPPER.createObjectNode();
            response.put("jsonrpc", "2.0");
            response.set("id", id);
            response.set("result", result == null ? NullNode.getInstance() : result);
            write(response);
        } catch (AcpError e) {
            write(errorMessage(id, e.code(), e.getMessage()));
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.toString();
            write(errorMessage(id, INTERNAL_ERROR, message));
        }
    }

    private ObjectNode errorMessage(JsonNode id, int code, String message) {
        ObjectNode response = MAPPER.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", id == null ? NullNode.getInstance() : id);
        ObjectNode error = response.putObject("error");
        error.put("code", code);
        error.put("message", message);
        return response;
    }

    private void write(JsonNode message) {
        final byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(message);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        synchronized (writeLock) {
            try {
                out.write(bytes);
                out.write('\n');
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Ids are echoed verbatim by the peer, so the JSON token text is a stable map key. */
    private static String key(JsonNode id) {
        return id.toString();
    }

    private void failAll(Throwable cause) {
        pending.forEach((id, future) -> future.completeExceptionally(cause));
        pending.clear();
    }

    @Override
    public void close() {
        // Close the raw stream, not the BufferedReader: BufferedReader.close() takes the
        // reader lock, which a read in progress holds, so closing it from another thread
        // deadlocks against the read loop it is meant to stop. Closing the underlying
        // stream wakes that read with EOF instead.
        try {
            rawIn.close();
        } catch (IOException ignored) {
        }
        synchronized (writeLock) {
            try {
                out.close();
            } catch (IOException ignored) {
            }
        }
        handlers.close();
        failAll(new EOFException("connection closed"));
        try {
            eof.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

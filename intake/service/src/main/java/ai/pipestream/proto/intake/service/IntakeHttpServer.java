package ai.pipestream.proto.intake.service;

import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HTTP lane of the intake service: {@code POST /v1/intake:upload}, raw
 * binary body. It mirrors repo-service's raw-upload route
 * ({@code UploadHttpServer}) with {@code x-api-key} replacing the raw account
 * headers — the account is never a request parameter here, it rides the API
 * key's {@link IntakeScope}. Unlike repo's route, this lane never touches
 * object storage: the payload is wrapped into a repo Document with an inline
 * blob and saved through the {@code DocumentService} blocking stub over the
 * wire, exactly like the gRPC lanes, via the shared {@link RawIngests} core.
 *
 * <p>Plain JDK {@link HttpServer} on a virtual-thread executor, blocking
 * style end to end — a parked virtual thread per in-flight upload.
 *
 * <p><b>Auth.</b> The credential rides the {@code x-api-key} header, or the
 * standard {@code authorization} header with an optional case-insensitive
 * {@code Bearer} prefix. A missing or blank credential and an unknown key are
 * both 401; a failing key store is 500 ("API key store failure"), never a 401
 * that poses as a bad key.
 *
 * <p><b>Targeting.</b> Every value is accepted as a query parameter or as a
 * header, the query parameter winning when both are present (repo's upload
 * convention):
 * <ul>
 *   <li>{@code datasource_id} / {@code x-datasource-id} — required (400 names
 *   it when absent);</li>
 *   <li>{@code drive} / {@code x-drive-name} — optional, default
 *   {@code "intake"};</li>
 *   <li>{@code filename} / {@code x-filename} — optional;</li>
 *   <li>{@code content_type} query parameter, else the {@code Content-Type}
 *   header — optional (a content-type-restricted key demands one).</li>
 * </ul>
 * An {@code account_id} parameter (or {@code x-account-id} header) is a 400:
 * account identity rides the API key, period.
 *
 * <p><b>Errors.</b> Scope violations are 403 with the same messages as the
 * gRPC lanes' PERMISSION_DENIED; cap breaches are 413, checked against the
 * declared Content-Length BEFORE the body is buffered and again while
 * reading; a missing Content-Length is 411 (mirroring repo's route); non-POST
 * is 405. A failed repo save maps NOT_FOUND to 404, FAILED_PRECONDITION to
 * 409, INVALID_ARGUMENT to 400, and anything else to 502. Every error body is
 * JSON: {@code {"error": "..."}}.
 *
 * <p><b>Receipt.</b> 200 with the {@code IngestDocumentResponse} receipt
 * rendered as JSON (proto field names, no-presence fields included) — the
 * same vocabulary every lane answers: {@code doc_id}, {@code node_id},
 * {@code address}, {@code drive}, {@code size_bytes}, {@code sha256},
 * {@code deduplicated}.
 */
public final class IntakeHttpServer implements AutoCloseable {

    /** The single route this server serves. */
    public static final String UPLOAD_PATH = "/v1/intake:upload";

    private static final Logger LOG = LoggerFactory.getLogger(IntakeHttpServer.class);

    /** Proto field names, no-presence fields always printed: the receipt vocabulary. */
    private static final JsonFormat.Printer JSON =
            JsonFormat.printer().preservingProtoFieldNames().alwaysPrintFieldsWithNoPresence();

    private final RawIngests ingests;
    private final ApiKeyIdentityResolver resolver;

    private HttpServer server;
    private ExecutorService executor;

    IntakeHttpServer(RawIngests ingests, ApiKeyIdentityResolver resolver) {
        if (ingests == null) {
            throw new IllegalArgumentException("ingests must not be null");
        }
        if (resolver == null) {
            throw new IllegalArgumentException("resolver must not be null");
        }
        this.ingests = ingests;
        this.resolver = resolver;
    }

    /**
     * Binds and starts the server.
     *
     * @param port the listen port (0 = ephemeral, read it back via {@link #port()})
     * @return the bound port
     */
    public synchronized int start(int port) {
        if (server != null) {
            throw new IllegalStateException("HTTP intake server already started");
        }
        HttpServer created;
        try {
            created = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind HTTP intake server on port " + port, e);
        }
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            created.createContext(UPLOAD_PATH, this::handle);
            created.setExecutor(pool);
            created.start();
        } catch (RuntimeException e) {
            created.stop(0);
            pool.shutdownNow();
            throw e;
        }
        this.server = created;
        this.executor = pool;
        int bound = created.getAddress().getPort();
        LOG.info("HTTP intake route listening on port {} ({})", bound, UPLOAD_PATH);
        return bound;
    }

    /**
     * The bound port.
     *
     * @return the port the server is listening on
     */
    public synchronized int port() {
        if (server == null) {
            throw new IllegalStateException("HTTP intake server not started");
        }
        return server.getAddress().getPort();
    }

    @Override
    public synchronized void close() {
        if (server != null) {
            server.stop(1);
            server = null;
        }
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    // ------------------------------------------------------------------ route

    private void handle(HttpExchange exchange) throws IOException {
        try {
            // createContext prefix-matches; only the exact path is the route.
            if (!UPLOAD_PATH.equals(exchange.getRequestURI().getPath())) {
                throw new HttpError(404, "unknown route " + exchange.getRequestURI().getPath());
            }
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new HttpError(405, "method not allowed: " + exchange.getRequestMethod());
            }
            writeJson(exchange, 200, upload(exchange));
        } catch (HttpError e) {
            writeError(exchange, e.status, e.getMessage());
        } catch (StatusRuntimeException e) {
            // The shared intake core (and the repo save behind it) speaks the
            // gRPC status vocabulary; flatten it onto HTTP.
            writeError(exchange, httpStatus(e.getStatus().getCode()), e.getStatus().getDescription());
        } catch (RuntimeException | IOException e) {
            LOG.warn("intake upload failed: {}", e.getMessage());
            writeError(exchange, 502, "intake upload failed: " + e.getMessage());
        } finally {
            exchange.close();
        }
    }

    private IngestDocumentResponse upload(HttpExchange exchange) throws IOException {
        IntakeScope scope = authenticate(exchange);
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        requireNoAccountParam(exchange, query);
        String datasourceId = identity(exchange, query, "datasource_id", "x-datasource-id");
        if (datasourceId == null) {
            throw new HttpError(400, "datasource_id is required (query parameter or header)");
        }
        String drive = identity(exchange, query, "drive", "x-drive-name");
        String filename = identity(exchange, query, "filename", "x-filename");
        String contentType = query.get("content_type");
        if (contentType == null || contentType.isBlank()) {
            contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        }

        // The same narrowing the gRPC lanes run, before a single body byte.
        RawIngests.Targeting targeting = RawIngests.Targeting.of(scope, datasourceId, drive);
        RawIngests.requireDeclaredMimeTypeWithinScope(scope, contentType);

        // Mirrors repo's upload route: chunked/absent-length bodies are 411
        // before any byte is read, and a declared length over the caps is 413
        // before any byte is buffered.
        long contentLength = contentLength(exchange);
        ingests.requirePayloadWithinCaps(scope, contentLength);
        byte[] payload = readBody(exchange, scope, contentLength);

        RawIngests.Saved saved =
                ingests.saveRawPayload(
                        scope,
                        targeting,
                        ByteString.copyFrom(payload),
                        filename == null ? "" : filename,
                        contentType == null || contentType.isBlank() ? "" : contentType.trim(),
                        Map.of());
        LOG.debug("HTTP intake saved doc_id={} ({} bytes, sha256={}, deduplicated={})",
                saved.response().getAddress().getDocId(), saved.payloadSize(), saved.sha256(),
                saved.response().getDeduplicated());
        return IngestDocumentResponse.newBuilder()
                .setDocId(saved.response().getAddress().getDocId())
                .setNodeId(saved.response().getNodeId())
                .setAddress(saved.response().getAddress())
                .setDrive(saved.response().getDrive())
                .setSizeBytes(saved.payloadSize())
                .setSha256(saved.sha256())
                .setDeduplicated(saved.response().getDeduplicated())
                .build();
    }

    // ------------------------------------------------------------------ auth

    private IntakeScope authenticate(HttpExchange exchange) {
        String credential = credentialFrom(exchange.getRequestHeaders());
        if (credential == null) {
            throw new HttpError(401, "missing API key: send an x-api-key or authorization header");
        }
        Optional<IntakeScope> scope;
        try {
            scope = resolver.resolve(credential);
        } catch (RuntimeException e) {
            // The store failed; never misreport that as a bad key.
            LOG.error("API key store failure", e);
            throw new HttpError(500, "API key store failure");
        }
        return scope.orElseThrow(() -> new HttpError(401, "unknown API key"));
    }

    private static String credentialFrom(Headers headers) {
        String apiKey = headers.getFirst("x-api-key");
        if (apiKey != null && !apiKey.isBlank()) {
            return apiKey.trim();
        }
        String authorization = headers.getFirst("authorization");
        if (authorization == null || authorization.isBlank()) {
            return null;
        }
        String value = authorization.trim();
        if (value.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String stripped = value.substring(7).trim();
            return stripped.isEmpty() ? null : stripped;
        }
        return value;
    }

    // ------------------------------------------------------------------ plumbing

    /** A failure carrying its HTTP status; thrown at the decision point. */
    private static final class HttpError extends RuntimeException {
        private final int status;

        private HttpError(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    /** The gRPC status vocabulary of the intake core, flattened onto HTTP. */
    private static int httpStatus(Status.Code code) {
        return switch (code) {
            case UNAUTHENTICATED -> 401;
            case PERMISSION_DENIED -> 403;
            case INVALID_ARGUMENT -> 400;
            case NOT_FOUND -> 404;
            case FAILED_PRECONDITION -> 409;
            case RESOURCE_EXHAUSTED -> 413;
            default -> 502;
        };
    }

    private static void requireNoAccountParam(HttpExchange exchange, Map<String, String> query) {
        if (query.containsKey("account_id")
                || exchange.getRequestHeaders().containsKey("x-account-id")) {
            throw new HttpError(400,
                    "account_id is not accepted on the intake lane: account identity rides the API key");
        }
    }

    private static long contentLength(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Content-Length");
        if (header == null || header.isBlank()) {
            throw new HttpError(411, "Content-Length header is required"
                    + " (chunked uploads are not accepted on the intake lane)");
        }
        try {
            long length = Long.parseLong(header.trim());
            if (length < 0) {
                throw new NumberFormatException("negative");
            }
            return length;
        } catch (NumberFormatException e) {
            throw new HttpError(411, "Content-Length must be a non-negative byte count (got \""
                    + header + "\")");
        }
    }

    /**
     * Buffers the body, enforcing the caps on the running total while reading
     * — the declared Content-Length was checked before the first byte, this
     * catches a body that lies about it.
     */
    private byte[] readBody(HttpExchange exchange, IntakeScope scope, long declared)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        long total = 0;
        try (InputStream in = exchange.getRequestBody()) {
            int read;
            while ((read = in.read(chunk)) != -1) {
                total += read;
                ingests.requirePayloadWithinCaps(scope, total);
                if (total > declared) {
                    throw new HttpError(400, "request body exceeds the declared Content-Length of "
                            + declared + " bytes");
                }
                buffer.write(chunk, 0, read);
            }
        }
        if (total != declared) {
            throw new HttpError(400, "request body ended after " + total
                    + " bytes but Content-Length declared " + declared);
        }
        return buffer.toByteArray();
    }

    /** Query parameter wins over the header; either may be absent. */
    private static String identity(HttpExchange exchange, Map<String, String> query,
            String queryName, String headerName) {
        String value = query.get(queryName);
        if (value == null || value.isBlank()) {
            value = exchange.getRequestHeaders().getFirst(headerName);
        }
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> out = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return out;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            out.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return out;
    }

    private void writeJson(HttpExchange exchange, int status, MessageOrBuilder body)
            throws IOException {
        byte[] bytes;
        try {
            bytes = JSON.print(body).getBytes(StandardCharsets.UTF_8);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("failed to render the response body", e);
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private void writeError(HttpExchange exchange, int status, String message) throws IOException {
        String error = message == null || message.isBlank() ? "intake upload failed" : message;
        writeJson(exchange, status,
                Struct.newBuilder()
                        .putFields("error", Value.newBuilder().setStringValue(error).build()));
    }
}

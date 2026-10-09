package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.engine.ArchiveOperations;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * HTTP parsing and receipts for shared raw ingestion and archive operations.
 * Document uploads require a qualified RawIngestionRepository. Legacy constructors
 * remain source compatible but refuse document uploads until that port is supplied.
 * Content-Length is required. Physical keys are immutable per-attempt keys; the
 * receipt returns the committed reference, including on deduplicated uploads.
 */
public final class UploadHttpServer implements AutoCloseable {

    /** The claim-check store's upload route. */
    public static final String UPLOAD_PATH = "/v1/documents:upload";

    /** The archive's upload route (served when the archive surface is wired). */
    public static final String ARCHIVE_UPLOAD_PATH = "/v1/archive:upload";

    private static final Logger LOG = LoggerFactory.getLogger(UploadHttpServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ai.protomolt.proto.repo.spi.RawIngestionRepository ingestion;
    private final byte[] expectedToken;
    private final ArchiveRepository archiveOperations;

    private HttpServer server;
    private ExecutorService executor;

    /** Compatibility constructor: document uploads return 503 until a qualified ingestion port is supplied. */
    public UploadHttpServer(DocumentGrpcService documentService, DriveLedger drives,
            BlobStore blobStore, String apiToken) {
        this(documentService, drives, blobStore, apiToken, null);
    }

    /** Compatibility constructor: document uploads return 503 until a qualified ingestion port is supplied. */
    public UploadHttpServer(DocumentGrpcService documentService, DriveLedger drives,
            BlobStore blobStore, String apiToken, ArchiveRepository archiveOperations) {
        this(documentService.repository(), drives, blobStore, apiToken, archiveOperations);
    }

    /** Compatibility factory; document uploads require the RawIngestionRepository constructor. */
    public static UploadHttpServer forRepository(ai.protomolt.proto.repo.spi.DocumentRepository documents,
            DriveLedger drives, BlobStore blobStore, String apiToken) {
        return new UploadHttpServer(documents, drives, blobStore, apiToken, null);
    }

    private UploadHttpServer(ai.protomolt.proto.repo.spi.DocumentRepository documents,
            DriveLedger drives, BlobStore blobStore, String apiToken, ArchiveRepository archiveOperations) {
        this((ai.protomolt.proto.repo.spi.RawIngestionRepository) null, apiToken, archiveOperations);
    }

    /** Hosts only transport parsing; ingestion owns all byte and document publication behavior. */
    public UploadHttpServer(ai.protomolt.proto.repo.spi.RawIngestionRepository ingestion,
            String apiToken, ArchiveRepository archiveOperations) {
        this.ingestion = ingestion;
        this.expectedToken = RepositoryNetworkAuthentication.requireOperatorToken(apiToken).getBytes(StandardCharsets.UTF_8);
        this.archiveOperations = archiveOperations;
    }

    /**
     * Refuses a request that does not present the configured credential. Compared in
     * constant time, so a wrong credential costs the same as a right one and the
     * comparison leaks nothing about how much of it matched. The refusal names the
     * missing header and never echoes what was presented.
     */
    private void requireCredential(HttpExchange exchange) throws HttpError {
        String presented = exchange.getRequestHeaders().getFirst("api_token");
        if (presented == null) {
            String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization != null
                    && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        if (presented == null || presented.isBlank()
                || !MessageDigest.isEqual(presented.getBytes(StandardCharsets.UTF_8),
                        expectedToken)) {
            throw new HttpError(401, "missing or invalid credential 'api_token'");
        }
    }

    /**
     * Binds and starts the server.
     *
     * @param port the listen port (0 = ephemeral, read it back via {@link #port()})
     * @return the bound port
     */
    public synchronized int start(int port) {
        if (server != null || executor != null) {
            throw new IllegalStateException("HTTP upload server already started");
        }
        HttpServer created;
        try {
            created = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind HTTP upload server on port " + port, e);
        }
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        this.server = created;
        this.executor = pool;
        try {
            created.createContext(UPLOAD_PATH, this::handle);
            if (archiveOperations != null) {
                created.createContext(ARCHIVE_UPLOAD_PATH, this::handleArchive);
            }
            created.setExecutor(pool);
            created.start();
        } catch (RuntimeException | Error e) {
            try { close(); }
            catch (RuntimeException | Error cleanup) { e.addSuppressed(cleanup); }
            throw e;
        }
        int bound = created.getAddress().getPort();
        LOG.info("HTTP upload route listening on port {} ({})", bound, UPLOAD_PATH);
        return bound;
    }

    /**
     * The bound port.
     *
     * @return the port the server is listening on
     */
    public synchronized int port() {
        if (server == null) {
            throw new IllegalStateException("HTTP upload server not started");
        }
        return server.getAddress().getPort();
    }

    @Override
    public synchronized void close() {
        close(java.time.Duration.ofSeconds(10));
    }

    synchronized void close(java.time.Duration timeout) {
        if (server != null) {
            server.stop(1);
            server = null;
        }
        if (executor != null) {
            try { ExecutorShutdown.stop(executor, timeout); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("HTTP shutdown interrupted; resources retained", interrupted);
            }
            executor = null;
        }
    }

    // ------------------------------------------------------------------ route

    private void handle(HttpExchange exchange) throws IOException {
        try {
            // Ahead of route and method matching, so an unauthenticated probe learns
            // nothing about which routes exist or which methods they accept.
            requireCredential(exchange);
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
        } catch (ai.protomolt.proto.repo.spi.RepositoryException e) {
            int status = switch (e.code()) {
                case INVALID_ARGUMENT -> 400;
                case NOT_FOUND -> 404;
                case PERMISSION_DENIED -> 403;
                case FAILED_PRECONDITION, CONFLICT -> 409;
                case UNSUPPORTED -> 503;
                default -> 502;
            };
            writeError(exchange, status, e.getMessage());
        } catch (StatusRuntimeException e) {

            // The intake save's gRPC status vocabulary, flattened onto HTTP.
            Status.Code code = e.getStatus().getCode();
            int status = switch (code) {
                case INVALID_ARGUMENT -> 400;
                case NOT_FOUND -> 404;
                default -> 502;
            };
            writeError(exchange, status, e.getStatus().getDescription());
        } catch (RuntimeException | IOException e) {
            LOG.warn("upload failed: {}", e.getMessage());
            writeError(exchange, 502, "backing store failure: " + e.getMessage());
        } finally {
            exchange.close();
        }
    }

    private ObjectNode upload(HttpExchange exchange) throws IOException {
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String accountId = required(identity(exchange, query, "account_id", "x-account-id"), "account_id");
        String datasourceId = required(identity(exchange, query, "datasource_id", "x-datasource-id"),
                "datasource_id");
        String driveName = required(identity(exchange, query, "drive", "x-drive-name"), "drive");
        String filename = required(identity(exchange, query, "filename", "x-filename"), "filename");
        String connectorId = identity(exchange, query, "connector_id", "x-connector-id");
        String crawlId = identity(exchange, query, "crawl_id", "x-crawl-id");
        String docId = identity(exchange, query, "doc_id", "x-doc-id");
        String contentType = first(query.get("content_type"),
                exchange.getRequestHeaders().getFirst("Content-Type"), "application/octet-stream");

        // Deliberate contract (see class Javadoc): the S3 sync client streams
        // only with a known length, so chunked/absent-length bodies are 411
        // before any byte is read.
        long contentLength = contentLength(exchange);
        if (ingestion == null) throw new HttpError(503, "Managed document ingestion is not configured");

        var request = new ai.protomolt.proto.repo.spi.RawIngestionRepository.Request(accountId, datasourceId,
                driveName, docId, filename, contentType, connectorId, crawlId,
                exchange.getRequestHeaders().getFirst("X-Content-Sha256"));
        ai.protomolt.proto.repo.spi.RawIngestionRepository.Result result;
        try (InputStream body = exchange.getRequestBody()) {
            result = ingestion.upload(new ai.protomolt.proto.repo.spi.RepositoryCaller("http-upload", true),
                    request, body, contentLength);
        }
        var saved = result.document();
        var ref = result.storageRef();
        ObjectNode storageRef = MAPPER.createObjectNode()
                .put("drive_name", ref.getDriveName()).put("object_key", ref.getObjectKey());
        if (ref.hasVersionId()) storageRef.put("version_id", ref.getVersionId());
        ObjectNode response = MAPPER.createObjectNode()
                .put("node_id", saved.getNodeId())
                .put("doc_id", saved.getAddress().getDocId())
                .put("deduplicated", saved.getDeduplicated())
                .put("size_bytes", result.sizeBytes())
                .put("sha256", result.sha256());
        response.set("storage_ref", storageRef);
        return response;
    }

    // ------------------------------------------------------------------ archive route

    private void handleArchive(HttpExchange exchange) throws IOException {
        try {
            requireCredential(exchange);
            if (!ARCHIVE_UPLOAD_PATH.equals(exchange.getRequestURI().getPath())) {
                throw new HttpError(404, "unknown route " + exchange.getRequestURI().getPath());
            }
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                throw new HttpError(405, "method not allowed: " + exchange.getRequestMethod());
            }
            writeJson(exchange, 200, archiveUpload(exchange));
        } catch (HttpError e) {
            writeError(exchange, e.status, e.getMessage());
        } catch (ai.protomolt.proto.repo.spi.RepositoryException e) {
            int status = switch (e.code()) {
                case INVALID_ARGUMENT -> 400;
                case NOT_FOUND -> 404;
                case PERMISSION_DENIED -> 403;
                case FAILED_PRECONDITION, CONFLICT -> 409;
                default -> 502;
            };
            writeError(exchange, status, e.getMessage());
        } catch (StatusRuntimeException e) {
            // The archive flows' gRPC status vocabulary, flattened onto HTTP.
            int status = switch (e.getStatus().getCode()) {
                case INVALID_ARGUMENT -> 400;
                case NOT_FOUND -> 404;
                case FAILED_PRECONDITION -> 409;
                case ABORTED -> 409;
                default -> 502;
            };
            writeError(exchange, status, e.getStatus().getDescription());
        } catch (RuntimeException | IOException e) {
            LOG.warn("archive upload failed: {}", e.getMessage());
            writeError(exchange, 502, "backing store failure: " + e.getMessage());
        } finally {
            exchange.close();
        }
    }

    /**
     * The archive's zero-dependency door: the body is one rendition's bytes,
     * identity rides query parameters or headers (query wins) — account_id,
     * archive, entry_id, rendition (default {@code original}), filename,
     * Content-Type, optional X-Content-Sha256 — streamed through the same
     * verified, content-addressed landing as the gRPC streaming door, one
     * new entry version per upload.
     */
    private ObjectNode archiveUpload(HttpExchange exchange) throws IOException {
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String accountId = required(identity(exchange, query, "account_id", "x-account-id"),
                "account_id");
        String archive = required(identity(exchange, query, "archive", "x-archive"), "archive");
        String entryId = required(identity(exchange, query, "entry_id", "x-entry-id"),
                "entry_id");
        String rendition = identity(exchange, query, "rendition", "x-rendition");
        String filename = identity(exchange, query, "filename", "x-filename");
        String contentType = first(query.get("content_type"),
                exchange.getRequestHeaders().getFirst("Content-Type"));
        String declaredSha = exchange.getRequestHeaders().getFirst("X-Content-Sha256");
        long contentLength = contentLength(exchange);
        if (contentLength <= 0) {
            throw new HttpError(411, "Content-Length must be positive for an archive upload");
        }

        ai.protomolt.proto.repo.archive.v1.EntryAddress address =
                ai.protomolt.proto.repo.archive.v1.EntryAddress.newBuilder()
                        .setAccountId(accountId)
                        .setArchive(archive)
                        .setEntryId(entryId)
                        .build();
        ai.protomolt.proto.repo.archive.v1.RenditionDescriptor descriptor =
                ai.protomolt.proto.repo.archive.v1.RenditionDescriptor.newBuilder()
                        .setName(rendition == null ? "original" : rendition)
                        .setMediaType(contentType)
                        .build();
        ArchiveRepository.UploadResult result = archiveOperations.uploadStream(new RepositoryCaller("http-upload", true), address,
                descriptor, contentLength,
                declaredSha == null ? "" : declaredSha,
                null, filename, null, null, exchange.getRequestBody());
        return MAPPER.createObjectNode()
                .put("entry_uuid", result.entryUuid())
                .put("version", result.version())
                .put("rendition", descriptor.getName())
                .put("size_bytes", result.sizeBytes())
                .put("sha256", result.sha256())
                .put("object_key", result.objectKey())
                .put("root_checksum", result.rootChecksum())
                .put("deduplicated", result.deduplicated());
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

    private static long contentLength(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Content-Length");
        if (header == null || header.isBlank()) {
            throw new HttpError(411, "Content-Length header is required"
                    + " (chunked uploads are not accepted: the store streams with a known length)");
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

    /** Query parameter wins over the header; either may be absent. */
    private static String identity(HttpExchange exchange, Map<String, String> query,
            String queryName, String headerName) {
        String value = query.get(queryName);
        if (value == null || value.isBlank()) {
            value = exchange.getRequestHeaders().getFirst(headerName);
        }
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static String required(String value, String name) {
        if (value == null) {
            throw new HttpError(400, name + " is required (query parameter or header)");
        }
        return value;
    }

    private static String first(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate.trim();
            }
        }
        return "application/octet-stream";
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

    private void writeJson(HttpExchange exchange, int status, ObjectNode body) throws IOException {
        byte[] bytes = MAPPER.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private void writeError(HttpExchange exchange, int status, String message) throws IOException {
        writeJson(exchange, status,
                MAPPER.createObjectNode().put("error", message == null ? "upload failed" : message));
    }
}

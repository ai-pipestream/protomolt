package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.blob.DocumentIds;
import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.ChecksumType;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
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
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The streaming raw-upload route of the claim-check store:
 * {@code POST /v1/documents:upload}. The request body streams through a
 * SHA-256 {@link DigestInputStream} straight into
 * {@link BlobStore#put(BlobStore.PutSpec, InputStream, long)} — at no point
 * is the payload buffered in memory, which is why this route (and not the
 * unary {@code PutBlob} RPC) is where bulk bytes belong.
 *
 * <p>Plain JDK {@link HttpServer} on a virtual-thread executor, deliberately
 * NOT protomolt-server-jdk: that host is a protobuf-JSON REST gateway bound
 * to {@code ProtoRestGateway}'s invoke(body, headers, query) shape, and a raw
 * octet-stream body has no place in it. Blocking style end to end — a parked
 * virtual thread per in-flight upload, no reactive bridging.
 *
 * <p><b>Identity contract.</b> Every identity value is accepted as a query
 * parameter or as a header (the query parameter wins when both are present):
 * <ul>
 *   <li>{@code account_id} / {@code x-account-id} — required;</li>
 *   <li>{@code datasource_id} / {@code x-datasource-id} — required (it is the
 *   intake storage address);</li>
 *   <li>{@code drive} / {@code x-drive-name} — required;</li>
 *   <li>{@code filename} / {@code x-filename} — required;</li>
 *   <li>{@code content_type} query param, else the {@code Content-Type}
 *   header, else {@code application/octet-stream};</li>
 *   <li>{@code connector_id} / {@code x-connector-id} — optional;</li>
 *   <li>{@code crawl_id} / {@code x-crawl-id} — optional;</li>
 *   <li>{@code doc_id} / {@code x-doc-id} — optional; blank derives a
 *   name-based UUID from the content SHA-256 (see below).</li>
 * </ul>
 *
 * <p><b>Content-Length is REQUIRED</b> — a deliberate contract, not an
 * oversight: the S3 sync client behind {@link BlobStore} needs a known length
 * to stream a PUT, so chunked/absent-length requests are rejected with
 * {@code 411 Length Required} before a single byte is read.
 *
 * <p><b>Landing and dedupe.</b> The body lands at
 * {@code <drive.prefix>/blobs/<accountId>/<blobId>.bin} where
 * {@code blobId = DocumentIds.blobId(docId, datasourceId, accountId)} — a
 * deterministic key, so a re-upload of the same logical document overwrites
 * in place instead of orphaning a randomly-keyed object. The assembled
 * Document (ownership + blob_bag with a {@code FileStorageReference} and the
 * computed SHA-256) then runs through the SAME intake-save path as gRPC
 * {@code SaveDocument} ({@link DocumentGrpcService#saveBlocking},
 * use_datasource_id arm, graph {@code "intake:<accountId>"}), whose
 * root-checksum dedupe answers an identical re-upload with
 * {@code deduplicated=true} and skips the part re-write.
 *
 * <p><b>Blank doc_id costs one server-side copy.</b> The deterministic blob
 * key depends on the doc id, but a derived doc id needs the content hash,
 * which only exists once the stream has landed. So a blank-doc_id upload
 * streams to a staging key, derives {@code doc_id} from the landed SHA-256,
 * server-side copies to the final deterministic key, and deletes the staging
 * object. An explicit doc_id never pays this.
 *
 * <p><b>Client checksum.</b> An {@code X-Content-Sha256} header is verified
 * against the digest computed while streaming: a mismatch is a 400 and the
 * landed object is best-effort deleted (a corrupt landing must not pose as
 * the document's body).
 *
 * <p><b>Errors.</b> 400 names the offending parameter; 404 = unknown drive;
 * 405 = non-POST; 411 = absent/invalid Content-Length; 502 = the backing
 * store or the intake save failed.
 */
public final class UploadHttpServer implements AutoCloseable {

    /** The single route this server serves. */
    public static final String UPLOAD_PATH = "/v1/documents:upload";

    private static final Logger LOG = LoggerFactory.getLogger(UploadHttpServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DocumentGrpcService documentService;
    private final DriveLedger drives;
    private final BlobStore blobStore;

    private HttpServer server;
    private ExecutorService executor;

    /**
     * @param documentService the gRPC document service whose blocking intake
     *        save this route reuses (one save path, one dedupe semantic)
     * @param drives the drive ledger (drive name + account → bucket/prefix)
     * @param blobStore the object-storage port the body streams into
     */
    public UploadHttpServer(DocumentGrpcService documentService, DriveLedger drives,
            BlobStore blobStore) {
        this.documentService = documentService;
        this.drives = drives;
        this.blobStore = blobStore;
    }

    /**
     * Binds and starts the server.
     *
     * @param port the listen port (0 = ephemeral, read it back via {@link #port()})
     * @return the bound port
     */
    public synchronized int start(int port) {
        if (server != null) {
            throw new IllegalStateException("HTTP upload server already started");
        }
        HttpServer created;
        try {
            created = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind HTTP upload server on port " + port, e);
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

        DriveRecord drive = drives.findByName(accountId, driveName)
                .orElseThrow(() -> new HttpError(404,
                        "drive '" + driveName + "' not found for account '" + accountId + "'"));

        boolean deriveDocId = docId == null || docId.isBlank();
        // Blank doc_id: the final key depends on the content hash, which only
        // exists after the stream lands — stage, derive, server-side copy.
        String objectKey = deriveDocId
                ? blobKey(drive, accountId, "staging-" + UUID.randomUUID())
                : blobKey(drive, accountId,
                        DocumentIds.blobId(docId, datasourceId, accountId).toString());

        MessageDigest digest = sha256();
        try (InputStream body = new DigestInputStream(exchange.getRequestBody(), digest)) {
            // No sha256Hex on the PutSpec: the digest is only complete once
            // the stream is consumed, which is exactly when put returns. The
            // landed bytes are proven by the X-Content-Sha256 check below and
            // by the checksum stamped on the Document's blob.
            blobStore.put(new BlobStore.PutSpec(drive.bucket, objectKey, contentType, null, null),
                    body, contentLength);
        } catch (RuntimeException | IOException e) {
            throw new HttpError(502, "blob store write failed for key " + objectKey
                    + ": " + e.getMessage());
        }
        String sha256 = HexFormat.of().formatHex(digest.digest());

        if (deriveDocId) {
            docId = UUID.nameUUIDFromBytes(
                    ("doc-content|" + sha256).getBytes(StandardCharsets.UTF_8)).toString();
            String finalKey = blobKey(drive, accountId,
                    DocumentIds.blobId(docId, datasourceId, accountId).toString());
            try {
                blobStore.copy(drive.bucket, objectKey, drive.bucket, finalKey);
                blobStore.delete(drive.bucket, objectKey);
            } catch (RuntimeException e) {
                throw new HttpError(502, "failed to move staged blob to " + finalKey
                        + ": " + e.getMessage());
            }
            objectKey = finalKey;
        }

        String declaredSha = exchange.getRequestHeaders().getFirst("X-Content-Sha256");
        if (declaredSha != null && !declaredSha.isBlank()
                && !declaredSha.trim().equalsIgnoreCase(sha256)) {
            // The landed bytes are not what the client sent; they must not
            // pose as the document's body.
            try {
                blobStore.delete(drive.bucket, objectKey);
            } catch (RuntimeException e) {
                LOG.warn("best-effort delete of mismatched upload {} failed: {}",
                        objectKey, e.getMessage());
            }
            throw new HttpError(400, "X-Content-Sha256 mismatch: declared " + declaredSha.trim()
                    + " but the received body hashes to " + sha256);
        }

        UUID blobId = DocumentIds.blobId(docId, datasourceId, accountId);
        OwnershipContext.Builder ownership = OwnershipContext.newBuilder()
                .setAccountId(accountId)
                .setDatasourceId(datasourceId);
        if (connectorId != null && !connectorId.isBlank()) {
            ownership.setConnectorId(connectorId);
        }
        Document document = Document.newBuilder()
                .setDocId(docId)
                .setOwnership(ownership)
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId(blobId.toString())
                        .setDriveId(driveName)
                        .setStorageRef(FileStorageReference.newBuilder()
                                .setDriveName(driveName)
                                .setObjectKey(objectKey))
                        .setMimeType(contentType)
                        .setFilename(filename)
                        .setSizeBytes(contentLength)
                        .setChecksum(sha256)
                        .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256)))
                .build();

        // The SAME intake save as gRPC SaveDocument: use_datasource_id arm at
        // the account's intake graph — one dedupe, one upsert, one revive.
        SaveDocumentRequest.Builder save = SaveDocumentRequest.newBuilder()
                .setDocument(document)
                .setDrive(driveName)
                .setUseDatasourceId(true)
                .setGraphId("intake:" + accountId);
        if (connectorId != null && !connectorId.isBlank()) {
            save.setConnectorId(connectorId);
        }
        if (crawlId != null && !crawlId.isBlank()) {
            save.setCrawlId(crawlId);
        }
        SaveDocumentResponse saved = documentService.saveBlocking(save.build());
        LOG.debug("Uploaded doc_id={} to {} ({} bytes, sha256={}, deduplicated={})",
                docId, objectKey, contentLength, sha256, saved.getDeduplicated());

        ObjectNode storageRef = MAPPER.createObjectNode()
                .put("drive_name", driveName)
                .put("object_key", objectKey);
        ObjectNode response = MAPPER.createObjectNode()
                .put("node_id", saved.getNodeId())
                .put("doc_id", docId)
                .put("deduplicated", saved.getDeduplicated())
                .put("size_bytes", contentLength)
                .put("sha256", sha256);
        response.set("storage_ref", storageRef);
        return response;
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

    /** Blob object key: {@code <drive.prefix>/blobs/<accountId>/<blobId>.bin}. */
    private static String blobKey(DriveRecord drive, String accountId, String blobId) {
        String prefix = drive.prefix == null ? "" : drive.prefix;
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return (prefix.isBlank() ? "" : prefix + "/")
                + "blobs/" + accountId + "/" + blobId + ".bin";
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

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
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

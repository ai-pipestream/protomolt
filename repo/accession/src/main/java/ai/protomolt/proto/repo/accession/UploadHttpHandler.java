package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.UploadHeader;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * HTTP raw streaming upload handler backed by {@link DocumentUploads}
 * (requirements §6, §11, §15.10).
 * <p>
 * Streams {@code POST /v1/documents:upload} request bodies directly into the configured stack
 * through the v2 accession pipeline without buffering the uploaded file in heap memory.
 */
public final class UploadHttpHandler implements HttpHandler {
    /** Canonical HTTP path for raw streaming document uploads. */
    public static final String UPLOAD_PATH = "/v1/documents:upload";

    private static final String METHOD_POST = "POST";
    private static final String HEADER_API_TOKEN = "api_token";
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String HEADER_CONTENT_LENGTH = "Content-Length";
    private static final String HEADER_TRANSFER_ENCODING = "Transfer-Encoding";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_CONTENT_SHA256 = "X-Content-Sha256";
    private static final String HEADER_ACCOUNT_ID = "x-account-id";
    private static final String HEADER_DATASOURCE_ID = "x-datasource-id";
    private static final String HEADER_DRIVE_NAME = "x-drive-name";
    private static final String HEADER_FILENAME = "x-filename";
    private static final String HEADER_OPERATION_ID = "x-operation-id";
    private static final String HEADER_DOC_ID = "x-doc-id";
    private static final String HEADER_BLOB_ID = "x-blob-id";
    private static final String HEADER_CONNECTOR_ID = "x-connector-id";
    private static final String HEADER_EXPECTED_REVISION = "x-expected-revision";

    private static final String PARAM_ACCOUNT_ID = "account_id";
    private static final String PARAM_DATASOURCE_ID = "datasource_id";
    private static final String PARAM_DRIVE = "drive";
    private static final String PARAM_FILENAME = "filename";
    private static final String PARAM_OPERATION_ID = "operation_id";
    private static final String PARAM_DOC_ID = "doc_id";
    private static final String PARAM_BLOB_ID = "blob_id";
    private static final String PARAM_CONNECTOR_ID = "connector_id";
    private static final String PARAM_CONTENT_TYPE = "content_type";
    private static final String PARAM_EXPECTED_REVISION = "expected_revision";
    private static final String PARAM_SHA256 = "sha256";

    private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";

    private static final int HTTP_OK = 200;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_UNAUTHORIZED = 401;
    private static final int HTTP_FORBIDDEN = 403;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_CONFLICT = 409;
    private static final int HTTP_LENGTH_REQUIRED = 411;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_INTERNAL_ERROR = 500;
    private static final int HTTP_UNAVAILABLE = 503;

    private final DocumentUploads uploads;
    private final byte[] expectedTokenBytes;
    private final Supplier<RepositoryCaller> callerSupplier;

    /**
     * Constructs an HTTP streaming upload handler using a process-authority intake caller when
     * the configured API token is presented.
     *
     * @param uploads the streaming upload coordinator
     * @param apiToken the required API token (must not be blank)
     */
    public UploadHttpHandler(DocumentUploads uploads, String apiToken) {
        this(uploads, apiToken, () -> new RepositoryCaller("http-upload-host", true));
    }

    /**
     * Constructs an HTTP streaming upload handler with an explicit caller supplier.
     *
     * @param uploads the streaming upload coordinator
     * @param apiToken the required API token (must not be blank)
     * @param callerSupplier supplies the authenticated {@link RepositoryCaller} once the API token matches
     */
    public UploadHttpHandler(
            DocumentUploads uploads,
            String apiToken,
            Supplier<RepositoryCaller> callerSupplier) {
        this.uploads = Objects.requireNonNull(uploads, "uploads");
        Objects.requireNonNull(apiToken, "apiToken");
        if (apiToken.isBlank()) {
            throw new IllegalArgumentException("apiToken must not be blank");
        }
        this.expectedTokenBytes = apiToken.getBytes(StandardCharsets.UTF_8);
        this.callerSupplier = Objects.requireNonNull(callerSupplier, "callerSupplier");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (!authenticate(exchange.getRequestHeaders())) {
                sendError(exchange, HTTP_UNAUTHORIZED, "Unauthorized");
                return;
            }
            if (!UPLOAD_PATH.equals(exchange.getRequestURI().getPath())) {
                sendError(exchange, HTTP_NOT_FOUND, "Not found");
                return;
            }
            if (!METHOD_POST.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendError(exchange, HTTP_METHOD_NOT_ALLOWED, "Method not allowed");
                return;
            }

            long contentLength = parseContentLength(exchange.getRequestHeaders());
            var query = parseQuery(exchange.getRequestURI().getRawQuery());
            var headers = exchange.getRequestHeaders();

            var accountId = param(query, headers, PARAM_ACCOUNT_ID, HEADER_ACCOUNT_ID);
            var datasourceId = param(query, headers, PARAM_DATASOURCE_ID, HEADER_DATASOURCE_ID);
            var driveName = param(query, headers, PARAM_DRIVE, HEADER_DRIVE_NAME);
            var filename = param(query, headers, PARAM_FILENAME, HEADER_FILENAME);
            if (isBlank(accountId) || isBlank(datasourceId) || isBlank(driveName) || isBlank(filename)) {
                sendError(
                        exchange,
                        HTTP_BAD_REQUEST,
                        "Missing required parameter: account_id, datasource_id, drive, and filename are required");
                return;
            }

            var explicitOpId = param(query, headers, PARAM_OPERATION_ID, HEADER_OPERATION_ID);
            var operationId = !isBlank(explicitOpId) ? explicitOpId : UUID.randomUUID().toString();
            var declaredSha256 = param(query, headers, PARAM_SHA256, HEADER_CONTENT_SHA256);
            var explicitDocId = param(query, headers, PARAM_DOC_ID, HEADER_DOC_ID);
            final String docId;
            if (!isBlank(explicitDocId)) {
                docId = explicitDocId;
            } else if (!isBlank(declaredSha256)) {
                docId = UUID.nameUUIDFromBytes(
                        ("doc-content|" + declaredSha256.toLowerCase(Locale.ROOT)).getBytes(StandardCharsets.UTF_8))
                        .toString();
            } else if (!isBlank(explicitOpId)) {
                docId = UUID.nameUUIDFromBytes(
                        ("doc-op|" + operationId).getBytes(StandardCharsets.UTF_8))
                        .toString();
            } else {
                docId = UUID.randomUUID().toString();
            }

            var explicitBlobId = param(query, headers, PARAM_BLOB_ID, HEADER_BLOB_ID);
            var blobId = !isBlank(explicitBlobId)
                    ? explicitBlobId
                    : UUID.nameUUIDFromBytes(
                            ("blob-doc|" + docId + "|" + filename).getBytes(StandardCharsets.UTF_8))
                            .toString();

            var connectorId = param(query, headers, PARAM_CONNECTOR_ID, HEADER_CONNECTOR_ID);
            var contentType = param(query, headers, PARAM_CONTENT_TYPE, HEADER_CONTENT_TYPE);
            if (isBlank(contentType)) {
                contentType = DEFAULT_CONTENT_TYPE;
            }
            var expectedRevision = parseExpectedRevision(
                    param(query, headers, PARAM_EXPECTED_REVISION, HEADER_EXPECTED_REVISION));

            var header = buildUploadHeader(
                    operationId,
                    accountId,
                    datasourceId,
                    connectorId,
                    driveName,
                    docId,
                    blobId,
                    filename,
                    contentType,
                    contentLength,
                    declaredSha256,
                    expectedRevision);

            try (var body = exchange.getRequestBody()) {
                var result = uploads.uploadResult(callerSupplier.get(), header, body);
                var saved = result.response().getSaved();
                var accession = saved.getAccession();
                var json = formatSuccessJson(
                        operationId,
                        accession.getAddress().getDocId(),
                        accession.getRevision(),
                        saved.getReplayed(),
                        result.sizeBytes(),
                        result.sha256());
                sendJson(exchange, HTTP_OK, json);
            }
        } catch (LengthRequiredException missingLength) {
            sendError(exchange, HTTP_LENGTH_REQUIRED, missingLength.getMessage());
        } catch (BadRequestException badRequest) {
            sendError(exchange, HTTP_BAD_REQUEST, badRequest.getMessage());
        } catch (AccessionException failure) {
            sendError(exchange, mapStatusCode(failure), failure.getMessage());
        } catch (RuntimeException failure) {
            sendError(exchange, HTTP_INTERNAL_ERROR, "Upload failed");
        } finally {
            exchange.close();
        }
    }

    private boolean authenticate(Headers headers) {
        String presented = headers.getFirst(HEADER_API_TOKEN);
        if (presented != null) {
            presented = presented.trim();
        }
        if (presented == null || presented.isEmpty()) {
            String auth = headers.getFirst(HEADER_AUTHORIZATION);
            if (auth != null) {
                String trimmed = auth.trim();
                if (trimmed.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
                    presented = trimmed.substring(BEARER_PREFIX.length()).trim();
                }
            }
        }
        if (presented == null || presented.isEmpty()) {
            return false;
        }
        return MessageDigest.isEqual(expectedTokenBytes, presented.getBytes(StandardCharsets.UTF_8));
    }

    private static long parseContentLength(Headers headers) throws LengthRequiredException, BadRequestException {
        String transferEncoding = headers.getFirst(HEADER_TRANSFER_ENCODING);
        if (transferEncoding != null && !transferEncoding.isBlank()) {
            throw new LengthRequiredException("Content-Length header is required; chunked transfer is not supported");
        }
        var values = headers.get(HEADER_CONTENT_LENGTH);
        if (values == null || values.isEmpty()) {
            throw new LengthRequiredException("Content-Length header is required");
        }
        if (values.size() > 1) {
            throw new BadRequestException("Multiple Content-Length headers are not allowed");
        }
        String raw = values.getFirst();
        if (raw == null || raw.isBlank()) {
            throw new LengthRequiredException("Content-Length header is required");
        }
        final long length;
        try {
            length = Long.parseLong(raw.trim());
        } catch (NumberFormatException invalid) {
            throw new BadRequestException("Invalid Content-Length header");
        }
        if (length < 0) {
            throw new BadRequestException("Content-Length must be non-negative");
        }
        return length;
    }

    private static ExpectedRevision parseExpectedRevision(String value) throws BadRequestException {
        if (isBlank(value)) {
            return ExpectedRevision.newBuilder().setNone(true).build();
        }
        final long revision;
        try {
            revision = Long.parseLong(value.trim());
        } catch (NumberFormatException invalid) {
            throw new BadRequestException("Invalid expected_revision");
        }
        if (revision < 0) {
            throw new BadRequestException("expected_revision must be non-negative");
        }
        if (revision == 0) {
            return ExpectedRevision.newBuilder().setNone(true).build();
        }
        return ExpectedRevision.newBuilder().setRevision(revision).build();
    }

    private static UploadHeader buildUploadHeader(
            String operationId,
            String accountId,
            String datasourceId,
            String connectorId,
            String driveName,
            String docId,
            String blobId,
            String filename,
            String contentType,
            long contentLength,
            String declaredSha256,
            ExpectedRevision expectedRevision) {
        var ownership = OwnershipContext.newBuilder()
                .setAccountId(accountId)
                .setDatasourceId(datasourceId);
        var intake = IntakeOrigin.newBuilder()
                .setDatasourceId(datasourceId);
        if (!isBlank(connectorId)) {
            ownership.setConnectorId(connectorId);
            intake.setConnectorId(connectorId);
        }

        var blob = Blob.newBuilder()
                .setBlobId(blobId)
                .setMimeType(contentType)
                .setFilename(filename)
                .setSizeBytes(contentLength)
                .build();
        var document = Document.newBuilder()
                .setDocId(docId)
                .setOwnership(ownership)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle(filename)
                        .setSourceMimeType(contentType))
                .setBlobBag(BlobBag.newBuilder().setBlob(blob))
                .build();

        var save = SaveDocumentRequest.newBuilder()
                .setOperationId(operationId)
                .setTarget(DocumentTarget.newBuilder().setUseDatasourceId(datasourceId))
                .setExpected(expectedRevision)
                .setDrive(driveName)
                .setOwner(SaveOwner.newBuilder().setIntake(intake))
                .setDocument(document)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS))
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .build();

        var header = UploadHeader.newBuilder()
                .setSave(save)
                .setBlobId(blobId)
                .setDeclaredLength(contentLength);
        if (!isBlank(declaredSha256)) {
            header.setExpectedSha256(declaredSha256.toLowerCase(Locale.ROOT));
        }
        return header.build();
    }

    private static int mapStatusCode(AccessionException failure) {
        return switch (failure.code()) {
            case INVALID_ARGUMENT -> HTTP_BAD_REQUEST;
            case PERMISSION_DENIED -> HTTP_FORBIDDEN;
            case NOT_FOUND -> HTTP_NOT_FOUND;
            case ABORTED, ALREADY_EXISTS, FAILED_PRECONDITION -> HTTP_CONFLICT;
            case RESOURCE_EXHAUSTED -> HTTP_TOO_MANY_REQUESTS;
            case UNAVAILABLE -> HTTP_UNAVAILABLE;
            default -> HTTP_INTERNAL_ERROR;
        };
    }

    private static String param(
            Map<String, String> query,
            Headers headers,
            String queryKey,
            String headerName) {
        String fromQuery = query.get(queryKey);
        if (fromQuery != null && !fromQuery.isBlank()) {
            return fromQuery.trim();
        }
        String fromHeader = headers.getFirst(headerName);
        if (fromHeader != null && !fromHeader.isBlank()) {
            return fromHeader.trim();
        }
        return null;
    }

    static Map<String, String> parseQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }
        var result = new LinkedHashMap<String, String>();
        int start = 0;
        while (start <= rawQuery.length()) {
            int amp = rawQuery.indexOf('&', start);
            int end = amp < 0 ? rawQuery.length() : amp;
            if (end > start) {
                String pair = rawQuery.substring(start, end);
                int eq = pair.indexOf('=');
                if (eq < 0) {
                    result.putIfAbsent(decode(pair), "");
                } else {
                    result.putIfAbsent(decode(pair.substring(0, eq)), decode(pair.substring(eq + 1)));
                }
            }
            if (amp < 0) break;
            start = amp + 1;
        }
        return result;
    }

    private static String decode(String component) {
        return URLDecoder.decode(component, StandardCharsets.UTF_8).trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String formatSuccessJson(
            String operationId,
            String docId,
            long revision,
            boolean replayed,
            long sizeBytes,
            String sha256) {
        return "{"
                + "\"operation_id\":\"" + escapeJson(operationId) + "\","
                + "\"doc_id\":\"" + escapeJson(docId) + "\","
                + "\"revision\":" + revision + ","
                + "\"replayed\":" + replayed + ","
                + "\"size_bytes\":" + sizeBytes + ","
                + "\"sha256\":\"" + escapeJson(sha256) + "\""
                + "}";
    }

    private static void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String safeMessage = message == null ? "" : message;
        exchange.getResponseHeaders().set("Connection", "close");
        sendJson(exchange, statusCode, "{\"error\":\"" + escapeJson(safeMessage) + "\"}");
    }

    private static void sendJson(HttpExchange exchange, int statusCode, String json) throws IOException {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set(HEADER_CONTENT_TYPE, JSON_CONTENT_TYPE);
        exchange.sendResponseHeaders(statusCode, payload.length);
        try (var out = exchange.getResponseBody()) {
            out.write(payload);
        }
    }

    private static String escapeJson(String value) {
        if (value == null) return "";
        var sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    private static final class LengthRequiredException extends Exception {
        LengthRequiredException(String message) {
            super(message);
        }
    }

    private static final class BadRequestException extends Exception {
        BadRequestException(String message) {
            super(message);
        }
    }
}

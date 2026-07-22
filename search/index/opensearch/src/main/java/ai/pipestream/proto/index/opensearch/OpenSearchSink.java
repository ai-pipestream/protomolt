package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Thin HTTP sink for OpenSearch: creates an index from an {@link IndexingPlan} and writes
 * document maps through the {@code _bulk} endpoint. Talks plain JSON over
 * {@link java.net.http.HttpClient}; no OpenSearch client dependency.
 *
 * <p>{@link #ensureIndex} is idempotent: an existing index is left untouched (its mappings are
 * neither compared nor updated), a missing one is created with the
 * {@link OpenSearchMappingGenerator} mappings for the plan plus the {@code index.knn} setting
 * when the plan carries a {@link IndexFieldKind#VECTOR} field, which the {@code knn_vector}
 * mapping requires. A create lost to a concurrent writer counts as already existing.
 *
 * <p>{@link #bulkWrite} sends one NDJSON {@code index} action per document, with
 * caller-supplied ids or engine-assigned ones. Bulk is not atomic: when the engine reports
 * {@code errors: true}, the accepted documents stay written and the per-item failures are
 * collected into one {@link IOException} naming each failed id with the engine's error type
 * and reason.
 *
 * <p>Pair with {@link OpenSearchDocumentMapper} to produce the document maps. The sink owns
 * its client unless one is supplied; {@link #close()} closes only an owned client.
 */
public final class OpenSearchSink implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final OpenSearchMappingGenerator MAPPINGS = new OpenSearchMappingGenerator();
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final String baseUrl;
    private final HttpClient client;
    private final boolean ownsClient;

    /**
     * Creates a sink for the given base URL, e.g. {@code http://localhost:9200} (no trailing
     * slash required), with its own anonymous {@link HttpClient}.
     *
     * @param baseUrl base URL of the OpenSearch node
     */
    public OpenSearchSink(String baseUrl) {
        this(baseUrl, HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build(), true);
    }

    /**
     * Creates a sink over a caller-supplied client (custom TLS, authenticator, or executor);
     * the caller keeps ownership and {@link #close()} leaves the client open.
     *
     * @param baseUrl base URL of the OpenSearch node
     * @param client HTTP client to send requests through
     */
    public OpenSearchSink(String baseUrl, HttpClient client) {
        this(baseUrl, client, false);
    }

    private OpenSearchSink(String baseUrl, HttpClient client, boolean ownsClient) {
        String url = Objects.requireNonNull(baseUrl, "baseUrl");
        this.baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.client = Objects.requireNonNull(client, "client");
        this.ownsClient = ownsClient;
    }

    /**
     * Creates the index for a plan if it does not exist. An existing index is left untouched.
     *
     * @param index index name
     * @param plan plan whose generated mappings (and {@code index.knn} setting, when the plan
     *        has a vector field) define the index
     * @return {@code true} when this call created the index, {@code false} when it already existed
     * @throws IOException when the engine refuses the create or cannot be reached
     */
    public boolean ensureIndex(String index, IndexingPlan plan) throws IOException {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(plan, "plan");
        HttpResponse<String> head = send("HEAD", "/" + index, null, null);
        if (head.statusCode() == 200) {
            return false;
        }
        if (head.statusCode() != 404) {
            throw new IOException("Checking index '" + index + "' returned HTTP " + head.statusCode());
        }
        Map<String, Object> body = new LinkedHashMap<>();
        if (plan.indexable().stream().anyMatch(field -> field.hint().type() == IndexFieldKind.VECTOR)) {
            // knn_vector mappings fail the index create unless the index enables knn.
            body.put("settings", Map.of("index.knn", true));
        }
        body.put("mappings", MAPPINGS.generate(plan));
        HttpResponse<String> response = send("PUT", "/" + index,
                "application/json", JSON.writeValueAsString(body));
        if (response.statusCode() / 100 == 2) {
            return true;
        }
        if (alreadyExists(response.body())) {
            // Create race lost: another writer made the index between the check and the PUT.
            return false;
        }
        throw new IOException("Creating index '" + index + "' returned HTTP "
                + response.statusCode() + ": " + response.body());
    }

    /**
     * Writes documents under caller-supplied ids in one {@code _bulk} request, in the map's
     * iteration order. An empty map sends nothing.
     *
     * @param index index name
     * @param documentsById document maps keyed by the {@code _id} to write each one under
     * @param refresh when {@code true}, the request carries {@code refresh=true} so the
     *        documents are searchable as soon as the call returns
     * @throws IOException when the request fails, or when any bulk item fails — the message
     *         then lists every failed id with the engine's error type and reason
     */
    public void bulkWrite(String index, Map<String, Map<String, Object>> documentsById, boolean refresh)
            throws IOException {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(documentsById, "documentsById");
        if (documentsById.isEmpty()) {
            return;
        }
        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, Map<String, Object>> entry : documentsById.entrySet()) {
            appendAction(body, entry.getKey(), entry.getValue());
        }
        executeBulk(index, body.toString(), refresh, documentsById.size());
    }

    /**
     * Writes documents with engine-assigned ids in one {@code _bulk} request. Use
     * {@link #bulkWrite(String, Map, boolean)} when ids must be deterministic. An empty list
     * sends nothing.
     *
     * @param index index name
     * @param documents document maps, written in list order
     * @param refresh when {@code true}, the request carries {@code refresh=true} so the
     *        documents are searchable as soon as the call returns
     * @throws IOException when the request fails, or when any bulk item fails — the message
     *         then lists every failed id with the engine's error type and reason
     */
    public void bulkWrite(String index, List<Map<String, Object>> documents, boolean refresh)
            throws IOException {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(documents, "documents");
        if (documents.isEmpty()) {
            return;
        }
        StringBuilder body = new StringBuilder();
        for (Map<String, Object> document : documents) {
            appendAction(body, null, document);
        }
        executeBulk(index, body.toString(), refresh, documents.size());
    }

    /** Closes the underlying {@link HttpClient} when this sink created it. */
    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }

    // ---------------------------------------------------------------- bulk protocol

    /** Action line ({@code {"index": {"_id": id}}}, id omitted when null) plus source line. */
    private static void appendAction(StringBuilder body, String id, Map<String, Object> document)
            throws IOException {
        body.append(JSON.writeValueAsString(
                        Map.of("index", id == null ? Map.of() : Map.of("_id", id))))
                .append('\n')
                .append(JSON.writeValueAsString(document))
                .append('\n');
    }

    private void executeBulk(String index, String ndjson, boolean refresh, int documentCount)
            throws IOException {
        String path = "/" + index + "/_bulk" + (refresh ? "?refresh=true" : "");
        HttpResponse<String> response = send("POST", path, "application/x-ndjson", ndjson);
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Bulk write to index '" + index + "' returned HTTP "
                    + response.statusCode() + ": " + response.body());
        }
        JsonNode result = JSON.readTree(response.body());
        if (!result.path("errors").asBoolean()) {
            return;
        }
        List<String> failures = new ArrayList<>();
        for (JsonNode wrapper : result.path("items")) {
            JsonNode item = wrapper.path("index"); // only index actions are sent
            JsonNode error = item.path("error");
            if (error.isMissingNode() || error.isNull()) {
                continue;
            }
            failures.add(item.path("_id").asText() + ": " + error.path("type").asText()
                    + ": " + error.path("reason").asText());
        }
        throw new IOException("Bulk write to index '" + index + "' failed for " + failures.size()
                + " of " + documentCount + " documents: " + String.join("; ", failures));
    }

    // ---------------------------------------------------------------- HTTP plumbing

    private static boolean alreadyExists(String body) {
        try {
            return "resource_already_exists_exception".equals(
                    JSON.readTree(body).path("error").path("type").asText());
        } catch (IOException e) {
            return false;
        }
    }

    private HttpResponse<String> send(String method, String path, String contentType, String body)
            throws IOException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(REQUEST_TIMEOUT);
        if (contentType != null) {
            request.header("Content-Type", contentType);
        }
        request.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        try {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during " + method + " " + path, e);
        }
    }
}

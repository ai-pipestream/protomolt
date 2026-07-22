package ai.pipestream.proto.index.opensearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Thin HTTP read side for OpenSearch: runs a kNN query against an index and parses the hits.
 * The read-side sibling of {@link OpenSearchSink}, with the same client discipline: plain JSON
 * over {@link java.net.http.HttpClient}, no OpenSearch client dependency, a 10 second connect
 * timeout and a 30 second request timeout.
 *
 * <p>{@link #knn} POSTs {@code /{index}/_search} with a {@code knn} query clause asking for the
 * {@code k} nearest documents and parses {@code hits.hits[]} into {@link OpenSearchHit} records.
 * A non-2xx response surfaces the index name, the status, and the engine's body.
 *
 * <p>The search owns its client unless one is supplied; {@link #close()} closes only an owned
 * client.
 */
public final class OpenSearchSearch implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final String baseUrl;
    private final HttpClient client;
    private final boolean ownsClient;

    /**
     * Creates a search for the given base URL, e.g. {@code http://localhost:9200} (no trailing
     * slash required), with its own anonymous {@link HttpClient}.
     *
     * @param baseUrl base URL of the OpenSearch node
     */
    public OpenSearchSearch(String baseUrl) {
        this(baseUrl, HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build(), true);
    }

    /**
     * Creates a search over a caller-supplied client (custom TLS, authenticator, or executor);
     * the caller keeps ownership and {@link #close()} leaves the client open.
     *
     * @param baseUrl base URL of the OpenSearch node
     * @param client HTTP client to send requests through
     */
    public OpenSearchSearch(String baseUrl, HttpClient client) {
        this(baseUrl, client, false);
    }

    private OpenSearchSearch(String baseUrl, HttpClient client, boolean ownsClient) {
        String url = Objects.requireNonNull(baseUrl, "baseUrl");
        this.baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.client = Objects.requireNonNull(client, "client");
        this.ownsClient = ownsClient;
    }

    /**
     * Returns the {@code k} documents of {@code index} nearest {@code vector} on the
     * {@code knn_vector} field named {@code vectorField}, nearest first.
     *
     * @param index index name
     * @param vectorField name of the {@code knn_vector} field to query
     * @param vector the query vector; its length must match the field's dimension
     * @param k how many nearest documents to return
     * @return the hits in engine order (nearest first)
     * @throws IOException when the request fails, or when the engine answers non-2xx — the
     *         message then names the index, the status, and the engine's body
     */
    public List<OpenSearchHit> knn(String index, String vectorField, List<Float> vector, int k)
            throws IOException {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(vectorField, "vectorField");
        Objects.requireNonNull(vector, "vector");
        Map<String, Object> body = Map.of(
                "size", k,
                "query", Map.of("knn", Map.of(vectorField, Map.of(
                        "vector", vector, "k", k))));
        HttpResponse<String> response = send("POST", "/" + index + "/_search",
                "application/json", JSON.writeValueAsString(body));
        if (response.statusCode() / 100 != 2) {
            throw new IOException("kNN search on index '" + index + "' returned HTTP "
                    + response.statusCode() + ": " + response.body());
        }
        JsonNode hits = JSON.readTree(response.body()).path("hits").path("hits");
        List<OpenSearchHit> results = new ArrayList<>(hits.size());
        for (JsonNode hit : hits) {
            results.add(new OpenSearchHit(
                    hit.path("_id").asText(),
                    hit.path("_score").asDouble(),
                    JSON.convertValue(hit.path("_source"), new TypeReference<>() {
                    })));
        }
        return results;
    }

    /** Closes the underlying {@link HttpClient} when this search created it. */
    @Override
    public void close() {
        if (ownsClient) {
            client.close();
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

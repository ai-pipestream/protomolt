package ai.pipestream.proto.rerank.ovms;

import ai.pipestream.proto.rerank.RerankProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link RerankProvider} that calls an OpenVINO Model Server (OVMS) rerank servable over the
 * OpenAI-style REST rerank endpoint, not gRPC: the OVMS rerank servable's graph expects an
 * HTTP payload packet, so the gRPC ModelInfer path answers "ovms::HttpPayload was requested"
 * on these servables. Each call POSTs {@code {base}/v3/rerank} with a JSON body of model,
 * query, and documents, and reads the {@code results} array of index/relevance_score objects
 * back into input order.
 *
 * <p>The client is a plain {@link HttpClient} with a 10 second connect timeout; every
 * request carries a 30 second timeout.
 *
 * <p>The {@link #OvmsRerankProvider(String, String)} constructor configures eagerly. The
 * no-argument ServiceLoader constructor resolves the base URL and model on first use from
 * the {@value #URL_PROPERTY} and {@value #MODEL_PROPERTY} system properties, falling back to
 * the {@value #URL_ENVIRONMENT_VARIABLE} and {@value #MODEL_ENVIRONMENT_VARIABLE}
 * environment variables, so discovery through
 * {@link ai.pipestream.proto.rerank.RerankProviders} never fails on an unconfigured provider
 * that is not actually used.
 *
 * <p>The provider is safe for concurrent use; {@link HttpClient} multiplexes calls.
 */
public final class OvmsRerankProvider implements RerankProvider {

    /** The id this provider registers under: {@value}. */
    public static final String PROVIDER_ID = "ovms";

    /** System property naming the OVMS base URL (e.g. {@code http://localhost:8003}): {@value}. */
    public static final String URL_PROPERTY = "protomolt.rerank.ovms.url";

    /** Environment variable consulted when {@link #URL_PROPERTY} is unset: {@value}. */
    public static final String URL_ENVIRONMENT_VARIABLE = "PROTOMOLT_RERANK_OVMS_URL";

    /** System property naming the servable to rerank against: {@value}. */
    public static final String MODEL_PROPERTY = "protomolt.rerank.ovms.model";

    /** Environment variable consulted when {@link #MODEL_PROPERTY} is unset: {@value}. */
    public static final String MODEL_ENVIRONMENT_VARIABLE = "PROTOMOLT_RERANK_OVMS_MODEL";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Object lock = new Object();
    private final HttpClient client;
    private volatile String baseUrl;
    private volatile String modelName;

    /**
     * ServiceLoader constructor. The base URL and model are resolved on the first
     * {@link #score(String, List)} call, from the {@value #URL_PROPERTY} and
     * {@value #MODEL_PROPERTY} system properties or, when those are unset, the
     * {@value #URL_ENVIRONMENT_VARIABLE} and {@value #MODEL_ENVIRONMENT_VARIABLE}
     * environment variables.
     */
    public OvmsRerankProvider() {
        this.client = defaultClient();
    }

    /**
     * Reranks against the servable {@code modelName} at {@code baseUrl} (e.g.
     * {@code http://localhost:8003}; a trailing slash is stripped).
     */
    public OvmsRerankProvider(String baseUrl, String modelName) {
        this(baseUrl, modelName, defaultClient());
    }

    /**
     * Reranks over a caller-supplied {@link HttpClient}, for tests and for deployments that
     * customize the client.
     */
    public OvmsRerankProvider(String baseUrl, String modelName, HttpClient client) {
        this.baseUrl = stripTrailingSlashes(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.modelName = Objects.requireNonNull(modelName, "modelName");
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    /**
     * {@inheritDoc}
     *
     * <p>One POST to {@code {base}/v3/rerank} carries the model, the query, and every
     * candidate text. The response's results can arrive in any order, each carrying the index
     * of the document it scores, so the scores are scattered back into input order. An empty
     * candidate list short-circuits to an empty result without a request.
     *
     * @throws IllegalStateException when this provider came from the ServiceLoader constructor
     *         and the configuration knobs do not name a URL and a model, when the server
     *         answers a non-2xx status (the message names the URL, the model, the status, and
     *         the body), when the call fails or is interrupted, or when the response results
     *         do not cover the request's documents exactly once
     */
    @Override
    public List<Double> score(String query, List<String> texts) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(texts, "texts");
        if (texts.isEmpty()) {
            return List.of();
        }
        resolveConfiguration();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v3/rerank"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody(query, texts)))
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("OVMS rerank failed for model '" + modelName
                    + "' against '" + baseUrl + "'", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reranking for model '"
                    + modelName + "' against '" + baseUrl + "'", e);
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("OVMS rerank failed for model '" + modelName
                    + "' against '" + baseUrl + "': HTTP " + response.statusCode()
                    + ": " + response.body());
        }
        return scores(response.body(), texts.size());
    }

    private String requestBody(String query, List<String> texts) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("model", modelName);
        body.put("query", query);
        ArrayNode documents = body.putArray("documents");
        for (String text : texts) {
            documents.add(text);
        }
        return body.toString();
    }

    private List<Double> scores(String body, int texts) {
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (IOException e) {
            throw new IllegalStateException("OVMS rerank returned a body that is not JSON: "
                    + body, e);
        }
        JsonNode results = root.path("results");
        if (!results.isArray()) {
            throw new IllegalStateException(
                    "OVMS rerank response carries no 'results' array: " + body);
        }
        double[] scores = new double[texts];
        boolean[] scored = new boolean[texts];
        for (JsonNode result : results) {
            int index = result.path("index").asInt(-1);
            if (index < 0 || index >= texts) {
                throw new IllegalStateException("OVMS rerank returned result index " + index
                        + " for a batch of " + texts + " documents");
            }
            if (scored[index]) {
                throw new IllegalStateException(
                        "OVMS rerank returned result index " + index + " twice");
            }
            scored[index] = true;
            scores[index] = result.path("relevance_score").asDouble();
        }
        for (int i = 0; i < scored.length; i++) {
            if (!scored[i]) {
                throw new IllegalStateException("OVMS rerank returned no result for document "
                        + i + " of " + texts);
            }
        }
        List<Double> aligned = new ArrayList<>(texts);
        for (double score : scores) {
            aligned.add(score);
        }
        return aligned;
    }

    /**
     * Resolves the base URL and model for the ServiceLoader constructor on first use, failing
     * with one message that names every missing knob. A no-op for the eager constructors,
     * which set both fields.
     */
    private void resolveConfiguration() {
        if (baseUrl != null && modelName != null) {
            return;
        }
        synchronized (lock) {
            if (baseUrl == null && modelName == null) {
                String resolvedUrl = configured(URL_PROPERTY, URL_ENVIRONMENT_VARIABLE);
                String resolvedModel = configured(MODEL_PROPERTY, MODEL_ENVIRONMENT_VARIABLE);
                if (resolvedUrl == null || resolvedModel == null) {
                    StringBuilder message = new StringBuilder(
                            "OVMS rerank provider is not configured;");
                    if (resolvedUrl == null) {
                        message.append(" set the '").append(URL_PROPERTY)
                                .append("' system property or the ")
                                .append(URL_ENVIRONMENT_VARIABLE)
                                .append(" environment variable to the server's base URL")
                                .append(" (e.g. http://localhost:8003);");
                    }
                    if (resolvedModel == null) {
                        message.append(" set the '").append(MODEL_PROPERTY)
                                .append("' system property or the ")
                                .append(MODEL_ENVIRONMENT_VARIABLE)
                                .append(" environment variable to the servable name;");
                    }
                    throw new IllegalStateException(message.toString());
                }
                baseUrl = stripTrailingSlashes(resolvedUrl);
                modelName = resolvedModel;
                return;
            }
            // The eager constructors never leave one field set and the other unset.
            throw new IllegalStateException("OVMS rerank provider is not configured");
        }
    }

    /** The value of {@code property}, falling back to {@code environmentVariable}; null when
     * neither is set. */
    private static String configured(String property, String environmentVariable) {
        String value = System.getProperty(property);
        if (value == null) {
            value = System.getenv(environmentVariable);
        }
        return value;
    }

    private static HttpClient defaultClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    private static String stripTrailingSlashes(String baseUrl) {
        String stripped = baseUrl;
        while (stripped.endsWith("/")) {
            stripped = stripped.substring(0, stripped.length() - 1);
        }
        return stripped;
    }
}

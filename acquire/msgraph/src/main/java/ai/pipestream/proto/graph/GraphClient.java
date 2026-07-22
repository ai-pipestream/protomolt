package ai.pipestream.proto.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A thin authorized door to the Microsoft Graph REST API: JSON in, JSON out, bearer token
 * from a supplier (so token refresh lives with {@link GraphAuth}, not here), and Graph's
 * throttling contract honored — 429/503 responses are retried after the server-directed
 * {@code Retry-After}. No Microsoft SDK: the surface this toolkit needs is a handful of
 * routes, and the whole client stays inspectable.
 */
public final class GraphClient {

    /** A Graph error, with the service's own code and message (client-relevant by design). */
    public static final class GraphApiException extends IOException {

        /** {@link #status()} for a failure Graph never expressed as an HTTP status. */
        public static final int NO_HTTP_STATUS = 0;

        private final int status;
        private final String code;
        private final String body;

        GraphApiException(int status, String code, String message) {
            this(status, code, message, "");
        }

        GraphApiException(int status, String code, String message, String body) {
            super("Graph " + (status == NO_HTTP_STATUS ? "operation" : Integer.toString(status))
                    + (code.isEmpty() ? "" : " (" + code + ")") + ": " + message);
            this.status = status;
            this.code = code;
            this.body = body;
        }

        /**
         * The HTTP status of the failing Graph response, or {@link #NO_HTTP_STATUS} when the
         * failure was not an HTTP one — an async operation that reported failure, or one this
         * client stopped waiting on. Both arrive over a 200 poll, so reporting 200 would name
         * a success.
         */
        public int status() {
            return status;
        }

        public String code() {
            return code;
        }

        /** Graph's full error response body; {@code innerError} often names the real cause. */
        public String body() {
            return body;
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_THROTTLE_RETRIES = 3;

    private final String baseUrl;
    private final Supplier<String> bearer;
    private final HttpClient http;

    public GraphClient(Supplier<String> bearer) {
        this("https://graph.microsoft.com/v1.0", bearer);
    }

    /** {@code baseUrl} is overridable for tests and national-cloud endpoints. */
    public GraphClient(String baseUrl, Supplier<String> bearer) {
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
        this.bearer = Objects.requireNonNull(bearer, "bearer");
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
    }

    public JsonNode get(String path) throws IOException, InterruptedException {
        return json(send(request(path).GET().build(), HttpResponse.BodyHandlers.ofString()));
    }

    public byte[] getBytes(String path) throws IOException, InterruptedException {
        HttpResponse<byte[]> response = send(request(path).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() >= 400) {
            throw error(response.statusCode(), new String(response.body(),
                    java.nio.charset.StandardCharsets.UTF_8));
        }
        return response.body();
    }

    public JsonNode post(String path, JsonNode body) throws IOException, InterruptedException {
        return json(send(request(path)
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.ofString()));
    }

    /** POST that answers 202 with a Location header (Graph's async operations). */
    public Optional<String> postAsync(String path, JsonNode body)
            throws IOException, InterruptedException {
        HttpResponse<String> response = send(request(path)
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw error(response.statusCode(), response.body());
        }
        return response.headers().firstValue("Location");
    }

    public JsonNode patch(String path, JsonNode body) throws IOException, InterruptedException {
        return json(send(request(path)
                        .header("content-type", "application/json")
                        .method("PATCH", HttpRequest.BodyPublishers.ofString(body.toString()))
                        .build(),
                HttpResponse.BodyHandlers.ofString()));
    }

    public JsonNode put(String path, JsonNode body) throws IOException, InterruptedException {
        return json(send(request(path)
                        .header("content-type", "application/json")
                        .PUT(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.ofString()));
    }

    public JsonNode putBytes(String path, byte[] content, String contentType)
            throws IOException, InterruptedException {
        return json(send(request(path)
                        .header("content-type", contentType)
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(content)).build(),
                HttpResponse.BodyHandlers.ofString()));
    }

    public void delete(String path) throws IOException, InterruptedException {
        HttpResponse<String> response = send(request(path).DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw error(response.statusCode(), response.body());
        }
    }

    /** Follows an async-operation URL until it reports {@code completed} (or fails). */
    public JsonNode awaitOperation(String operationUrl, Duration timeout)
            throws IOException, InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            JsonNode operation = get(operationUrl);
            String status = operation.path("status").asText("");
            if (status.equalsIgnoreCase("completed")) {
                return operation;
            }
            if (status.equalsIgnoreCase("failed")) {
                throw new GraphApiException(GraphApiException.NO_HTTP_STATUS, "operationFailed",
                        operation.path("error").path("message").asText("operation failed"),
                        operation.toString());
            }
            if (System.nanoTime() >= deadline) {
                throw new GraphApiException(GraphApiException.NO_HTTP_STATUS, "operationTimeout",
                        "Async operation still " + status + " after " + timeout);
            }
            Thread.sleep(Duration.ofSeconds(2));
        }
    }

    private HttpRequest.Builder request(String path) {
        String url = path.startsWith("http") ? path : baseUrl + path;
        return HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("authorization", "Bearer " + bearer.get());
    }

    /** Sends with Graph's throttling contract: 429/503 retried after Retry-After. */
    private <T> HttpResponse<T> send(HttpRequest req, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        HttpResponse<T> response = http.send(req, handler);
        int retries = 0;
        while ((response.statusCode() == 429 || response.statusCode() == 503)
                && retries < MAX_THROTTLE_RETRIES) {
            long waitSeconds = response.headers().firstValueAsLong("Retry-After").orElse(2L);
            Thread.sleep(Duration.ofSeconds(Math.min(waitSeconds, 60)));
            response = http.send(req, handler);
            retries++;
        }
        return response;
    }

    private static JsonNode json(HttpResponse<String> response) throws IOException {
        String body = response.body();
        if (response.statusCode() >= 400) {
            throw error(response.statusCode(), body);
        }
        if (body == null || body.isBlank()) {
            return JSON.createObjectNode();
        }
        return JSON.readTree(body);
    }

    private static GraphApiException error(int status, String body) {
        String code = "";
        String message = body == null ? "" : body;
        try {
            JsonNode node = JSON.readTree(body);
            code = node.path("error").path("code").asText("");
            message = node.path("error").path("message").asText(message);
        } catch (Exception ignored) {
            // not JSON; keep the raw body as the message
        }
        return new GraphApiException(status, code, message, body == null ? "" : body);
    }

    static ObjectNode object() {
        return JSON.createObjectNode();
    }
}

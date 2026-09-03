package ai.protomolt.proto.acquire.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A thin authorized client for the Confluence Cloud REST API v2: JSON in, JSON
 * out, basic auth from the account email plus API token, and the service's
 * throttling contract honored - 429 responses are retried after the
 * server-directed {@code Retry-After} with jitter, and a politeness limiter
 * keeps a minimum gap between requests. No Atlassian SDK: the crawler needs a
 * handful of read routes, and the whole client stays inspectable.
 *
 * <p>All calls are blocking and virtual-thread friendly: run them on virtual
 * threads and the {@code Thread.sleep} backoffs park instead of pinning.</p>
 */
public final class ConfluenceClient {

    /** A Confluence API error, carrying the service's status and raw body. */
    public static final class ConfluenceApiException extends IOException {
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        private final int status;
        private final String body;

        ConfluenceApiException(int status, String body) {
            super("Confluence " + status + ": " + abbreviate(body));
            this.status = status;
            this.body = body == null ? "" : body;
        }

        /** The HTTP status of the failing response. */
        public int status() {
            return status;
        }

        /** The raw error response body. */
        public String body() {
            return body;
        }

        private static String abbreviate(String body) {
            if (body == null) {
                return "";
            }
            String oneLine = body.replace('\n', ' ').trim();
            return oneLine.length() <= 200 ? oneLine : oneLine.substring(0, 200) + "...";
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_THROTTLE_RETRIES = 5;
    private static final Duration DEFAULT_MIN_REQUEST_INTERVAL = Duration.ofMillis(100);

    private final String baseUrl;
    private final String origin;
    private final String basePath;
    private final String authHeader;
    private final HttpClient http;
    private final Duration minRequestInterval;

    private long lastRequestNanos;

    public ConfluenceClient(ConfluenceConnectorConfig config) {
        this(config.baseUrl(), config.email(), config.apiToken());
    }

    public ConfluenceClient(String baseUrl, String email, String apiToken) {
        this(baseUrl, email, apiToken, DEFAULT_MIN_REQUEST_INTERVAL);
    }

    /** {@code minRequestInterval} is overridable for tests (zero disables politeness). */
    public ConfluenceClient(String baseUrl, String email, String apiToken,
            Duration minRequestInterval) {
        String normalized = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
        this.baseUrl = normalized;
        URI uri = URI.create(normalized);
        this.origin = uri.getScheme() + "://" + uri.getAuthority();
        this.basePath = uri.getPath() == null ? "" : uri.getPath();
        String credentials = Objects.requireNonNull(email, "email") + ":"
                + Objects.requireNonNull(apiToken, "apiToken");
        this.authHeader = "Basic "
                + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
        this.minRequestInterval = Objects.requireNonNull(minRequestInterval);
    }

    /** One page of a list response: the body plus the resolved next-page URL, if any. */
    public record ResultPage(JsonNode body, String nextUrl) {
    }

    /**
     * GET a v2 route (e.g. {@code /api/v2/pages}) or an absolute URL taken
     * from a {@code _links.next} cursor, with query parameters.
     */
    public JsonNode get(String pathOrUrl, Map<String, String> query)
            throws IOException, InterruptedException {
        return json(send(request(url(pathOrUrl, query)).GET().build(),
                HttpResponse.BodyHandlers.ofString()));
    }

    /** GET with no query parameters. */
    public JsonNode get(String pathOrUrl) throws IOException, InterruptedException {
        return get(pathOrUrl, Map.of());
    }

    /**
     * GET one page of a cursor-paginated list endpoint. The next-page URL
     * comes from the body's {@code _links.next} (relative against the tenant
     * origin) or, failing that, a {@code Link: <...>; rel="next"} header.
     */
    public ResultPage getPage(String pathOrUrl, Map<String, String> query)
            throws IOException, InterruptedException {
        HttpResponse<String> response = send(request(url(pathOrUrl, query)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode body = json(response);
        String next = nextPageUrl(body)
                .or(() -> nextFromLinkHeader(response).map(this::resolveRelative))
                .orElse(null);
        return new ResultPage(body, next);
    }

    /**
     * Download an attachment binary. {@code downloadUrl} may be the relative
     * {@code downloadLink} of an attachment; it is resolved against the
     * tenant origin.
     */
    public byte[] downloadAttachmentBytes(String downloadUrl)
            throws IOException, InterruptedException {
        HttpResponse<byte[]> response = send(
                request(resolveRelative(downloadUrl)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() >= 400) {
            throw new ConfluenceApiException(response.statusCode(),
                    new String(response.body(), StandardCharsets.UTF_8));
        }
        return response.body();
    }

    /**
     * The absolute URL of the next result page, from the body's
     * {@code _links.next} (relative against the tenant origin).
     *
     * @return the absolute next-page URL, or empty when the page is the last
     */
    public Optional<String> nextPageUrl(JsonNode body) {
        JsonNode next = body.path("_links").path("next");
        if (next.isTextual() && !next.asText().isBlank()) {
            return Optional.of(resolveRelative(next.asText()));
        }
        return Optional.empty();
    }

    /**
     * The next-page URL a {@code Link} response header points at, when the
     * service paginates through headers instead of the body.
     */
    static Optional<String> nextFromLinkHeader(HttpResponse<?> response) {
        return response.headers().firstValue("Link").flatMap(header -> {
            for (String part : header.split(",")) {
                String[] segments = part.split(";");
                if (segments.length == 2 && segments[1].trim().equals("rel=\"next\"")) {
                    String url = segments[0].trim();
                    if (url.startsWith("<") && url.endsWith(">")) {
                        return Optional.of(url.substring(1, url.length() - 1));
                    }
                }
            }
            return Optional.empty();
        });
    }

    private String url(String pathOrUrl, Map<String, String> query) {
        String url = pathOrUrl.startsWith("http") ? pathOrUrl : baseUrl + pathOrUrl;
        if (query == null || query.isEmpty()) {
            return url;
        }
        StringBuilder sb = new StringBuilder(url);
        sb.append(url.contains("?") ? '&' : '?');
        boolean first = true;
        for (Map.Entry<String, String> entry : query.entrySet()) {
            if (!first) {
                sb.append('&');
            }
            sb.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)).append('=')
                    .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
            first = false;
        }
        return sb.toString();
    }

    /**
     * Resolve a server-relative URL. Paths already carrying the base path
     * ({@code /wiki/api/v2/...}, as {@code _links.next} returns) resolve
     * against the origin; bare paths ({@code /download/...}) against the full
     * base URL - the same rule the mapper applies to webui links.
     */
    private String resolveRelative(String url) {
        if (url.startsWith("http")) {
            return url;
        }
        String path = url.startsWith("/") ? url : "/" + url;
        if (!basePath.isEmpty() && path.startsWith(basePath + "/")) {
            return origin + path;
        }
        return baseUrl + path;
    }

    private HttpRequest.Builder request(String url) {
        return HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("authorization", authHeader)
                .header("accept", "application/json");
    }

    /**
     * Sends with Confluence's throttling contract: the politeness gap first,
     * then 429 retried after {@code Retry-After} seconds plus jitter.
     */
    private <T> HttpResponse<T> send(HttpRequest req, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        HttpResponse<T> response = null;
        for (int attempt = 0; attempt <= MAX_THROTTLE_RETRIES; attempt++) {
            throttle();
            response = http.send(req, handler);
            if (response.statusCode() != 429 || attempt == MAX_THROTTLE_RETRIES) {
                return response;
            }
            long waitSeconds = response.headers().firstValueAsLong("Retry-After").orElse(2L);
            long jitterMillis = ThreadLocalRandom.current().nextLong(500);
            Thread.sleep(Duration.ofSeconds(Math.min(waitSeconds, 60)).plusMillis(jitterMillis));
        }
        return response;
    }

    /** Keeps at least {@code minRequestInterval} between requests, sleeping on the caller. */
    private synchronized void throttle() throws InterruptedException {
        if (minRequestInterval.isZero() || minRequestInterval.isNegative()) {
            return;
        }
        long now = System.nanoTime();
        long wait = lastRequestNanos + minRequestInterval.toNanos() - now;
        if (wait > 0) {
            Thread.sleep(Duration.ofNanos(wait));
        }
        lastRequestNanos = System.nanoTime();
    }

    private static JsonNode json(HttpResponse<String> response) throws IOException {
        String body = response.body();
        if (response.statusCode() >= 400) {
            throw new ConfluenceApiException(response.statusCode(), body);
        }
        if (body == null || body.isBlank()) {
            return JSON.createObjectNode();
        }
        return JSON.readTree(body);
    }
}

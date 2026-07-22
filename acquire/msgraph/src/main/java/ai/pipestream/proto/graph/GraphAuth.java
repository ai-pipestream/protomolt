package ai.pipestream.proto.graph;

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
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Microsoft Entra OAuth2 for Graph, with no SDK: the two token flows a headless toolkit
 * needs. <b>Client credentials</b> (an app registration with a secret and application
 * permissions) is the service lane — connector ingestion, scheduled crawls.
 * <b>Device code</b> (a public-client registration, delegated permissions) is the operator
 * lane — sign in as yourself from any terminal, no browser on this machine required.
 *
 * <p>Tokens stay in memory. Persisting a refresh token is credential storage and therefore
 * the operator's deliberate act, never this class's default.</p>
 */
public final class GraphAuth {

    /** Where tokens come from; override {@code authority} only in tests. */
    public record Config(String authority, String tenantId, String clientId,
                         String clientSecret) {

        public Config {
            authority = authority == null || authority.isBlank()
                    ? "https://login.microsoftonline.com" : authority.replaceAll("/+$", "");
            Objects.requireNonNull(tenantId, "tenantId");
            Objects.requireNonNull(clientId, "clientId");
        }

        public static Config delegated(String tenantId, String clientId) {
            return new Config(null, tenantId, clientId, null);
        }

        public static Config application(String tenantId, String clientId, String clientSecret) {
            return new Config(null, tenantId, clientId,
                    Objects.requireNonNull(clientSecret, "clientSecret"));
        }
    }

    /** An issued token; {@code refreshToken} is null in the client-credentials flow. */
    public record Token(String accessToken, Instant expiresAt, String refreshToken) {

        public boolean expired() {
            return Instant.now().isAfter(expiresAt.minusSeconds(60));
        }
    }

    /** What the operator must do to finish a device-code sign-in. */
    public record DeviceCodePrompt(String verificationUri, String userCode, String message) {
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Config config;
    private final HttpClient http;

    public GraphAuth(Config config) {
        this(config, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build());
    }

    GraphAuth(Config config, HttpClient http) {
        this.config = Objects.requireNonNull(config, "config");
        this.http = http;
    }

    /** Application permissions against {@code https://graph.microsoft.com/.default}. */
    public Token clientCredentials() throws IOException, InterruptedException {
        if (config.clientSecret() == null) {
            throw new IllegalStateException("The client-credentials flow needs a clientSecret");
        }
        JsonNode response = postForm(tokenEndpoint(), Map.of(
                "grant_type", "client_credentials",
                "client_id", config.clientId(),
                "client_secret", config.clientSecret(),
                "scope", "https://graph.microsoft.com/.default"));
        return token(response);
    }

    /**
     * Delegated sign-in: surfaces the verification URL and code through {@code prompt},
     * then blocks (polling at the server-directed interval) until the operator approves,
     * the code expires, or the flow is denied.
     */
    public Token deviceCode(String scope, Consumer<DeviceCodePrompt> prompt)
            throws IOException, InterruptedException {
        JsonNode start = postForm(config.authority() + "/" + config.tenantId()
                        + "/oauth2/v2.0/devicecode",
                Map.of("client_id", config.clientId(), "scope", scope));
        if (start.has("error")) {
            throw new IOException("Device-code start failed: " + start.path("error").asText()
                    + " - " + start.path("error_description").asText());
        }
        prompt.accept(new DeviceCodePrompt(
                start.path("verification_uri").asText(),
                start.path("user_code").asText(),
                start.path("message").asText()));

        long intervalSeconds = Math.max(1, start.path("interval").asLong(5));
        Instant deadline = Instant.now().plusSeconds(start.path("expires_in").asLong(900));
        while (Instant.now().isBefore(deadline)) {
            Thread.sleep(Duration.ofSeconds(intervalSeconds));
            JsonNode poll = postForm(tokenEndpoint(), Map.of(
                    "grant_type", "urn:ietf:params:oauth:grant-type:device_code",
                    "client_id", config.clientId(),
                    "device_code", start.path("device_code").asText()));
            String error = poll.path("error").asText("");
            switch (error) {
                case "" -> {
                    return token(poll);
                }
                case "authorization_pending" -> {
                    // keep polling
                }
                case "slow_down" -> intervalSeconds += 5;
                default -> throw new IOException("Device-code sign-in failed: " + error
                        + " - " + poll.path("error_description").asText());
            }
        }
        throw new IOException("Device-code sign-in expired before it was approved");
    }

    /** Exchanges a delegated refresh token for a fresh access token. */
    public Token refresh(String refreshToken, String scope)
            throws IOException, InterruptedException {
        JsonNode response = postForm(tokenEndpoint(), Map.of(
                "grant_type", "refresh_token",
                "client_id", config.clientId(),
                "refresh_token", Objects.requireNonNull(refreshToken, "refreshToken"),
                "scope", scope));
        return token(response);
    }

    private String tokenEndpoint() {
        return config.authority() + "/" + config.tenantId() + "/oauth2/v2.0/token";
    }

    private static Token token(JsonNode response) throws IOException {
        if (response.has("error")) {
            throw new IOException("Token request failed: " + response.path("error").asText()
                    + " - " + response.path("error_description").asText());
        }
        String accessToken = response.path("access_token").asText("");
        if (accessToken.isBlank()) {
            throw new IOException(
                    "Token response carried neither an access_token nor an error field");
        }
        return new Token(
                accessToken,
                Instant.now().plusSeconds(response.path("expires_in").asLong(3600)),
                response.path("refresh_token").isMissingNode()
                        ? null : response.path("refresh_token").asText());
    }

    /** How much of an unusable response body to quote back in the failure message. */
    private static final int ERROR_BODY_EXCERPT = 512;

    /**
     * Posts a form and returns the parsed JSON object.
     *
     * <p>A non-2xx status is not by itself an error: the device-code flow polls the token
     * endpoint and Entra answers {@code 400} carrying {@code authorization_pending} until the
     * operator approves. So a body that parses to an object with an {@code error} field is
     * returned to the caller whatever the status, and only a body that is unusable — blank,
     * not JSON, or not an object — or a non-2xx status with no {@code error} field is raised
     * here. Without that check an empty 5xx body would parse to a missing node and yield a
     * {@link Token} holding an empty access token.
     */
    private JsonNode postForm(String url, Map<String, String> form)
            throws IOException, InterruptedException {
        Map<String, String> ordered = new LinkedHashMap<>(form);
        StringBuilder body = new StringBuilder();
        ordered.forEach((key, value) -> {
            if (!body.isEmpty()) {
                body.append('&');
            }
            body.append(URLEncoder.encode(key, StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        });
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(url))
                        .header("content-type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        JsonNode parsed;
        try {
            parsed = JSON.readTree(response.body());
        } catch (IOException e) {
            throw new IOException(tokenEndpointFailure(url, response, "body was not JSON"), e);
        }
        if (parsed == null || !parsed.isObject()) {
            throw new IOException(
                    tokenEndpointFailure(url, response, "body was not a JSON object"));
        }
        if (response.statusCode() / 100 != 2 && !parsed.has("error")) {
            throw new IOException(
                    tokenEndpointFailure(url, response, "no error field to explain it"));
        }
        return parsed;
    }

    private static String tokenEndpointFailure(
            String url, HttpResponse<String> response, String problem) {
        String body = response.body() == null ? "" : response.body();
        String excerpt = body.length() > ERROR_BODY_EXCERPT
                ? body.substring(0, ERROR_BODY_EXCERPT) + "..." : body;
        return "OAuth request to " + url + " returned HTTP " + response.statusCode()
                + " and " + problem + ": " + excerpt;
    }
}

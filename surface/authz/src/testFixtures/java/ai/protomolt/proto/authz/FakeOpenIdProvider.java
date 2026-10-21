package ai.protomolt.proto.authz;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fake OpenID provider on a plain JDK {@link HttpServer}, for testing the console sign-in
 * without Keycloak: discovery, an authorization endpoint that answers the redirect at once
 * (no login page), a token endpoint that checks the client's basic credentials, the code
 * and the PKCE verifier, and an RFC 7662 introspection endpoint answering canned claims.
 * The ID token is an unsigned JWT ({@code alg: none}), which the console accepts for a token
 * it fetched itself over the back channel. Knobs on the instance steer each failure path.
 */
public final class FakeOpenIdProvider implements AutoCloseable {

    /** One authorization code's state between the authorize and token calls. */
    private record IssuedCode(String nonce, String challenge, String redirectUri) {
    }

    private final HttpServer server;
    private final String clientId;
    private final String clientSecret;
    private final Clock clock;
    private final AtomicInteger sequence = new AtomicInteger();
    private final Map<String, IssuedCode> codes = new ConcurrentHashMap<>();
    private final Map<String, String> introspections = new ConcurrentHashMap<>();

    /** The subject the next login authenticates as. */
    public volatile String subject = "alice-subject";

    /** The introspection answer the next issued access token gets; set per login. */
    public volatile String nextIntrospection =
            "{\"active\": true, \"username\": \"alice\", \"protomolt_scopes\": [\"search-query\"]}";

    /** When set, the authorization endpoint sends the browser back with this error instead of a code. */
    public volatile String denyWith;

    /** When set, the token endpoint answers this status with an empty JSON body. */
    public volatile int tokenEndpointStatus;

    /** When set, overrides the ID token's issuer claim. */
    public volatile String idTokenIssuer;

    /** When set, overrides the ID token's audience claim. */
    public volatile String idTokenAudience;

    /** When set, overrides the ID token's nonce claim. */
    public volatile String idTokenNonce;

    /** The ID token's lifetime; negative makes it already expired. */
    public volatile Duration idTokenLifetime = Duration.ofMinutes(5);

    /** The last access token the token endpoint issued. */
    public volatile String lastAccessToken;

    /** The last verifier the token endpoint received. */
    public volatile String lastVerifier;

    /** The last authorization request's query, decoded. */
    public volatile Map<String, String> lastAuthorization = Map.of();

    public FakeOpenIdProvider(String clientId, String clientSecret, Clock clock) throws IOException {
        this.clientId = Objects.requireNonNull(clientId);
        this.clientSecret = Objects.requireNonNull(clientSecret);
        this.clock = Objects.requireNonNull(clock);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/realm/.well-known/openid-configuration", this::discovery);
        server.createContext("/realm/authorize", this::authorize);
        server.createContext("/realm/token", this::token);
        server.createContext("/realm/introspect", this::introspect);
        server.start();
    }

    /** The issuer this provider answers for. */
    public URI issuer() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/realm");
    }

    /** Settings naming this provider, with {@code redirectUri} and a one hour session. */
    public OidcConsoleSettings settings(URI redirectUri) {
        return new OidcConsoleSettings(issuer(), clientId, clientSecret, redirectUri, Duration.ofHours(1));
    }

    /** Settings naming this provider with a chosen session lifetime. */
    public OidcConsoleSettings settings(URI redirectUri, Duration sessionTtl) {
        return new OidcConsoleSettings(issuer(), clientId, clientSecret, redirectUri, sessionTtl);
    }

    /** Sets the introspection answer of an access token already issued. */
    public void introspectAs(String accessToken, String json) {
        introspections.put(accessToken, json);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void discovery(HttpExchange exchange) throws IOException {
        String base = issuer().toString();
        json(exchange, 200, "{\"issuer\": \"" + base + "\", \"authorization_endpoint\": \"" + base
                + "/authorize\", \"token_endpoint\": \"" + base + "/token\", \"introspection_endpoint\": \""
                + base + "/introspect\"}");
    }

    private void authorize(HttpExchange exchange) throws IOException {
        Map<String, String> query = form(exchange.getRequestURI().getRawQuery());
        lastAuthorization = Map.copyOf(query);
        String redirect = query.get("redirect_uri");
        String state = query.get("state");
        String location;
        if (denyWith != null) {
            location = redirect + "?error=" + encode(denyWith) + "&state=" + encode(state);
        } else {
            String code = "code-" + sequence.incrementAndGet();
            codes.put(code, new IssuedCode(query.get("nonce"), query.get("code_challenge"), redirect));
            location = redirect + "?code=" + encode(code) + "&state=" + encode(state);
        }
        exchange.getResponseHeaders().set("Location", location);
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }

    private void token(HttpExchange exchange) throws IOException {
        if (tokenEndpointStatus != 0) {
            json(exchange, tokenEndpointStatus, "{}");
            return;
        }
        if (!basicAuthorized(exchange)) {
            json(exchange, 401, "{\"error\": \"invalid_client\"}");
            return;
        }
        Map<String, String> form = form(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        IssuedCode issued = codes.remove(form.get("code"));
        lastVerifier = form.get("code_verifier");
        if (issued == null || !"authorization_code".equals(form.get("grant_type"))
                || !issued.redirectUri().equals(form.get("redirect_uri"))
                || !issued.challenge().equals(challenge(lastVerifier))) {
            json(exchange, 400, "{\"error\": \"invalid_grant\"}");
            return;
        }
        String accessToken = "access-" + sequence.incrementAndGet();
        introspections.put(accessToken, nextIntrospection);
        lastAccessToken = accessToken;
        long exp = clock.instant().plus(idTokenLifetime).getEpochSecond();
        String claims = "{\"iss\": \"" + (idTokenIssuer != null ? idTokenIssuer : issuer())
                + "\", \"aud\": \"" + (idTokenAudience != null ? idTokenAudience : clientId)
                + "\", \"sub\": \"" + subject + "\", \"exp\": " + exp
                + ", \"nonce\": \"" + (idTokenNonce != null ? idTokenNonce : issued.nonce()) + "\"}";
        String idToken = base64Url("{\"alg\":\"none\"}") + "." + base64Url(claims) + ".";
        json(exchange, 200, "{\"access_token\": \"" + accessToken + "\", \"token_type\": \"Bearer\","
                + " \"id_token\": \"" + idToken + "\"}");
    }

    private void introspect(HttpExchange exchange) throws IOException {
        if (!basicAuthorized(exchange)) {
            json(exchange, 401, "{\"error\": \"invalid_client\"}");
            return;
        }
        Map<String, String> form = form(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        String answer = introspections.get(form.getOrDefault("token", ""));
        json(exchange, 200, answer == null ? "{\"active\": false}" : answer);
    }

    private boolean basicAuthorized(HttpExchange exchange) {
        String expected = "Basic " + Base64.getEncoder().encodeToString(
                (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
        return expected.equals(exchange.getRequestHeaders().getFirst("Authorization"));
    }

    private static void json(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
        exchange.close();
    }

    /** Decodes a query string or form body. */
    public static Map<String, String> form(String raw) {
        Map<String, String> out = new HashMap<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        for (String pair : List.of(raw.split("&"))) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                out.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    private static String challenge(String verifier) {
        if (verifier == null) {
            return "";
        }
        try {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(
                    MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
    }
}

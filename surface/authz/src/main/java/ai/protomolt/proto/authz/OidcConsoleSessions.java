package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Browser sessions signed in through an OpenID provider, for a console a host serves on
 * its own HTTP port. The login is the authorization code flow with PKCE: {@link #beginLogin}
 * sends the browser to the provider and {@link #completeLogin} turns the callback into a
 * session, whose opaque token rides an HttpOnly cookie. The page never sees a credential
 * and the host never holds the provider's tokens past the login: the access token is
 * introspected once through {@link OidcCallerResolver}, so the session's principal name and
 * scopes are exactly what that token would resolve to presented as a bearer, and the account
 * bindings key on the same name. A session lives for the configured lifetime, fixed at
 * login, or until it is revoked.
 *
 * <p>The provider's endpoints come from its discovery document, fetched once at
 * construction, so a wrong issuer fails the boot. The ID token is validated on its claims
 * (issuer, audience, expiry, nonce) and not on its signature: it arrives over the host's own
 * TLS connection to the token endpoint, where the provider authenticated itself, which the
 * OpenID Connect Core specification accepts in place of the signature for this flow.
 *
 * <p>As a {@link RequestCallerResolver}, a live session's caller is a scoped caller that
 * carries no credential binding. Everything is in memory: a restart signs everyone out.
 */
public final class OidcConsoleSessions implements RequestCallerResolver {

    /** The session cookie's name; the {@code __Host-} prefix pins it to this origin and path {@code /}. */
    public static final String COOKIE = "__Host-protomolt_console_session";

    /** The cookie carrying the pending login's state between redirects. */
    public static final String LOGIN_COOKIE = "__Host-protomolt_console_login";

    /** The console path prefix a login may return to. */
    public static final String RETURN_PREFIX = "/console/";

    /** How long a login may take between leaving for the provider and the callback. */
    static final Duration LOGIN_WINDOW = Duration.ofMinutes(5);

    private static final Logger LOG = LoggerFactory.getLogger(OidcConsoleSessions.class);
    private static final String DISCOVERY_PATH = "/.well-known/openid-configuration";

    /** The provider's endpoints, as its discovery document names them. */
    public record Provider(URI authorizationEndpoint, URI tokenEndpoint, URI introspectionEndpoint) {
        public Provider {
            Objects.requireNonNull(authorizationEndpoint, "authorizationEndpoint");
            Objects.requireNonNull(tokenEndpoint, "tokenEndpoint");
            Objects.requireNonNull(introspectionEndpoint, "introspectionEndpoint");
        }
    }

    /**
     * A login that has left for the provider.
     *
     * @param authorizationUri where to send the browser
     * @param state the pending login's key, which the login cookie carries back
     */
    public record LoginStart(URI authorizationUri, String state) {
    }

    /**
     * The outcome of a callback: a session and where to send the browser, or why not.
     *
     * @param sessionToken the issued session token, or null on failure
     * @param returnPath the console path the login returns to, or null on failure
     * @param failure the reason, {@code SIGN_IN_FAILURE_UNSPECIFIED} on success
     */
    public record LoginResult(String sessionToken, String returnPath, SignInFailure failure) {
        /** Whether a session was issued. */
        public boolean succeeded() {
            return failure == SignInFailure.SIGN_IN_FAILURE_UNSPECIFIED;
        }

        static LoginResult failed(SignInFailure failure) {
            return new LoginResult(null, null, failure);
        }
    }

    private record PendingLogin(String verifier, String nonce, String returnPath, Instant expiresAt) {
    }

    private final OidcConsoleSettings settings;
    private final Provider provider;
    private final CallerResolver callers;
    private final HttpClient http;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, PendingLogin> pending = new ConcurrentHashMap<>();
    private final Map<String, ConsoleSession> sessions = new ConcurrentHashMap<>();

    /**
     * Sessions against the provider {@code settings} names, discovered now.
     *
     * @param settings the explicit sign-in settings
     * @throws IllegalStateException when the issuer's discovery document cannot be fetched
     *         or names another issuer
     */
    public OidcConsoleSessions(OidcConsoleSettings settings) {
        this(settings, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                Clock.systemUTC());
    }

    /** Full-control constructor: discovery over {@code http}, time from {@code clock}. */
    public OidcConsoleSessions(OidcConsoleSettings settings, HttpClient http, Clock clock) {
        this(settings, discover(settings, http), http, clock);
    }

    /** The provider's endpoints given, for a host that already knows them. */
    public OidcConsoleSessions(OidcConsoleSettings settings, Provider provider, HttpClient http,
                               Clock clock) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.provider = Objects.requireNonNull(provider, "provider");
        this.http = Objects.requireNonNull(http, "http");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.callers = new OidcCallerResolver(provider.introspectionEndpoint(), settings.clientId(),
                settings.clientSecret(), http, Duration.ofSeconds(10));
    }

    /**
     * Fetches the issuer's discovery document and reads the three endpoints this flow uses.
     *
     * @param settings the settings naming the issuer
     * @param http the client to fetch with
     * @return the provider's endpoints
     * @throws IllegalStateException when the document cannot be fetched, is not JSON, names
     *         another issuer, or lacks an endpoint
     */
    public static Provider discover(OidcConsoleSettings settings, HttpClient http) {
        String base = settings.issuer().toString();
        URI document = URI.create(base.endsWith("/")
                ? base.substring(0, base.length() - 1) + DISCOVERY_PATH
                : base + DISCOVERY_PATH);
        Struct claims = fetchJson(http, HttpRequest.newBuilder(document)
                .timeout(Duration.ofSeconds(10)).header("Accept", "application/json").GET().build(),
                "OIDC discovery at " + document);
        String issuer = stringClaim(claims, "issuer");
        if (!issuer.equals(settings.issuer().toString())) {
            throw new IllegalStateException("OIDC discovery at " + document + " names issuer '"
                    + issuer + "', not the configured '" + settings.issuer() + "'");
        }
        return new Provider(endpoint(claims, "authorization_endpoint", document),
                endpoint(claims, "token_endpoint", document),
                endpoint(claims, "introspection_endpoint", document));
    }

    /** The settings these sessions were built from. */
    public OidcConsoleSettings settings() {
        return settings;
    }

    /** The provider's endpoints. */
    public Provider provider() {
        return provider;
    }

    /** The session lifetime in seconds, for the cookie's {@code Max-Age}. */
    public long sessionTtlSeconds() {
        return settings.sessionTtl().toSeconds();
    }

    /** The login window in seconds, for the login cookie's {@code Max-Age}. */
    public long loginWindowSeconds() {
        return LOGIN_WINDOW.toSeconds();
    }

    /**
     * Whether {@code path} is a console path a login may return to: it must start with
     * {@value #RETURN_PREFIX}, stay inside this origin, and carry no fragment or control
     * characters.
     *
     * @param path the candidate return path
     * @return true when a login may return to it
     */
    public static boolean acceptableReturnPath(String path) {
        if (path == null || !path.startsWith(RETURN_PREFIX) || path.indexOf('#') >= 0
                || path.indexOf('\\') >= 0 || path.contains("/../") || path.endsWith("/..")) {
            return false;
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c <= ' ' || c > '~') {
                return false;
            }
        }
        return true;
    }

    /**
     * Starts a login that returns to {@code returnPath}.
     *
     * @param returnPath where the browser goes after the callback, an
     *        {@link #acceptableReturnPath acceptable} console path
     * @return where to send the browser, and the state the login cookie carries
     * @throws IllegalArgumentException when the return path is not acceptable
     */
    public LoginStart beginLogin(String returnPath) {
        if (!acceptableReturnPath(returnPath)) {
            throw new IllegalArgumentException("a login returns only to a path under "
                    + RETURN_PREFIX + " on this origin");
        }
        purgeExpired();
        String state = token();
        String nonce = token();
        String verifier = token();
        pending.put(state, new PendingLogin(verifier, nonce, returnPath,
                clock.instant().plus(LOGIN_WINDOW)));
        String query = "response_type=code"
                + "&client_id=" + encode(settings.clientId())
                + "&redirect_uri=" + encode(settings.redirectUri().toString())
                + "&scope=openid"
                + "&state=" + encode(state)
                + "&nonce=" + encode(nonce)
                + "&code_challenge=" + encode(base64Url(sha256(verifier)))
                + "&code_challenge_method=S256";
        String endpoint = provider.authorizationEndpoint().toString();
        return new LoginStart(URI.create(endpoint + (endpoint.indexOf('?') >= 0 ? "&" : "?") + query),
                state);
    }

    /**
     * Completes the login the callback reports. The pending login is consumed whatever the
     * outcome, so a callback replays nothing.
     *
     * @param loginCookie the login cookie's value, or null when the browser sent none
     * @param query the callback's query parameters, decoded
     * @return the session and return path, or the failure
     * @throws IllegalStateException when the provider or the caller store is unreachable or
     *         answers with a failure of its own
     */
    public LoginResult completeLogin(String loginCookie, Map<String, String> query) {
        Objects.requireNonNull(query, "query");
        String state = query.get("state");
        if (state == null || state.isBlank() || loginCookie == null || !state.equals(loginCookie)) {
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_STATE_MISMATCH);
        }
        PendingLogin login = pending.remove(state);
        if (login == null || !login.expiresAt().isAfter(clock.instant())) {
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_LOGIN_EXPIRED);
        }
        String code = query.get("code");
        if (query.containsKey("error") || code == null || code.isBlank()) {
            LOG.warn("console sign-in refused by the provider: {}",
                    query.getOrDefault("error", "no authorization code"));
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_PROVIDER_DENIED);
        }
        Struct tokens = exchange(code, login.verifier());
        if (tokens == null) {
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_TOKEN_EXCHANGE_FAILED);
        }
        String accessToken = stringClaim(tokens, "access_token");
        String idToken = stringClaim(tokens, "id_token");
        if (accessToken.isEmpty() || idToken.isEmpty()) {
            LOG.warn("console sign-in: the token endpoint answered without an access token and an ID token");
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_TOKEN_EXCHANGE_FAILED);
        }
        Struct identity = identityClaims(idToken, login.nonce());
        if (identity == null) {
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_ID_TOKEN_INVALID);
        }
        Optional<Caller> caller = callers.resolve(accessToken);
        if (caller.isEmpty()) {
            LOG.warn("console sign-in: the provider authenticated subject '{}' but introspection"
                    + " resolves its token to no principal with scopes", stringClaim(identity, "sub"));
            return LoginResult.failed(SignInFailure.SIGN_IN_FAILURE_UNKNOWN_PRINCIPAL);
        }
        Instant now = clock.instant();
        ConsoleSession session = requireWellFormed(ConsoleSession.newBuilder()
                .setPrincipal(caller.get().name())
                .addAllScopes(caller.get().scopes().stream().sorted().toList())
                .setIssuer(settings.issuer().toString())
                .setSubject(stringClaim(identity, "sub"))
                .setIssuedAt(timestamp(now))
                .setExpiresAt(timestamp(now.plus(settings.sessionTtl())))
                .build());
        purgeExpired();
        String token = token();
        sessions.put(token, session);
        LOG.info("console session issued to '{}'", session.getPrincipal());
        return new LoginResult(token, login.returnPath(), SignInFailure.SIGN_IN_FAILURE_UNSPECIFIED);
    }

    /**
     * The live session the request's cookie names.
     *
     * @param headers the request's headers
     * @return the session, or empty when the cookie is absent, unknown or expired
     */
    public Optional<ConsoleSession> session(RequestHeaders headers) {
        String token = cookie(headers, COOKIE);
        if (token == null) {
            return Optional.empty();
        }
        ConsoleSession session = sessions.get(token);
        if (session == null) {
            return Optional.empty();
        }
        if (!expiresAt(session).isAfter(clock.instant())) {
            sessions.remove(token);
            return Optional.empty();
        }
        return Optional.of(session);
    }

    @Override
    public Optional<AuthenticatedCaller> resolve(RequestHeaders headers) {
        return session(headers).map(session -> AuthenticatedCaller.unbound(
                Caller.scoped(session.getPrincipal(), Set.copyOf(session.getScopesList()))));
    }

    /**
     * Drops the session the request's cookie names, if any.
     *
     * @param headers the request's headers
     * @return the principal signed out, or empty when no live session was named
     */
    public Optional<String> revoke(RequestHeaders headers) {
        String token = cookie(headers, COOKIE);
        ConsoleSession removed = token == null ? null : sessions.remove(token);
        return Optional.ofNullable(removed).map(ConsoleSession::getPrincipal);
    }

    /**
     * The value of the cookie {@code name} among the request's {@code Cookie} headers.
     *
     * @param headers the request's headers
     * @param name the cookie's name
     * @return its value, or null when absent
     */
    public static String cookie(RequestHeaders headers, String name) {
        for (String header : headers.values("Cookie")) {
            int from = 0;
            while (from <= header.length()) {
                int semicolon = header.indexOf(';', from);
                String value = (semicolon < 0 ? header.substring(from) : header.substring(from, semicolon)).trim();
                int equals = value.indexOf('=');
                if (equals > 0 && name.equals(value.substring(0, equals))) {
                    return value.substring(equals + 1);
                }
                if (semicolon < 0) {
                    break;
                }
                from = semicolon + 1;
            }
        }
        return null;
    }

    /** The number of live sessions, for tests and operators. */
    public int liveSessions() {
        purgeExpired();
        return sessions.size();
    }

    // ------------------------------------------------------------------ the provider

    /** The token endpoint's answer, or null when it refused the code. */
    private Struct exchange(String code, String verifier) {
        String body = "grant_type=authorization_code"
                + "&code=" + encode(code)
                + "&redirect_uri=" + encode(settings.redirectUri().toString())
                + "&client_id=" + encode(settings.clientId())
                + "&code_verifier=" + encode(verifier);
        HttpRequest request = HttpRequest.newBuilder(provider.tokenEndpoint())
                .timeout(Duration.ofSeconds(10))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(
                        (settings.clientId() + ":" + settings.clientSecret()).getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = send(http, request, "the token endpoint");
        if (response.statusCode() == 400 || response.statusCode() == 401) {
            // RFC 6749 section 5.2: the provider refused this code or client; the login fails,
            // the provider is fine.
            LOG.warn("console sign-in: the token endpoint refused the code with HTTP {}",
                    response.statusCode());
            return null;
        }
        if (response.statusCode() != 200) {
            throw new IllegalStateException("the token endpoint answered HTTP " + response.statusCode());
        }
        return parseJson(response.body(), "the token endpoint");
    }

    /** The ID token's claims when it is for this client, from the issuer, unexpired and carries {@code nonce}. */
    private Struct identityClaims(String idToken, String nonce) {
        int header = idToken.indexOf('.');
        int signature = header < 0 ? -1 : idToken.indexOf('.', header + 1);
        if (header < 0 || signature < 0 || idToken.indexOf('.', signature + 1) >= 0) {
            LOG.warn("console sign-in: the ID token is not a three-part JWT");
            return null;
        }
        Struct claims;
        try {
            claims = parseJson(new String(Base64.getUrlDecoder().decode(
                    idToken.substring(header + 1, signature)), StandardCharsets.UTF_8),
                    "the ID token");
        } catch (IllegalArgumentException | IllegalStateException e) {
            LOG.warn("console sign-in: the ID token payload does not decode: {}", e.getMessage());
            return null;
        }
        String issuer = stringClaim(claims, "iss");
        if (!issuer.equals(settings.issuer().toString())) {
            LOG.warn("console sign-in: the ID token names issuer '{}', not '{}'", issuer, settings.issuer());
            return null;
        }
        if (!audience(claims).contains(settings.clientId())) {
            LOG.warn("console sign-in: the ID token is not for client '{}'", settings.clientId());
            return null;
        }
        Value exp = claims.getFieldsMap().get("exp");
        if (exp == null || !exp.hasNumberValue()
                || !Instant.ofEpochSecond((long) exp.getNumberValue()).isAfter(clock.instant())) {
            LOG.warn("console sign-in: the ID token has expired or carries no expiry");
            return null;
        }
        if (!nonce.equals(stringClaim(claims, "nonce"))) {
            LOG.warn("console sign-in: the ID token's nonce is not this login's");
            return null;
        }
        if (stringClaim(claims, "sub").isEmpty()) {
            LOG.warn("console sign-in: the ID token names no subject");
            return null;
        }
        return claims;
    }

    private static Set<String> audience(Struct claims) {
        Value aud = claims.getFieldsMap().get("aud");
        if (aud == null) {
            return Set.of();
        }
        if (aud.hasListValue()) {
            return aud.getListValue().getValuesList().stream()
                    .map(Value::getStringValue).collect(Collectors.toSet());
        }
        return Set.of(aud.getStringValue());
    }

    private static Struct fetchJson(HttpClient http, HttpRequest request, String what) {
        HttpResponse<String> response = send(http, request, what);
        if (response.statusCode() != 200) {
            throw new IllegalStateException(what + " answered HTTP " + response.statusCode());
        }
        return parseJson(response.body(), what);
    }

    private static HttpResponse<String> send(HttpClient http, HttpRequest request, String what) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException(what + " is unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(what + " request interrupted", e);
        }
    }

    private static Struct parseJson(String body, String what) {
        Struct.Builder claims = Struct.newBuilder();
        try {
            JsonFormat.parser().merge(body, claims);
        } catch (IOException e) {
            throw new IllegalStateException(what + " answered with invalid JSON: " + e.getMessage(), e);
        }
        return claims.build();
    }

    private static URI endpoint(Struct claims, String name, URI document) {
        String value = stringClaim(claims, name);
        if (value.isEmpty()) {
            throw new IllegalStateException("OIDC discovery at " + document + " names no " + name);
        }
        try {
            URI endpoint = URI.create(value);
            if (!endpoint.isAbsolute()) {
                throw new IllegalArgumentException("not absolute");
            }
            return endpoint;
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("OIDC discovery at " + document + " names an unusable "
                    + name + ": " + value, e);
        }
    }

    private static String stringClaim(Struct claims, String name) {
        Value value = claims.getFieldsMap().get(name);
        return value == null ? "" : value.getStringValue().trim();
    }

    // ------------------------------------------------------------------ sessions

    private static ConsoleSession requireWellFormed(ConsoleSession session) {
        ValidationResult rules = ProtoValidator.create().validate(session);
        if (!rules.valid()) {
            throw new IllegalStateException("console session is invalid: " + rules.violations().stream()
                    .map(violation -> violation.path() + ": " + violation.message())
                    .collect(Collectors.joining("; ")));
        }
        return session;
    }

    private void purgeExpired() {
        Instant now = clock.instant();
        pending.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
        sessions.entrySet().removeIf(entry -> !expiresAt(entry.getValue()).isAfter(now));
    }

    private static Instant expiresAt(ConsoleSession session) {
        Timestamp at = session.getExpiresAt();
        return Instant.ofEpochSecond(at.getSeconds(), at.getNanos());
    }

    private static Timestamp timestamp(Instant at) {
        return Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build();
    }

    private String token() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return base64Url(bytes);
    }

    private static String base64Url(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.US_ASCII));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}

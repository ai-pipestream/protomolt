package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.ConsoleSession;
import ai.protomolt.proto.authz.ConsoleSessionStatus;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.authz.OidcConsoleSessions.LoginResult;
import ai.protomolt.proto.authz.OidcConsoleSessions.LoginStart;
import ai.protomolt.proto.authz.OidcConsoleSettings;
import ai.protomolt.proto.authz.RequestHeaders;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The console's sign-in routes on the repository host's HTTP port, under {@link #PATH}:
 * <ul>
 *   <li>{@code GET /console/session/login?return=<path>} sends the browser to the provider
 *   and sets the login cookie; a return path outside the console is refused with 400.</li>
 *   <li>{@code GET /console/session/callback} is the provider's redirect target: on success
 *   it sets the session cookie and sends the browser to the return path, on failure to the
 *   console's sign-in page naming the reason.</li>
 *   <li>{@code GET /console/session} answers {@link ConsoleSessionStatus} as binary protobuf,
 *   200 with a live session and 401 without one.</li>
 *   <li>{@code POST /console/session/logout} revokes the session and clears the cookie.</li>
 * </ul>
 * Both cookies are HttpOnly and Secure: the session cookie is SameSite=Strict, so only
 * this origin's own requests carry it; the login cookie is SameSite=Lax, because the
 * provider's redirect back is a cross-site navigation and a Strict cookie would not ride it.
 *
 * <p>A host without sign-in settings mounts {@link #notConfigured()}, which answers every
 * route with 503 naming the variables.
 */
public final class ConsoleSignInHandler implements HttpHandler {

    /** The route prefix; more specific than the console's static route, so it wins. */
    public static final String PATH = "/console/session";

    /** The console's sign-in page, where a failed login lands with its reason. */
    public static final String SIGN_IN_PAGE = "/console/sign-in";

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSignInHandler.class);
    private static final String LOGIN = PATH + "/login";
    private static final String CALLBACK = PATH + "/callback";
    private static final String LOGOUT = PATH + "/logout";
    private static final String PROTOBUF = "application/proto";

    private final OidcConsoleSessions sessions;
    private final Function<AuthenticatedCaller, List<String>> boundAccounts;
    private final Function<AuthenticatedCaller, List<IntakeWriterScope>> boundWriters;

    private ConsoleSignInHandler(OidcConsoleSessions sessions,
                                 Function<AuthenticatedCaller, List<String>> boundAccounts,
                                 Function<AuthenticatedCaller, List<IntakeWriterScope>> boundWriters) {
        this.sessions = sessions;
        this.boundAccounts = boundAccounts;
        this.boundWriters = boundWriters;
    }

    /**
     * The routes over {@code sessions}, for a host whose sessions only read: the status
     * answer lists no intake writers.
     *
     * @param sessions the session store
     * @param boundAccounts the accounts a session's caller is bound to, for the status answer
     * @return the handler
     */
    public static ConsoleSignInHandler over(OidcConsoleSessions sessions,
                                            Function<AuthenticatedCaller, List<String>> boundAccounts) {
        return over(sessions, boundAccounts, caller -> List.of());
    }

    /**
     * The routes over {@code sessions}.
     *
     * @param sessions the session store
     * @param boundAccounts the accounts a session's caller is bound to, for the status answer
     * @param boundWriters the intake owners a session's caller may save as, for the status answer
     * @return the handler
     */
    public static ConsoleSignInHandler over(OidcConsoleSessions sessions,
                                            Function<AuthenticatedCaller, List<String>> boundAccounts,
                                            Function<AuthenticatedCaller, List<IntakeWriterScope>> boundWriters) {
        return new ConsoleSignInHandler(Objects.requireNonNull(sessions, "sessions"),
                Objects.requireNonNull(boundAccounts, "boundAccounts"),
                Objects.requireNonNull(boundWriters, "boundWriters"));
    }

    /**
     * A host without sign-in settings: every route answers 503 naming the variables.
     *
     * @return the handler
     */
    public static ConsoleSignInHandler notConfigured() {
        return new ConsoleSignInHandler(null, null, null);
    }

    /** Whether sign-in is configured on this host. */
    public boolean configured() {
        return sessions != null;
    }

    /** The session store, as the gateway's request-to-caller seam; empty when not configured. */
    public Optional<OidcConsoleSessions> sessions() {
        return Optional.ofNullable(sessions);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            if (sessions == null) {
                text(exchange, 503, "console sign-in is not configured: set "
                        + OidcConsoleSettings.ENV_ISSUER + ", " + OidcConsoleSettings.ENV_CLIENT_ID + ", "
                        + OidcConsoleSettings.ENV_CLIENT_SECRET + ", " + OidcConsoleSettings.ENV_REDIRECT_URI
                        + " and " + OidcConsoleSettings.ENV_SESSION_TTL);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
            try {
                switch (path) {
                    case PATH -> {
                        if (!"GET".equals(method)) {
                            exchange.sendResponseHeaders(405, -1);
                            return;
                        }
                        status(exchange);
                    }
                    case LOGIN -> {
                        if (!"GET".equals(method)) {
                            exchange.sendResponseHeaders(405, -1);
                            return;
                        }
                        login(exchange);
                    }
                    case CALLBACK -> {
                        if (!"GET".equals(method)) {
                            exchange.sendResponseHeaders(405, -1);
                            return;
                        }
                        callback(exchange);
                    }
                    case LOGOUT -> {
                        if (!"POST".equals(method)) {
                            exchange.sendResponseHeaders(405, -1);
                            return;
                        }
                        logout(exchange);
                    }
                    default -> exchange.sendResponseHeaders(404, -1);
                }
            } catch (IllegalStateException e) {
                // The provider or the caller store is down: an outage, not a refusal.
                LOG.warn("console sign-in unavailable: {}", e.getMessage());
                text(exchange, 503, "console sign-in unavailable: " + e.getMessage());
            }
        }
    }

    private void status(HttpExchange exchange) throws IOException {
        RequestHeaders headers = headers(exchange);
        Optional<ConsoleSession> session = sessions.session(headers);
        ConsoleSessionStatus.Builder status = ConsoleSessionStatus.newBuilder()
                .setAuthenticated(session.isPresent());
        if (session.isPresent()) {
            AuthenticatedCaller caller = sessions.resolve(headers).orElseThrow();
            status.setPrincipal(session.get().getPrincipal())
                    .addAllAccountIds(boundAccounts.apply(caller))
                    .addAllIntakeWriters(boundWriters.apply(caller))
                    .setExpiresAt(session.get().getExpiresAt());
        }
        byte[] body = status.build().toByteArray();
        exchange.getResponseHeaders().set("Content-Type", PROTOBUF);
        exchange.sendResponseHeaders(session.isPresent() ? 200 : 401, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private void login(HttpExchange exchange) throws IOException {
        String returnPath = query(exchange).get("return");
        if (!OidcConsoleSessions.acceptableReturnPath(returnPath)) {
            text(exchange, 400, "return must be a console path under "
                    + OidcConsoleSessions.RETURN_PREFIX + " on this origin");
            return;
        }
        LoginStart start = sessions.beginLogin(returnPath);
        exchange.getResponseHeaders().add("Set-Cookie", OidcConsoleSessions.LOGIN_COOKIE + "="
                + start.state() + "; Path=/; Max-Age=" + sessions.loginWindowSeconds()
                + "; HttpOnly; Secure; SameSite=Lax");
        redirect(exchange, start.authorizationUri().toString());
    }

    private void callback(HttpExchange exchange) throws IOException {
        RequestHeaders headers = headers(exchange);
        LoginResult result = sessions.completeLogin(
                OidcConsoleSessions.cookie(headers, OidcConsoleSessions.LOGIN_COOKIE), query(exchange));
        exchange.getResponseHeaders().add("Set-Cookie", clearing(OidcConsoleSessions.LOGIN_COOKIE));
        if (!result.succeeded()) {
            redirect(exchange, SIGN_IN_PAGE + "?error="
                    + URLEncoder.encode(result.failure().name(), StandardCharsets.UTF_8));
            return;
        }
        exchange.getResponseHeaders().add("Set-Cookie", OidcConsoleSessions.COOKIE + "="
                + result.sessionToken() + "; Path=/; Max-Age=" + sessions.sessionTtlSeconds()
                + "; HttpOnly; Secure; SameSite=Strict");
        redirect(exchange, result.returnPath());
    }

    private void logout(HttpExchange exchange) throws IOException {
        sessions.revoke(headers(exchange)).ifPresent(principal ->
                LOG.info("console session of '{}' signed out", principal));
        exchange.getResponseHeaders().add("Set-Cookie", clearing(OidcConsoleSessions.COOKIE));
        exchange.sendResponseHeaders(204, -1);
    }

    private static String clearing(String cookie) {
        return cookie + "=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict";
    }

    private static void redirect(HttpExchange exchange, String location) throws IOException {
        exchange.getResponseHeaders().set("Location", location);
        exchange.sendResponseHeaders(302, -1);
    }

    private static void text(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = message.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    /** The exchange's headers behind the authz seam. */
    static RequestHeaders headers(HttpExchange exchange) {
        return RequestHeaders.of(exchange.getRequestHeaders());
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> out = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        int from = 0;
        while (from <= raw.length()) {
            int amp = raw.indexOf('&', from);
            String pair = amp < 0 ? raw.substring(from) : raw.substring(from, amp);
            int eq = pair.indexOf('=');
            if (eq > 0) {
                out.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
            if (amp < 0) {
                break;
            }
            from = amp + 1;
        }
        return out;
    }
}

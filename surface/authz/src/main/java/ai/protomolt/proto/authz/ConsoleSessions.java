package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import com.sun.net.httpserver.HttpExchange;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scoped browser sessions for a console page: a credential logs in once and an HttpOnly
 * cookie carries the resolved {@link Caller} for the session's lifetime, so the page never
 * holds a credential and never holds the operator token. A login is one of two things — a
 * console's own login token, bound to a fixed bounded identity, or a credential an access
 * policy names, bound to that principal — and everything else never gets a session. The
 * presented credential is retained with the session so a console that proxies onto sibling
 * guarded surfaces can present it there, keeping those surfaces the authority over what the
 * session may do.
 *
 * <p>The open form serves consoles on trusted-network nodes: no login, and every request
 * runs with process authority, exactly the pre-session behavior.
 */
public final class ConsoleSessions {

    private record Session(Caller caller, String credential, Instant expiresAt) {
    }

    private final String cookieName;
    private final boolean secured;
    private final byte[] loginDigest;
    private final Caller loginTokenCaller;
    private final Duration ttl;
    private final Clock clock;
    private final SecureRandom random;
    private final CallerResolver resolver;
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    private ConsoleSessions(String cookieName, boolean secured, byte[] loginDigest,
                            Caller loginTokenCaller, Duration ttl, Clock clock,
                            CallerResolver resolver) {
        if (cookieName == null || cookieName.isBlank()) {
            throw new IllegalArgumentException("cookie name must not be blank");
        }
        this.cookieName = cookieName;
        this.secured = secured;
        this.loginDigest = loginDigest;
        this.loginTokenCaller = loginTokenCaller;
        this.ttl = ttl;
        this.clock = clock;
        this.random = new SecureRandom();
        this.resolver = resolver;
    }

    /** The open console: no login, process authority. */
    public static ConsoleSessions open(String cookieName) {
        return new ConsoleSessions(cookieName, false, null, null, Duration.ZERO,
                Clock.systemUTC(), null);
    }

    /** Sessions for access-policy principals only: no console login token. */
    public static ConsoleSessions secured(String cookieName, Duration ttl,
                                          CallerResolver resolver) {
        Objects.requireNonNull(resolver, "caller resolver");
        validateTtl(ttl);
        return new ConsoleSessions(cookieName, true, null, null, ttl,
                Clock.systemUTC(), resolver);
    }

    /**
     * Sessions with a console login token bound to {@code loginTokenCaller} — a bounded
     * identity, never the operator — beside any principals {@code resolver} (nullable)
     * names.
     */
    public static ConsoleSessions secured(String cookieName, Duration ttl,
                                          CallerResolver resolver, String loginToken,
                                          Caller loginTokenCaller) {
        validateSettings(loginToken, ttl);
        Objects.requireNonNull(loginTokenCaller, "login token caller");
        if (loginTokenCaller.unrestricted()) {
            throw new IllegalArgumentException(
                    "a console login token must never bind to the operator");
        }
        return new ConsoleSessions(cookieName, true, digest(loginToken), loginTokenCaller,
                ttl, Clock.systemUTC(), resolver);
    }

    /** Validates a console login token and session ttl, for refusing at option parsing. */
    public static void validateSettings(String loginToken, Duration ttl) {
        Objects.requireNonNull(loginToken, "console login token");
        if (loginToken.length() < 32 || loginToken.length() > 1024) {
            throw new IllegalArgumentException(
                    "console login token must contain 32 to 1024 characters");
        }
        validateTtl(ttl);
    }

    private static void validateTtl(Duration ttl) {
        Objects.requireNonNull(ttl, "console session ttl");
        if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(Duration.ofDays(7)) > 0) {
            throw new IllegalArgumentException(
                    "console session ttl must be between one second and seven days");
        }
    }

    /** The session cookie's name. */
    public String cookieName() {
        return cookieName;
    }

    /** Whether this console demands a login before serving. */
    public boolean requiresLogin() {
        return secured;
    }

    /** The caller {@code candidate} logs in as, or null when it is unknown. */
    public Caller loginCaller(String candidate) {
        if (!secured || candidate == null || candidate.isBlank()) {
            return null;
        }
        if (loginDigest != null && MessageDigest.isEqual(loginDigest, digest(candidate))) {
            return loginTokenCaller;
        }
        return resolver == null ? null : resolver.resolve(candidate).orElse(null);
    }

    /** Issues a session bound to {@code caller}, retaining the credential it presented. */
    public String issue(Caller caller, String credential) {
        if (!secured) {
            throw new IllegalStateException("an open console does not issue sessions");
        }
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(credential, "credential");
        purgeExpired();
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String token = encoder.encodeToString(bytes);
        sessions.put(token, new Session(caller, credential, clock.instant().plus(ttl)));
        return token;
    }

    /** Whether {@code exchange} carries a live session (an open console always does). */
    public boolean authorized(HttpExchange exchange) {
        return caller(exchange).isPresent();
    }

    /** The session's caller; the open console runs with process authority. */
    public Optional<Caller> caller(HttpExchange exchange) {
        if (!secured) {
            return Optional.of(Caller.operator());
        }
        return session(exchange).map(Session::caller);
    }

    /** The credential the session logged in with; empty on the open console. */
    public Optional<String> credential(HttpExchange exchange) {
        return session(exchange).map(Session::credential);
    }

    /** Drops the session {@code exchange} carries, if any. */
    public void revoke(HttpExchange exchange) {
        String token = cookie(exchange);
        if (token != null) {
            sessions.remove(token);
        }
    }

    /** The session lifetime in seconds, for the cookie's {@code Max-Age}. */
    public long maxAgeSeconds() {
        return ttl.toSeconds();
    }

    private Optional<Session> session(HttpExchange exchange) {
        if (!secured) {
            return Optional.empty();
        }
        purgeExpired();
        String token = cookie(exchange);
        Session session = token == null ? null : sessions.get(token);
        if (session == null || !session.expiresAt().isAfter(clock.instant())) {
            return Optional.empty();
        }
        return Optional.of(session);
    }

    private void purgeExpired() {
        Instant now = clock.instant();
        sessions.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
    }

    private String cookie(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Cookie");
        if (header == null) {
            return null;
        }
        for (String part : header.split(";")) {
            String value = part.trim();
            int equals = value.indexOf('=');
            if (equals > 0 && cookieName.equals(value.substring(0, equals))) {
                return value.substring(equals + 1);
            }
        }
        return null;
    }

    private static byte[] digest(String value) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}

package ai.pipestream.proto.serve;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import com.sun.net.httpserver.HttpExchange;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Scoped browser sessions for the task console. */
final class TaskConsoleSessions {

    static final String COOKIE = "__Host-protomolt_task_session";

    /** The console login token's identity: task steering, never the operator. */
    static final Caller CONSOLE = Caller.scoped("task-console",
            Set.of(Scopes.WORKER_COORDINATE));

    private record Session(Caller caller, Instant expiresAt) {
    }

    private final byte[] loginDigest;
    private final Duration ttl;
    private final Clock clock;
    private final SecureRandom random;
    private final CallerResolver resolver;
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    private TaskConsoleSessions(byte[] loginDigest, Duration ttl, Clock clock,
                                SecureRandom random, CallerResolver resolver) {
        this.loginDigest = loginDigest;
        this.ttl = ttl;
        this.clock = clock;
        this.random = random;
        this.resolver = resolver;
    }

    static TaskConsoleSessions open() {
        return new TaskConsoleSessions(null, Duration.ZERO, Clock.systemUTC(),
                new SecureRandom(), null);
    }

    static TaskConsoleSessions secured(String loginToken, Duration ttl) {
        return secured(loginToken, ttl, null);
    }

    /**
     * With a resolver, a credential the access policy names also logs in, and the session
     * is bound to that principal for its whole lifetime; the console login token binds to
     * {@link #CONSOLE}, never to the operator.
     */
    static TaskConsoleSessions secured(String loginToken, Duration ttl,
                                       CallerResolver resolver) {
        validateSettings(loginToken, ttl);
        return new TaskConsoleSessions(digest(loginToken), ttl, Clock.systemUTC(),
                new SecureRandom(), resolver);
    }

    static void validateSettings(String loginToken, Duration ttl) {
        Objects.requireNonNull(loginToken, "task console login token");
        Objects.requireNonNull(ttl, "task console session ttl");
        if (loginToken.length() < 32 || loginToken.length() > 1024) {
            throw new IllegalArgumentException(
                    "task console login token must contain 32 to 1024 characters");
        }
        if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(Duration.ofDays(7)) > 0) {
            throw new IllegalArgumentException(
                    "task console session ttl must be between one second and seven days");
        }
    }

    boolean requiresLogin() {
        return loginDigest != null;
    }

    /** The caller {@code candidate} logs in as, or null when it is unknown. */
    Caller loginCaller(String candidate) {
        if (!requiresLogin() || candidate == null || candidate.isBlank()) {
            return null;
        }
        if (MessageDigest.isEqual(loginDigest, digest(candidate))) {
            return CONSOLE;
        }
        return resolver == null ? null : resolver.resolve(candidate).orElse(null);
    }

    String issue(Caller caller) {
        if (!requiresLogin()) {
            throw new IllegalStateException("open task console does not issue sessions");
        }
        Objects.requireNonNull(caller, "caller");
        purgeExpired();
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        sessions.put(token, new Session(caller, clock.instant().plus(ttl)));
        return token;
    }

    boolean authorized(HttpExchange exchange) {
        return caller(exchange).isPresent();
    }

    /** The session's caller; the open console runs with process authority. */
    Optional<Caller> caller(HttpExchange exchange) {
        if (!requiresLogin()) {
            return Optional.of(Caller.operator());
        }
        purgeExpired();
        String token = cookie(exchange, COOKIE);
        Session session = token == null ? null : sessions.get(token);
        if (session == null || !session.expiresAt().isAfter(clock.instant())) {
            return Optional.empty();
        }
        return Optional.of(session.caller());
    }

    void revoke(HttpExchange exchange) {
        String token = cookie(exchange, COOKIE);
        if (token != null) {
            sessions.remove(token);
        }
    }

    long maxAgeSeconds() {
        return ttl.toSeconds();
    }

    private void purgeExpired() {
        Instant now = clock.instant();
        sessions.entrySet().removeIf(entry -> !entry.getValue().expiresAt().isAfter(now));
    }

    private static String cookie(HttpExchange exchange, String name) {
        String header = exchange.getRequestHeaders().getFirst("Cookie");
        if (header == null) {
            return null;
        }
        for (String part : header.split(";")) {
            String value = part.trim();
            int equals = value.indexOf('=');
            if (equals > 0 && name.equals(value.substring(0, equals))) {
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

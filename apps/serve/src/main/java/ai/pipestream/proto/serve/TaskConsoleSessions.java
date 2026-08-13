package ai.pipestream.proto.serve;

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
import java.util.concurrent.ConcurrentHashMap;

/** Scoped browser sessions for the task console. */
final class TaskConsoleSessions {

    static final String COOKIE = "__Host-protomolt_task_session";

    private final byte[] loginDigest;
    private final Duration ttl;
    private final Clock clock;
    private final SecureRandom random;
    private final Map<String, Instant> sessions = new ConcurrentHashMap<>();

    private TaskConsoleSessions(byte[] loginDigest, Duration ttl, Clock clock,
                                SecureRandom random) {
        this.loginDigest = loginDigest;
        this.ttl = ttl;
        this.clock = clock;
        this.random = random;
    }

    static TaskConsoleSessions open() {
        return new TaskConsoleSessions(null, Duration.ZERO, Clock.systemUTC(),
                new SecureRandom());
    }

    static TaskConsoleSessions secured(String loginToken, Duration ttl) {
        validateSettings(loginToken, ttl);
        return new TaskConsoleSessions(digest(loginToken), ttl, Clock.systemUTC(),
                new SecureRandom());
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

    boolean acceptsLogin(String candidate) {
        return requiresLogin() && candidate != null
                && MessageDigest.isEqual(loginDigest, digest(candidate));
    }

    String issue() {
        if (!requiresLogin()) {
            throw new IllegalStateException("open task console does not issue sessions");
        }
        purgeExpired();
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        sessions.put(token, clock.instant().plus(ttl));
        return token;
    }

    boolean authorized(HttpExchange exchange) {
        if (!requiresLogin()) {
            return true;
        }
        purgeExpired();
        String token = cookie(exchange, COOKIE);
        Instant expiresAt = token == null ? null : sessions.get(token);
        return expiresAt != null && expiresAt.isAfter(clock.instant());
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
        sessions.entrySet().removeIf(entry -> !entry.getValue().isAfter(now));
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

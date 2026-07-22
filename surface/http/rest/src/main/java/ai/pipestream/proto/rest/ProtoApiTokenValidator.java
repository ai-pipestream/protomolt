package ai.pipestream.proto.rest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Pluggable API-token check used by {@link ProtoRestGateway}.
 * Framework glue (Quarkus/Spring) or a simple shared-secret validator can implement this.
 */
@FunctionalInterface
public interface ProtoApiTokenValidator {

    /**
     * @param tokenConfig scheme describing how the token is expected
     * @param headers lowercase header name → value (may be empty)
     * @param queryParams query name → value (may be empty)
     * @return empty if valid; otherwise a reason string
     */
    Optional<String> validate(
            ApiTokenRequirement tokenConfig,
            Map<String, String> headers,
            Map<String, String> queryParams);

    /**
     * Rejects every token-protected method. This is the fail-closed default used by
     * {@link ProtoRestGateway#ProtoRestGateway(ProtoRestMethodRegistry, ai.pipestream.proto.json.ProtobufJsonTranscoder)}
     * when no validator is supplied: methods carrying a required token always get 401
     * until the integrator wires a real validator (for example {@link #sharedSecret(String)}).
     */
    static ProtoApiTokenValidator denyAll() {
        return (tokenConfig, headers, queryParams) -> Optional.of(
                "No API token validator configured; supply a ProtoApiTokenValidator to accept token '"
                        + tokenConfig.name() + "'");
    }

    /**
     * Accepts any non-blank token in the configured location.
     *
     * <p><strong>WARNING:</strong> this performs no verification whatsoever; any junk value
     * passes. It exists only for dev / demo / open gateways where the token requirement is
     * documentation, not security. Never use it in production; prefer
     * {@link #sharedSecret(String)} or a framework-backed validator.
     */
    static ProtoApiTokenValidator acceptNonBlank() {
        return (tokenConfig, headers, queryParams) -> {
            String value = extract(tokenConfig, headers, queryParams);
            if (value == null || value.isBlank()) {
                return Optional.of("Missing API token '" + tokenConfig.name() + "'");
            }
            return Optional.empty();
        };
    }

    /**
     * Requires an exact match against {@code expectedToken} (constant-time comparison).
     */
    static ProtoApiTokenValidator sharedSecret(String expectedToken) {
        Objects.requireNonNull(expectedToken, "expectedToken");
        return (tokenConfig, headers, queryParams) -> {
            String value = extract(tokenConfig, headers, queryParams);
            if (value == null || value.isBlank()) {
                return Optional.of("Missing API token '" + tokenConfig.name() + "'");
            }
            if (tokenConfig.scheme() == ProtoApiToken.Scheme.HTTP
                    && value.regionMatches(true, 0, "Bearer ", 0, 7)) {
                value = value.substring(7).trim();
            }
            if (!MessageDigest.isEqual(
                    expectedToken.getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8))) {
                return Optional.of("Invalid API token");
            }
            return Optional.empty();
        };
    }

    private static String extract(
            ApiTokenRequirement tokenConfig,
            Map<String, String> headers,
            Map<String, String> queryParams) {
        return switch (tokenConfig.in()) {
            case HEADER -> headers.getOrDefault(tokenConfig.name().toLowerCase(Locale.ROOT), null);
            case QUERY -> queryParams.get(tokenConfig.name());
            case COOKIE -> cookieValue(headers.get("cookie"), tokenConfig.name());
        };
    }

    private static String cookieValue(String cookieHeader, String cookieName) {
        if (cookieHeader == null) {
            return null;
        }
        for (String pair : cookieHeader.split(";")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            if (pair.substring(0, eq).trim().equals(cookieName)) {
                return pair.substring(eq + 1).trim();
            }
        }
        return null;
    }
}

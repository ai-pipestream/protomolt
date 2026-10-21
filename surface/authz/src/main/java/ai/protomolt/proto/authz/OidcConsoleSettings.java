package ai.protomolt.proto.authz;

import java.net.URI;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The console's OIDC sign-in settings, every one explicit. The environment names them
 * all or none: none means the console has no sign-in, and any subset short of all five
 * refuses at boot naming what is missing, because a half-configured login is a
 * misconfiguration and not a default.
 *
 * @param issuer the OpenID provider's issuer, discovered at
 *        {@code /.well-known/openid-configuration}; the ID token must name it
 * @param clientId this console's client id at the provider
 * @param clientSecret this console's client secret
 * @param redirectUri the callback route as the provider reaches it, registered at the
 *        provider exactly
 * @param sessionTtl how long a session authenticates after login, one second to seven days
 */
public record OidcConsoleSettings(URI issuer, String clientId, String clientSecret,
                                  URI redirectUri, Duration sessionTtl) {

    /** Environment variable naming the issuer: {@value}. */
    public static final String ENV_ISSUER = "PROTOMOLT_CONSOLE_OIDC_ISSUER";

    /** Environment variable carrying the client id: {@value}. */
    public static final String ENV_CLIENT_ID = "PROTOMOLT_CONSOLE_OIDC_CLIENT_ID";

    /** Environment variable carrying the client secret: {@value}. */
    public static final String ENV_CLIENT_SECRET = "PROTOMOLT_CONSOLE_OIDC_CLIENT_SECRET";

    /** Environment variable naming the callback as the provider reaches it: {@value}. */
    public static final String ENV_REDIRECT_URI = "PROTOMOLT_CONSOLE_OIDC_REDIRECT_URI";

    /** Environment variable giving the session lifetime as an ISO-8601 duration: {@value}. */
    public static final String ENV_SESSION_TTL = "PROTOMOLT_CONSOLE_SESSION_TTL";

    private static final List<String> ALL = List.of(
            ENV_ISSUER, ENV_CLIENT_ID, ENV_CLIENT_SECRET, ENV_REDIRECT_URI, ENV_SESSION_TTL);

    /** Refuses a blank or unusable setting by name. */
    public OidcConsoleSettings {
        requireAbsolute(issuer, "issuer");
        if (issuer.getQuery() != null || issuer.getFragment() != null) {
            throw new IllegalArgumentException("issuer must carry no query or fragment: " + issuer);
        }
        requireText(clientId, "clientId");
        requireText(clientSecret, "clientSecret");
        requireAbsolute(redirectUri, "redirectUri");
        Objects.requireNonNull(sessionTtl, "sessionTtl");
        if (sessionTtl.isNegative() || sessionTtl.isZero() || sessionTtl.compareTo(Duration.ofDays(7)) > 0) {
            throw new IllegalArgumentException(
                    "sessionTtl must be between one second and seven days: " + sessionTtl);
        }
    }

    /**
     * Reads the settings from {@code environment}.
     *
     * @param environment the process environment
     * @return the settings, or empty when none of the five variables is set
     * @throws IllegalArgumentException when some but not all are set, or a value is unusable,
     *         naming the variable
     */
    public static Optional<OidcConsoleSettings> fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        List<String> missing = new ArrayList<>();
        for (String name : ALL) {
            String value = environment.get(name);
            if (value == null || value.isBlank()) {
                missing.add(name);
            }
        }
        if (missing.size() == ALL.size()) {
            return Optional.empty();
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("console sign-in is partly configured: set "
                    + String.join(", ", missing) + " as well, or unset all of "
                    + String.join(", ", ALL));
        }
        Duration ttl;
        try {
            ttl = Duration.parse(environment.get(ENV_SESSION_TTL).trim());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(ENV_SESSION_TTL
                    + " must be an ISO-8601 duration such as PT8H: " + e.getMessage(), e);
        }
        try {
            return Optional.of(new OidcConsoleSettings(
                    URI.create(environment.get(ENV_ISSUER).trim()),
                    environment.get(ENV_CLIENT_ID).trim(),
                    environment.get(ENV_CLIENT_SECRET).trim(),
                    URI.create(environment.get(ENV_REDIRECT_URI).trim()),
                    ttl));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("console sign-in settings refused ("
                    + String.join(", ", ALL) + "): " + e.getMessage(), e);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static void requireAbsolute(URI value, String name) {
        Objects.requireNonNull(value, name);
        if (!value.isAbsolute() || value.getHost() == null) {
            throw new IllegalArgumentException(name + " must be an absolute http(s) URI: " + value);
        }
        String scheme = value.getScheme();
        if (!"https".equals(scheme) && !"http".equals(scheme)) {
            throw new IllegalArgumentException(name + " must be an http(s) URI: " + value);
        }
    }
}

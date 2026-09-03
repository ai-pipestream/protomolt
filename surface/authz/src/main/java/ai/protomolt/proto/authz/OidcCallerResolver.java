package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
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
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The IdP-backed caller store: resolves credentials through OAuth 2.0 token introspection
 * (RFC 7662), mirroring the intake service's {@code OidcIntrospectionResolver}. A
 * credential is an IdP-issued opaque token; this resolver asks the introspection endpoint
 * whether it is active and reads the caller off the token's claims, so rotation,
 * revocation, and expiry are entirely the IdP's policy and revocation takes effect on the
 * next call.
 *
 * <p>Claim mapping:
 * <ul>
 *   <li>{@code username}, falling back to {@code sub}: the principal name (both are RFC
 *   7662 standard claims).</li>
 *   <li>{@code protomolt_scopes} (JSON array of string or space-delimited string): the
 *   scopes the caller holds, each from the closed vocabulary.</li>
 * </ul>
 *
 * <p>An active token that names no principal, holds no scopes, or names a scope outside
 * the vocabulary is a misconfigured grant and resolves as unknown after logging loudly:
 * a store that cannot say what a credential may do must not authenticate it. The IdP
 * never mints the operator: an introspected caller is always a bounded principal.
 *
 * <p>Pure JDK: {@link HttpClient} for the wire, protobuf's {@link JsonFormat} for the
 * JSON. The resolver authenticates itself to the introspection endpoint with HTTP basic
 * client credentials, per RFC 7662; an unreachable or failing endpoint is a thrown
 * {@link IllegalStateException}, never a bad-credential verdict.
 */
public final class OidcCallerResolver implements CallerResolver {

    /** The scopes claim: {@value}. */
    public static final String CLAIM_SCOPES = "protomolt_scopes";

    /** Environment variable naming the introspection endpoint. */
    public static final String ENV_INTROSPECTION_URL =
            "PROTOMOLT_AUTHZ_OIDC_INTROSPECTION_URL";

    /** Environment variable carrying this process's client id at the IdP. */
    public static final String ENV_CLIENT_ID = "PROTOMOLT_AUTHZ_OIDC_CLIENT_ID";

    /** Environment variable carrying this process's client secret. */
    public static final String ENV_CLIENT_SECRET = "PROTOMOLT_AUTHZ_OIDC_CLIENT_SECRET";

    private static final Logger LOG = LoggerFactory.getLogger(OidcCallerResolver.class);

    private final URI introspectionEndpoint;
    private final String basicCredentials;
    private final HttpClient http;
    private final Duration requestTimeout;

    /**
     * @param introspectionEndpoint the RFC 7662 endpoint (Keycloak:
     *        {@code <realm>/protocol/openid-connect/token/introspect})
     * @param clientId this resolver's client id at the IdP
     * @param clientSecret this resolver's client secret
     */
    public OidcCallerResolver(URI introspectionEndpoint, String clientId,
                              String clientSecret) {
        this(introspectionEndpoint, clientId, clientSecret,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                Duration.ofSeconds(10));
    }

    /**
     * Full-control constructor for tests and tuned deployments.
     *
     * @param introspectionEndpoint the RFC 7662 endpoint
     * @param clientId this resolver's client id at the IdP
     * @param clientSecret this resolver's client secret
     * @param http the HTTP client to use
     * @param requestTimeout per-request timeout
     */
    public OidcCallerResolver(URI introspectionEndpoint, String clientId,
                              String clientSecret, HttpClient http,
                              Duration requestTimeout) {
        if (introspectionEndpoint == null) {
            throw new IllegalArgumentException("introspectionEndpoint must not be null");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
        if (clientSecret == null || clientSecret.isBlank()) {
            throw new IllegalArgumentException("clientSecret must not be blank");
        }
        this.introspectionEndpoint = introspectionEndpoint;
        this.basicCredentials = Base64.getEncoder().encodeToString(
                (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
        this.http = http;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Builds a resolver from an environment map, or null when
     * {@link #ENV_INTROSPECTION_URL} is unset; a set endpoint without both client
     * credentials refuses by name.
     *
     * @param env the environment map (injectable for tests)
     * @return the resolver, or null when OIDC introspection is not configured
     */
    public static OidcCallerResolver fromEnvironmentMap(Map<String, String> env) {
        String endpoint = env.getOrDefault(ENV_INTROSPECTION_URL, "").trim();
        if (endpoint.isEmpty()) {
            return null;
        }
        String clientId = env.getOrDefault(ENV_CLIENT_ID, "").trim();
        String clientSecret = env.getOrDefault(ENV_CLIENT_SECRET, "").trim();
        if (clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new IllegalArgumentException(ENV_INTROSPECTION_URL + " requires "
                    + ENV_CLIENT_ID + " and " + ENV_CLIENT_SECRET);
        }
        return new OidcCallerResolver(URI.create(endpoint), clientId, clientSecret);
    }

    @Override
    public Optional<Caller> resolve(String credential) {
        Struct claims = introspect(credential);
        boolean active = claims.getFieldsMap()
                .getOrDefault("active", Value.newBuilder().setBoolValue(false).build())
                .getBoolValue();
        if (!active) {
            return Optional.empty();
        }
        String name = stringClaim(claims, "username");
        if (name.isBlank()) {
            name = stringClaim(claims, "sub");
        }
        if (name.isBlank()) {
            LOG.error("introspection returned an ACTIVE token with neither 'username' nor"
                    + " 'sub'; refusing to authenticate a credential with no principal");
            return Optional.empty();
        }
        Set<String> scopes = setClaim(claims, CLAIM_SCOPES);
        if (scopes.isEmpty()) {
            LOG.error("introspection returned an ACTIVE token for '{}' without the '{}'"
                    + " claim; refusing to authenticate a credential that may do nothing",
                    name, CLAIM_SCOPES);
            return Optional.empty();
        }
        for (String scope : scopes) {
            if (!Scopes.VOCABULARY.contains(scope)) {
                LOG.error("introspection returned scope '{}' for '{}', which is outside"
                        + " the vocabulary; refusing the misconfigured grant", scope, name);
                return Optional.empty();
            }
        }
        return Optional.of(Caller.scoped(name, scopes));
    }

    private Struct introspect(String credential) {
        String body = "token=" + URLEncoder.encode(credential, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(introspectionEndpoint)
                .timeout(requestTimeout)
                .header("Authorization", "Basic " + basicCredentials)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("introspection endpoint unreachable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("introspection interrupted", e);
        }
        if (response.statusCode() != 200) {
            throw new IllegalStateException(
                    "introspection endpoint answered HTTP " + response.statusCode());
        }
        Struct.Builder claims = Struct.newBuilder();
        try {
            JsonFormat.parser().merge(response.body(), claims);
        } catch (IOException e) {
            throw new IllegalStateException("introspection response is not valid JSON", e);
        }
        return claims.build();
    }

    private static String stringClaim(Struct claims, String name) {
        Value value = claims.getFieldsMap().get(name);
        return value == null ? "" : value.getStringValue().trim();
    }

    /** Reads a set claim from a JSON array of strings or a space-delimited string. */
    private static Set<String> setClaim(Struct claims, String name) {
        Value value = claims.getFieldsMap().get(name);
        if (value == null) {
            return Set.of();
        }
        Set<String> out = new LinkedHashSet<>();
        if (value.hasListValue()) {
            ListValue list = value.getListValue();
            for (Value entry : list.getValuesList()) {
                String s = entry.getStringValue().trim();
                if (!s.isEmpty()) {
                    out.add(s);
                }
            }
        } else if (value.hasStringValue()) {
            for (String s : value.getStringValue().split("\\s+")) {
                if (!s.isBlank()) {
                    out.add(s.trim());
                }
            }
        }
        return Set.copyOf(out);
    }
}

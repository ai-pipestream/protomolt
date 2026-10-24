package ai.protomolt.proto.intake.service.identity;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
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
import java.util.Optional;
import java.util.Set;

/**
 * The IdP-backed key store: resolves API keys through OAuth 2.0 token
 * introspection (RFC 7662), the mechanism Keycloak and every mainstream IdP
 * expose. An API key is an IdP-issued opaque token; this resolver asks the
 * introspection endpoint whether it is active and reads the intake scope off
 * the token's claims. Key rotation, revocation, expiry, and the
 * rotation-grace window are therefore entirely the IdP's policy — exactly
 * where a key store belongs — and revocation takes effect on the next call.
 *
 * <p>Claim mapping (all except {@code account_id} optional):
 * <ul>
 *   <li>{@code account_id} (string, REQUIRED): the owning account. An active
 *   token without it is a misconfigured key and resolves as unknown after
 *   logging loudly — a key store that cannot say who owns the key must not
 *   authenticate it.</li>
 *   <li>{@code datasource_ids}, {@code drives}, {@code mime_types} (JSON
 *   array of string or space-delimited string): the narrowing sets, empty
 *   meaning unrestricted within the account.</li>
 *   <li>{@code max_payload_bytes} (number): the per-key payload cap.</li>
 * </ul>
 *
 * <p>Pure JDK: {@link HttpClient} for the wire, protobuf's {@link JsonFormat}
 * for the JSON. The resolver authenticates itself to the introspection
 * endpoint with HTTP basic client credentials, per RFC 7662.
 */
public final class OidcIntrospectionResolver implements ApiKeyIdentityResolver {

    /** The account claim every key must carry. */
    public static final String CLAIM_ACCOUNT_ID = "account_id";

    /** The datasource-allowlist claim. */
    public static final String CLAIM_DATASOURCE_IDS = "datasource_ids";

    /** The drive-allowlist claim. */
    public static final String CLAIM_DRIVES = "drives";

    /** The content-type-restriction claim. */
    public static final String CLAIM_MIME_TYPES = "mime_types";

    /** The per-key payload-cap claim. */
    public static final String CLAIM_MAX_PAYLOAD_BYTES = "max_payload_bytes";

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(OidcIntrospectionResolver.class);

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
    public OidcIntrospectionResolver(URI introspectionEndpoint, String clientId, String clientSecret) {
        this(
                requireEndpoint(introspectionEndpoint),
                requireNonBlank(clientId, "clientId"),
                requireNonBlank(clientSecret, "clientSecret"),
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
    public OidcIntrospectionResolver(
            URI introspectionEndpoint,
            String clientId,
            String clientSecret,
            HttpClient http,
            Duration requestTimeout) {
        this.introspectionEndpoint = requireEndpoint(introspectionEndpoint);
        String validClientId = requireNonBlank(clientId, "clientId");
        String validClientSecret = requireNonBlank(clientSecret, "clientSecret");
        if (http == null) {
            throw new IllegalArgumentException("http must not be null");
        }
        if (requestTimeout == null) {
            throw new IllegalArgumentException("requestTimeout must not be null");
        }
        this.basicCredentials =
                Base64.getEncoder()
                        .encodeToString(
                                (validClientId + ":" + validClientSecret).getBytes(StandardCharsets.UTF_8));
        this.http = http;
        this.requestTimeout = requestTimeout;
    }

    private static URI requireEndpoint(URI introspectionEndpoint) {
        if (introspectionEndpoint == null) {
            throw new IllegalArgumentException("introspectionEndpoint must not be null");
        }
        return introspectionEndpoint;
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    @Override
    public Optional<IntakeScope> resolve(String credential) {
        if (credential == null) {
            throw new IllegalArgumentException("credential must not be null");
        }
        Struct claims = introspect(credential);
        boolean active =
                claims.getFieldsMap().getOrDefault(
                                "active", Value.newBuilder().setBoolValue(false).build())
                        .getBoolValue();
        if (!active) {
            return Optional.empty();
        }
        String accountId = stringClaim(claims, CLAIM_ACCOUNT_ID);
        if (accountId.isBlank()) {
            LOG.error(
                    "introspection returned an ACTIVE token without the required '{}' claim;"
                            + " refusing to authenticate a key with no owner",
                    CLAIM_ACCOUNT_ID);
            return Optional.empty();
        }
        long maxPayloadBytes = 0L;
        Value cap = claims.getFieldsMap().get(CLAIM_MAX_PAYLOAD_BYTES);
        if (cap != null && cap.hasNumberValue()) {
            maxPayloadBytes = (long) cap.getNumberValue();
        }
        return Optional.of(
                new IntakeScope(
                        accountId,
                        setClaim(claims, CLAIM_DATASOURCE_IDS),
                        setClaim(claims, CLAIM_DRIVES),
                        setClaim(claims, CLAIM_MIME_TYPES),
                        maxPayloadBytes));
    }

    private Struct introspect(String credential) {
        String body = "token=" + URLEncoder.encode(credential, StandardCharsets.UTF_8);
        HttpRequest request =
                HttpRequest.newBuilder(introspectionEndpoint)
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
            for (String s : whitespaceTokens(value.getStringValue())) {
                if (!s.isBlank()) {
                    out.add(s.trim());
                }
            }
        }
        return Set.copyOf(out);
    }

    /**
     * The maximal non-whitespace runs of {@code value}, where whitespace is
     * the regex {@code \s} set ({@code ' '}, {@code '\t'}, {@code '\n'},
     * {@code '\013'}, {@code '\f'}, {@code '\r'}) — the tokens a
     * whitespace-run split yields once its empty edge tokens are dropped.
     */
    private static java.util.List<String> whitespaceTokens(String value) {
        java.util.List<String> tokens = new java.util.ArrayList<>();
        int from = 0;
        while (from < value.length()) {
            while (from < value.length() && isRegexWhitespace(value.charAt(from))) {
                from++;
            }
            int start = from;
            while (from < value.length() && !isRegexWhitespace(value.charAt(from))) {
                from++;
            }
            if (start < from) {
                tokens.add(value.substring(start, from));
            }
        }
        return tokens;
    }

    private static boolean isRegexWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\013' || c == '\f' || c == '\r';
    }
}

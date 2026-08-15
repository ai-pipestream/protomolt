package ai.pipestream.proto.platform;

import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.service.identity.OidcIntrospectionResolver;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point: the whole document platform from environment
 * variables. The repository family is {@code DOCUMENT_PLATFORM_*} as
 * repo-service documents it; the platform's own variables are listed on
 * {@link DocumentPlatformConfig}.
 *
 * <p>Key-store selection matches the intake door's convention:
 * {@code DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL} (+ client id and
 * secret) selects the IdP-backed store, otherwise
 * {@code DOCUMENT_PLATFORM_INTAKE_KEYS} seeds the in-memory table. The
 * selection logic is intentionally the same shape as
 * {@code IntakeServiceMain}'s; it is duplicated here only because those
 * helpers are package-private, and should collapse onto them when they go
 * public.
 */
public final class DocumentPlatformMain {

    /** Env var seeding the in-memory key store: {@code <key>=<account>[@ds1,ds2];...}. */
    public static final String ENV_KEYS = "DOCUMENT_PLATFORM_INTAKE_KEYS";

    /** Env var naming the IdP's RFC 7662 introspection endpoint. */
    public static final String ENV_OIDC_URL = "DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL";

    /** Env var carrying the door's client id at the IdP. */
    public static final String ENV_OIDC_CLIENT_ID = "DOCUMENT_PLATFORM_INTAKE_OIDC_CLIENT_ID";

    /** Env var carrying the door's client secret at the IdP. */
    public static final String ENV_OIDC_CLIENT_SECRET =
            "DOCUMENT_PLATFORM_INTAKE_OIDC_CLIENT_SECRET";

    private static final Logger LOG = LoggerFactory.getLogger(DocumentPlatformMain.class);

    private DocumentPlatformMain() {
    }

    public static void main(String[] args) throws Exception {
        DocumentPlatformConfig config = DocumentPlatformConfig.fromEnvironment();
        DocumentPlatform platform = DocumentPlatform.start(config, resolverFrom(System.getenv()));
        Runtime.getRuntime().addShutdownHook(new Thread(platform::close, "platform-shutdown"));
        LOG.info("document platform serving; shut down with SIGTERM");
        Thread.currentThread().join();
    }

    /** Selects the key store the environment configures; loud when none is. */
    static ApiKeyIdentityResolver resolverFrom(Map<String, String> env) {
        String oidcUrl = env.get(ENV_OIDC_URL);
        if (oidcUrl != null && !oidcUrl.isBlank()) {
            String clientId = env.get(ENV_OIDC_CLIENT_ID);
            String clientSecret = env.get(ENV_OIDC_CLIENT_SECRET);
            if (clientId == null || clientId.isBlank()) {
                throw new IllegalArgumentException(
                        ENV_OIDC_CLIENT_ID + " is required with " + ENV_OIDC_URL);
            }
            if (clientSecret == null || clientSecret.isBlank()) {
                throw new IllegalArgumentException(
                        ENV_OIDC_CLIENT_SECRET + " is required with " + ENV_OIDC_URL);
            }
            return new OidcIntrospectionResolver(
                    URI.create(oidcUrl.trim()), clientId.trim(), clientSecret.trim());
        }
        return seededResolver(env.get(ENV_KEYS));
    }

    /**
     * Parses the {@code <key>=<account>[@ds1,ds2];...} format. A platform
     * with zero keys can authenticate nobody, so a missing value is refused
     * loudly.
     */
    static InMemoryApiKeyIdentityResolver seededResolver(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException(
                    ENV_KEYS + " is required for the env-seeded key store"
                            + " (format: <key>=<account_id>[@<datasource_id>,...];...)");
        }
        InMemoryApiKeyIdentityResolver resolver = new InMemoryApiKeyIdentityResolver();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        ENV_KEYS + " entry is not <key>=<account_id>[@...]: '" + trimmed + "'");
            }
            String key = trimmed.substring(0, eq).trim();
            String grant = trimmed.substring(eq + 1).trim();
            int at = grant.indexOf('@');
            if (at < 0) {
                resolver.register(key, IntakeScope.unrestricted(grant));
                continue;
            }
            String accountId = grant.substring(0, at).trim();
            Set<String> datasources = java.util.Arrays.stream(grant.substring(at + 1).split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            if (datasources.isEmpty()) {
                throw new IllegalArgumentException(
                        ENV_KEYS + " entry names '@' but no datasource ids: '" + trimmed + "'");
            }
            resolver.register(key, new IntakeScope(accountId, datasources, Set.of(), Set.of(), 0L));
        }
        return resolver;
    }
}

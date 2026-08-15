package ai.pipestream.proto.intake.service;

import ai.pipestream.proto.intake.service.identity.ApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.service.identity.OidcIntrospectionResolver;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point: environment-configured intake over Netty.
 *
 * <p>Key-store selection: when
 * {@code DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL} is set the door
 * authenticates against the IdP's RFC 7662 introspection endpoint (the
 * Keycloak-shaped production default; client id/secret ride the companion
 * env vars). Otherwise the store is seeded from
 * {@code DOCUMENT_PLATFORM_INTAKE_KEYS}, a semicolon-separated list of
 * {@code <key>=<account_id>[@<datasource_id>[,...]]} entries — the
 * env-seeded store for demos and single-tenant deployments. Exactly one
 * source must be configured; a door with no key store is refused loudly.
 */
public final class IntakeServiceMain {

    /** Env var seeding the in-memory key store: {@code <key>=<account>[@ds1,ds2];...}. */
    public static final String ENV_KEYS = "DOCUMENT_PLATFORM_INTAKE_KEYS";

    /** Env var naming the IdP's RFC 7662 introspection endpoint. */
    public static final String ENV_OIDC_URL = "DOCUMENT_PLATFORM_INTAKE_OIDC_INTROSPECTION_URL";

    /** Env var carrying this door's client id at the IdP. */
    public static final String ENV_OIDC_CLIENT_ID = "DOCUMENT_PLATFORM_INTAKE_OIDC_CLIENT_ID";

    /** Env var carrying this door's client secret at the IdP. */
    public static final String ENV_OIDC_CLIENT_SECRET =
            "DOCUMENT_PLATFORM_INTAKE_OIDC_CLIENT_SECRET";

    private static final Logger LOG = LoggerFactory.getLogger(IntakeServiceMain.class);

    private IntakeServiceMain() {
    }

    public static void main(String[] args) throws Exception {
        IntakeServiceConfig config = IntakeServiceConfig.fromEnvironment();
        ApiKeyIdentityResolver resolver = selectResolver(System.getenv());
        IntakeServices services = IntakeServices.build(config, resolver);
        services.startNetty(config.grpcPort());
        LOG.info(
                "intake-service listening on gRPC port {} (repo target {})",
                services.server().getPort(),
                config.repoTarget());
        Runtime.getRuntime().addShutdownHook(new Thread(services::close, "intake-shutdown"));
        services.server().awaitTermination();
    }

    /**
     * Picks the key store the environment configures: OIDC introspection
     * when {@link #ENV_OIDC_URL} is set (client id and secret then become
     * required, rejected loudly by name when missing), else the env-seeded
     * in-memory store.
     */
    static ApiKeyIdentityResolver selectResolver(java.util.Map<String, String> env) {
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
                    java.net.URI.create(oidcUrl.trim()), clientId.trim(), clientSecret.trim());
        }
        return resolverFromEnvironment(env.get(ENV_KEYS));
    }

    /**
     * Parses the {@code DOCUMENT_PLATFORM_INTAKE_KEYS} format. Rejects a
     * missing or empty value loudly — an intake door with zero keys can
     * authenticate nobody, and silently booting one would only look healthy.
     */
    static InMemoryApiKeyIdentityResolver resolverFromEnvironment(String spec) {
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
            Set<String> datasources =
                    java.util.Arrays.stream(grant.substring(at + 1).split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(java.util.stream.Collectors.toUnmodifiableSet());
            if (datasources.isEmpty()) {
                throw new IllegalArgumentException(
                        ENV_KEYS + " entry names '@' but no datasource ids: '" + trimmed + "'");
            }
            resolver.register(
                    key, new IntakeScope(accountId, datasources, Set.of(), Set.of(), 0L));
        }
        return resolver;
    }
}

package ai.pipestream.proto.intake.service;

import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point: environment-configured intake over Netty.
 *
 * <p>The key store is seeded from {@code DOCUMENT_PLATFORM_INTAKE_KEYS}, a
 * semicolon-separated list of {@code <key>=<account_id>[@<datasource_id>[,...]]}
 * entries — the env-seeded store for demos and single-tenant deployments.
 * Production deployments swap in an external key store (Keycloak, JDBC)
 * through {@link IntakeServices#build}; this main stays deliberately small.
 */
public final class IntakeServiceMain {

    /** Env var seeding the in-memory key store: {@code <key>=<account>[@ds1,ds2];...}. */
    public static final String ENV_KEYS = "DOCUMENT_PLATFORM_INTAKE_KEYS";

    private static final Logger LOG = LoggerFactory.getLogger(IntakeServiceMain.class);

    private IntakeServiceMain() {
    }

    public static void main(String[] args) throws Exception {
        IntakeServiceConfig config = IntakeServiceConfig.fromEnvironment();
        InMemoryApiKeyIdentityResolver resolver = resolverFromEnvironment(System.getenv(ENV_KEYS));
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

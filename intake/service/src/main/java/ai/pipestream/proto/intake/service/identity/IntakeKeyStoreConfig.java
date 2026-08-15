package ai.pipestream.proto.intake.service.identity;

import java.util.Map;

/**
 * Connection and migration settings for the JDBC-backed intake key store.
 * <p>
 * A plain value object — no framework configuration binding. Services either
 * build it directly or use {@link #fromEnvironment()}. Same shape as the
 * account store's config, with one deliberate difference: there is NO
 * localhost fallback for the connection itself. This store is the
 * authentication authority of an air-gapped deployment — a key store that
 * silently pointed itself at a defaulted local database would look healthy
 * while authenticating against the wrong (or an empty) key table, so the
 * JDBC URL, username, and password are all required and rejected loudly by
 * name when missing. Only the pool size and migration location fall back.
 *
 * @param jdbcUrl           JDBC URL of the PostgreSQL database; required
 * @param username          database username; required
 * @param password          database password; required to be present — an
 *                          empty password is a deliberate configuration
 *                          (trust auth), a null one is a missing one
 * @param maxPoolSize       HikariCP maximum pool size; values &lt;= 0 fall
 *                          back to {@link #DEFAULT_POOL_SIZE}. Keep this
 *                          small: callers run on virtual threads, so the
 *                          pool — not the thread count — is the concurrency
 *                          limit against the database.
 * @param migrationLocation Flyway location for the key store's migrations;
 *                          blank falls back to
 *                          {@link #DEFAULT_MIGRATION_LOCATION}
 */
public record IntakeKeyStoreConfig(
        String jdbcUrl,
        String username,
        String password,
        int maxPoolSize,
        String migrationLocation) {

    /** Default HikariCP pool size when none is configured. */
    public static final int DEFAULT_POOL_SIZE = 10;

    /** Default Flyway location, resolved from this module's resources. */
    // Module-scoped directory: repo, account, jobs, and intake-keys stores
    // must be able to share one classpath (the all-in-one container), so
    // each module's migrations live in a disjoint location.
    public static final String DEFAULT_MIGRATION_LOCATION = "classpath:db/migration/intakekeys";

    /** Environment variable carrying the JDBC URL; required, never defaulted. */
    public static final String ENV_JDBC_URL = "DOCUMENT_PLATFORM_INTAKE_KEYS_JDBC_URL";

    /** Environment variable carrying the database username; required. */
    public static final String ENV_USERNAME = "DOCUMENT_PLATFORM_INTAKE_KEYS_USERNAME";

    /** Environment variable carrying the database password; required. */
    public static final String ENV_PASSWORD = "DOCUMENT_PLATFORM_INTAKE_KEYS_PASSWORD";

    /** Environment variable overriding the pool size. */
    public static final String ENV_POOL_SIZE = "DOCUMENT_PLATFORM_INTAKE_KEYS_POOL_SIZE";

    public IntakeKeyStoreConfig {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl must not be blank");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username must not be blank");
        }
        if (password == null) {
            throw new IllegalArgumentException("password must not be null");
        }
        if (maxPoolSize <= 0) {
            maxPoolSize = DEFAULT_POOL_SIZE;
        }
        if (migrationLocation == null || migrationLocation.isBlank()) {
            migrationLocation = DEFAULT_MIGRATION_LOCATION;
        }
    }

    /**
     * Convenience constructor with the default pool size and migration
     * location.
     *
     * @param jdbcUrl  JDBC URL of the PostgreSQL database; required
     * @param username database username; required
     * @param password database password; required to be present
     */
    public IntakeKeyStoreConfig(String jdbcUrl, String username, String password) {
        this(jdbcUrl, username, password, DEFAULT_POOL_SIZE, DEFAULT_MIGRATION_LOCATION);
    }

    /**
     * Builds a config from the process environment. See
     * {@link #fromEnvironmentMap(Map)} for the variable family and the
     * required-vs-defaulted contract.
     *
     * @return the resolved config
     */
    public static IntakeKeyStoreConfig fromEnvironment() {
        return fromEnvironmentMap(System.getenv());
    }

    /**
     * Builds a config from an environment map: {@link #ENV_JDBC_URL},
     * {@link #ENV_USERNAME} and {@link #ENV_PASSWORD} are REQUIRED and
     * rejected loudly by name when missing — no localhost fallback, see the
     * class javadoc for why. {@link #ENV_POOL_SIZE} falls back to
     * {@link #DEFAULT_POOL_SIZE}; the migration location is always
     * {@link #DEFAULT_MIGRATION_LOCATION}.
     *
     * @param env the environment map (injectable for tests)
     * @return the resolved config
     */
    public static IntakeKeyStoreConfig fromEnvironmentMap(Map<String, String> env) {
        String jdbcUrl = env.get(ENV_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException(
                    ENV_JDBC_URL + " is required for the JDBC key store (no default database:"
                            + " the key store must point at the database you mean)");
        }
        String username = env.get(ENV_USERNAME);
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException(
                    ENV_USERNAME + " is required with " + ENV_JDBC_URL);
        }
        String password = env.get(ENV_PASSWORD);
        if (password == null) {
            throw new IllegalArgumentException(
                    ENV_PASSWORD + " is required with " + ENV_JDBC_URL
                            + " (set it empty for trust auth)");
        }
        return new IntakeKeyStoreConfig(
                jdbcUrl.trim(),
                username.trim(),
                password,
                parseIntOrDefault(env.get(ENV_POOL_SIZE), DEFAULT_POOL_SIZE),
                DEFAULT_MIGRATION_LOCATION);
    }

    private static int parseIntOrDefault(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}

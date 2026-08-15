package ai.pipestream.proto.account.service.store;

/**
 * Connection and migration settings for the account store.
 * <p>
 * A plain value object — no framework configuration binding. Services either
 * build it from their own config or use {@link #fromEnvironment()}. Same
 * contract as the repo ledger's config, but a separate variable family and a
 * separate default database: account data never shares repo's schema (or its
 * Flyway history).
 *
 * @param jdbcUrl           JDBC URL of the PostgreSQL database
 * @param username          database username
 * @param password          database password
 * @param maxPoolSize       HikariCP maximum pool size; values &lt;= 0 fall
 *                          back to {@link #DEFAULT_POOL_SIZE}. Keep this
 *                          small: callers run on virtual threads, so the
 *                          pool — not the thread count — is the concurrency
 *                          limit against the database.
 * @param migrationLocation Flyway location for the account store's migrations
 */
public record AccountStoreConfig(
        String jdbcUrl,
        String username,
        String password,
        int maxPoolSize,
        String migrationLocation) {

    /** Default HikariCP pool size when none is configured. */
    public static final int DEFAULT_POOL_SIZE = 10;

    /** Default Flyway location, resolved from this module's resources. */
    // Module-scoped directory: repo, account, and jobs stores must be able
    // to share one classpath (the all-in-one container), so each module's
    // migrations live in a disjoint location. Moving the files is safe for
    // existing databases: Flyway identifies migrations by version and
    // content checksum, not path.
    public static final String DEFAULT_MIGRATION_LOCATION = "classpath:db/migration/account";

    /** Environment variable overriding the JDBC URL. */
    public static final String ENV_JDBC_URL = "DOCUMENT_PLATFORM_ACCOUNT_JDBC_URL";

    /** Environment variable overriding the database username. */
    public static final String ENV_USERNAME = "DOCUMENT_PLATFORM_ACCOUNT_USERNAME";

    /** Environment variable overriding the database password. */
    public static final String ENV_PASSWORD = "DOCUMENT_PLATFORM_ACCOUNT_PASSWORD";

    /** Environment variable overriding the pool size. */
    public static final String ENV_POOL_SIZE = "DOCUMENT_PLATFORM_ACCOUNT_POOL_SIZE";

    static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/accounts";
    static final String DEFAULT_USERNAME = "accounts";
    static final String DEFAULT_PASSWORD = "accounts";

    public AccountStoreConfig {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            jdbcUrl = DEFAULT_JDBC_URL;
        }
        if (username == null || username.isBlank()) {
            username = DEFAULT_USERNAME;
        }
        if (password == null) {
            password = DEFAULT_PASSWORD;
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
     */
    public AccountStoreConfig(String jdbcUrl, String username, String password) {
        this(jdbcUrl, username, password, DEFAULT_POOL_SIZE, DEFAULT_MIGRATION_LOCATION);
    }

    /**
     * Build a config from the environment: {@code DOCUMENT_PLATFORM_ACCOUNT_JDBC_URL},
     * {@code DOCUMENT_PLATFORM_ACCOUNT_USERNAME},
     * {@code DOCUMENT_PLATFORM_ACCOUNT_PASSWORD} and
     * {@code DOCUMENT_PLATFORM_ACCOUNT_POOL_SIZE}, falling back to a local
     * PostgreSQL at {@code localhost:5432/accounts} (user/password
     * {@code accounts}/{@code accounts}).
     *
     * @return the resolved config
     */
    public static AccountStoreConfig fromEnvironment() {
        return new AccountStoreConfig(
                envOrDefault(ENV_JDBC_URL, DEFAULT_JDBC_URL),
                envOrDefault(ENV_USERNAME, DEFAULT_USERNAME),
                envOrDefault(ENV_PASSWORD, DEFAULT_PASSWORD),
                parseIntOrDefault(System.getenv(ENV_POOL_SIZE), DEFAULT_POOL_SIZE),
                DEFAULT_MIGRATION_LOCATION);
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
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

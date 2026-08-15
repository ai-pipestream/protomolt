package ai.pipestream.proto.jobs.service.store;

/**
 * Connection and migration settings for the chain-jobs store.
 * <p>
 * A plain value object — no framework configuration binding. Unlike the
 * account store there are no localhost fallbacks: a jobs store pointed at a
 * "reasonable default" database is a queue silently draining into the wrong
 * place, so a missing JDBC URL or username fails construction.
 *
 * @param jdbcUrl           JDBC URL of the PostgreSQL database (required)
 * @param username          database username (required)
 * @param password          database password (may be empty for trust auth)
 * @param maxPoolSize       HikariCP maximum pool size; values &lt;= 0 fall
 *                          back to {@link #DEFAULT_POOL_SIZE}. Keep this
 *                          small: callers run on virtual threads, so the
 *                          pool — not the thread count — is the concurrency
 *                          limit against the database.
 * @param migrationLocation Flyway location for the jobs migrations
 */
public record ChainJobStoreConfig(
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
    public static final String DEFAULT_MIGRATION_LOCATION = "classpath:db/migration/jobs";

    public ChainJobStoreConfig {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("jdbcUrl is required: the chain-jobs store "
                    + "has no default database (start protomolt-serve with --jobs-jdbc)");
        }
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("username is required for " + jdbcUrl);
        }
        if (password == null) {
            password = "";
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
     * @param jdbcUrl JDBC URL of the PostgreSQL database
     * @param username database username
     * @param password database password
     */
    public ChainJobStoreConfig(String jdbcUrl, String username, String password) {
        this(jdbcUrl, username, password, DEFAULT_POOL_SIZE, DEFAULT_MIGRATION_LOCATION);
    }
}

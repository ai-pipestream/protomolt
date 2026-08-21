package ai.pipestream.proto.authz.jdbc;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.CallerResolver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Set;

/**
 * The JDBC-backed caller store: the resolver for air-gapped deployments where no IdP
 * exists and credentials live in the operator's own PostgreSQL, mirroring the intake
 * service's JDBC key store. HikariCP pool then Flyway migration at construction, in that
 * order (the house persistence shape), then plain JDBC per lookup.
 *
 * <p>Credentials are NEVER stored raw: the table is keyed by the lowercase hex SHA-256 of
 * the credential, so a dump of the table authenticates nobody. {@link #resolve} hashes
 * the presented credential and looks the hash up; only a live row (one whose
 * {@code revoked_at} is NULL) resolves. A live row whose scopes fall outside the closed
 * vocabulary is a misconfigured grant and resolves as unknown after logging loudly. The
 * store never mints the operator: a resolved caller is always a bounded principal.
 *
 * <p>Error philosophy, matching the {@link CallerResolver} contract: an unknown or
 * revoked credential is {@link Optional#empty()} — the transport's unauthenticated
 * refusal. A store failure (connection lost, query failed) is a thrown
 * {@link IllegalStateException}, because a broken database must never masquerade as a
 * bad-credential verdict.
 *
 * <p>Rotation with grace: {@link #mint} the new credential, {@link #revoke} the old one
 * when the grace window ends. Two live rows resolving to the same principal IS the grace
 * window — the same property the access-policy document expresses with several digests.
 *
 * <p>Thread safety: safe for concurrent use; every call borrows its own pooled
 * connection.
 */
public final class JdbcCallerResolver implements CallerResolver, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCallerResolver.class);

    private static final String SELECT_LIVE =
            "SELECT principal_name, scopes FROM authz_principal"
                    + " WHERE credential_sha256 = ? AND revoked_at IS NULL";

    private static final String INSERT_PRINCIPAL =
            "INSERT INTO authz_principal (credential_sha256, principal_name, scopes)"
                    + " VALUES (?, ?, ?)";

    private static final String REVOKE_CREDENTIAL =
            "UPDATE authz_principal SET revoked_at = now()"
                    + " WHERE credential_sha256 = ? AND revoked_at IS NULL";

    /** PostgreSQL SQLState for a unique-constraint violation. */
    private static final String UNIQUE_VIOLATION = "23505";

    private final HikariDataSource dataSource;

    /**
     * Boots the store: starts the pool, then migrates the schema. Construction fails
     * fast — nothing starts lazily, and a half-started pool is closed before the failure
     * propagates.
     *
     * @param config connection and migration settings
     */
    public JdbcCallerResolver(CallerStoreConfig config) {
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("authz-callers");
        hikari.setJdbcUrl(config.jdbcUrl());
        hikari.setUsername(config.username());
        hikari.setPassword(config.password());
        hikari.setMaximumPoolSize(config.maxPoolSize());
        // Fail fast when the pool is exhausted: a wedged database should
        // surface as an error quickly, not as a silent hang on the service.
        hikari.setConnectionTimeout(10_000);
        // Retire connections well under the database's own idle/connection
        // lifetime so the server never kills a connection the pool still
        // believes is live.
        hikari.setMaxLifetime(600_000);
        hikari.setIdleTimeout(300_000);
        // Prepared-statement caching lives in the pgjdbc driver (HikariCP
        // deliberately has none of its own).
        hikari.addDataSourceProperty("preparedStatementCacheQueries", "256");
        hikari.addDataSourceProperty("preparedStatementCacheSizeMiB", "5");
        this.dataSource = new HikariDataSource(hikari);

        try {
            Flyway.configure()
                    .dataSource(dataSource)
                    .locations(config.migrationLocation())
                    .load()
                    .migrate();
        } catch (RuntimeException e) {
            // Don't leak the pool if migration fails.
            this.dataSource.close();
            throw e;
        }
    }

    @Override
    public Optional<Caller> resolve(String credential) {
        String hash = sha256Hex(credential);
        try (Connection c = dataSource.getConnection();
                PreparedStatement select = c.prepareStatement(SELECT_LIVE)) {
            select.setString(1, hash);
            try (ResultSet row = select.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                String name = row.getString("principal_name");
                Set<String> scopes = textArray(row, "scopes");
                if (scopes.isEmpty()) {
                    LOG.error("caller store row for '{}' holds no scopes; refusing to"
                            + " authenticate a credential that may do nothing", name);
                    return Optional.empty();
                }
                for (String scope : scopes) {
                    if (!Scopes.VOCABULARY.contains(scope)) {
                        LOG.error("caller store row for '{}' names scope '{}', which is"
                                + " outside the vocabulary; refusing the misconfigured"
                                + " grant", name, scope);
                        return Optional.empty();
                    }
                }
                return Optional.of(Caller.scoped(name, scopes));
            }
        } catch (SQLException e) {
            // Store failure, not a bad-credential verdict: INTERNAL upstream.
            throw new IllegalStateException("caller store lookup failed", e);
        }
    }

    /**
     * Mints a credential: stores its SHA-256 hash with the principal name and scopes it
     * resolves to. The raw credential never reaches the database.
     *
     * @param credential the credential material; must not be blank
     * @param caller the bounded principal the credential resolves to; never the operator
     * @throws IllegalArgumentException when the caller is unrestricted, or a credential
     *         with the same hash already exists (live or revoked) — the message names
     *         nothing secret
     * @throws IllegalStateException when the store itself fails
     */
    public void mint(String credential, Caller caller) {
        if (credential == null || credential.isBlank()) {
            throw new IllegalArgumentException("credential must not be blank");
        }
        if (caller == null) {
            throw new IllegalArgumentException("caller must not be null");
        }
        if (caller.unrestricted()) {
            throw new IllegalArgumentException(
                    "the caller store never mints the operator");
        }
        try (Connection c = dataSource.getConnection();
                PreparedStatement insert = c.prepareStatement(INSERT_PRINCIPAL)) {
            insert.setString(1, sha256Hex(credential));
            insert.setString(2, caller.name());
            insert.setArray(3, c.createArrayOf("text",
                    caller.scopes().toArray(String[]::new)));
            insert.executeUpdate();
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION.equals(e.getSQLState())) {
                throw new IllegalArgumentException(
                        "a credential with this hash already exists; mint fresh material",
                        e);
            }
            throw new IllegalStateException("caller store mint failed", e);
        }
    }

    /**
     * Revokes a credential: it is refused from this instant. Revoking an unknown or
     * already-revoked credential is a no-op. This is the second half of
     * rotation-with-grace: {@link #mint} the replacement first, then revoke the old one
     * when the grace window ends.
     *
     * @param credential the credential material to revoke
     * @throws IllegalStateException when the store itself fails
     */
    public void revoke(String credential) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement revoke = c.prepareStatement(REVOKE_CREDENTIAL)) {
            revoke.setString(1, sha256Hex(credential));
            revoke.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("caller store revoke failed", e);
        }
    }

    /** Drains the pool. */
    @Override
    public void close() {
        dataSource.close();
    }

    private static String sha256Hex(String credential) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every conforming JRE.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        return HexFormat.of().formatHex(
                digest.digest(credential.getBytes(StandardCharsets.UTF_8)));
    }

    private static Set<String> textArray(ResultSet row, String column) throws SQLException {
        Array array = row.getArray(column);
        // The column is NOT NULL, so null only means the schema drifted out
        // from under us — surface it as the store failure it is.
        if (array == null) {
            throw new SQLException("column " + column + " unexpectedly NULL");
        }
        return Set.of((String[]) array.getArray());
    }
}

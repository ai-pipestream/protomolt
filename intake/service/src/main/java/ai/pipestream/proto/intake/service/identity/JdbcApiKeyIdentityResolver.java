package ai.pipestream.proto.intake.service.identity;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import org.flywaydb.core.Flyway;

/**
 * The JDBC-backed key store: the resolver for air-gapped deployments where no
 * IdP exists and keys live in the operator's own PostgreSQL. HikariCP pool →
 * Flyway migration at construction, in that order (the house persistence
 * shape; see the account store), then plain JDBC per lookup.
 *
 * <p>Keys are NEVER stored raw: the table is keyed by the lowercase hex
 * SHA-256 of the credential, so a dump of the key table authenticates
 * nobody. {@link #resolve} hashes the presented credential and looks the
 * hash up; only a live row (one whose {@code revoked_at} is NULL) resolves.
 *
 * <p>Error philosophy, matching the {@link ApiKeyIdentityResolver} contract:
 * an unknown or revoked key is {@link Optional#empty()} — the caller's
 * {@code UNAUTHENTICATED}. A store failure (connection lost, query failed)
 * is a thrown {@link IllegalStateException} — the caller's {@code INTERNAL}.
 * A broken database must never masquerade as a bad-key verdict, because that
 * would turn an outage into a silent lockout that looks like the caller's
 * fault.
 *
 * <p>Rotation with grace: mint the new key, revoke the old one when the
 * grace window ends. Two live rows resolving to the same account IS the
 * grace window — the schema needs no rotation state beyond that.
 *
 * <p>Thread safety: safe for concurrent use; every call borrows its own
 * pooled connection.
 */
public final class JdbcApiKeyIdentityResolver implements ApiKeyIdentityResolver, AutoCloseable {

    private static final String SELECT_LIVE =
            "SELECT account_id, datasource_ids, drives, mime_types, max_payload_bytes"
                    + " FROM intake_api_key WHERE key_hash = ? AND revoked_at IS NULL";

    private static final String INSERT_KEY =
            "INSERT INTO intake_api_key"
                    + " (key_hash, account_id, datasource_ids, drives, mime_types, max_payload_bytes)"
                    + " VALUES (?, ?, ?, ?, ?, ?)";

    private static final String REVOKE_KEY =
            "UPDATE intake_api_key SET revoked_at = now()"
                    + " WHERE key_hash = ? AND revoked_at IS NULL";

    /** PostgreSQL SQLState for a unique-constraint violation. */
    private static final String UNIQUE_VIOLATION = "23505";

    private final HikariDataSource dataSource;

    /**
     * Boots the store: starts the pool, then migrates the schema.
     * Construction fails fast — nothing starts lazily, and a half-started
     * pool is closed before the failure propagates.
     *
     * @param config connection and migration settings
     */
    public JdbcApiKeyIdentityResolver(IntakeKeyStoreConfig config) {
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("intake-keys");
        hikari.setJdbcUrl(config.jdbcUrl());
        hikari.setUsername(config.username());
        hikari.setPassword(config.password());
        hikari.setMaximumPoolSize(config.maxPoolSize());
        // Fail fast when the pool is exhausted: a wedged database should
        // surface as an error quickly, not as a silent hang on the door.
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
    public Optional<IntakeScope> resolve(String credential) {
        String keyHash = sha256Hex(credential);
        try (Connection c = dataSource.getConnection();
                PreparedStatement select = c.prepareStatement(SELECT_LIVE)) {
            select.setString(1, keyHash);
            try (ResultSet row = select.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                return Optional.of(new IntakeScope(
                        row.getString("account_id"),
                        textArray(row, "datasource_ids"),
                        textArray(row, "drives"),
                        textArray(row, "mime_types"),
                        row.getLong("max_payload_bytes")));
            }
        } catch (SQLException e) {
            // Store failure, not a bad-key verdict: INTERNAL upstream.
            throw new IllegalStateException("intake key store lookup failed", e);
        }
    }

    /**
     * Mints a key: stores the credential's SHA-256 hash with the scope it
     * carries. The raw credential never reaches the database.
     *
     * @param credential the key material; must not be blank
     * @param scope the scope the key carries
     * @throws IllegalArgumentException when a key with the same hash already
     *         exists (live or revoked) — the message names nothing secret
     * @throws IllegalStateException when the store itself fails
     */
    public void mint(String credential, IntakeScope scope) {
        if (credential == null || credential.isBlank()) {
            throw new IllegalArgumentException("credential must not be blank");
        }
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        String keyHash = sha256Hex(credential);
        try (Connection c = dataSource.getConnection();
                PreparedStatement insert = c.prepareStatement(INSERT_KEY)) {
            insert.setString(1, keyHash);
            insert.setString(2, scope.accountId());
            insert.setArray(3, textArrayOf(c, scope.datasourceIds()));
            insert.setArray(4, textArrayOf(c, scope.drives()));
            insert.setArray(5, textArrayOf(c, scope.mimeTypes()));
            insert.setLong(6, scope.maxPayloadBytes());
            insert.executeUpdate();
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION.equals(e.getSQLState())) {
                throw new IllegalArgumentException(
                        "a key with this hash already exists; mint a fresh credential", e);
            }
            throw new IllegalStateException("intake key store mint failed", e);
        }
    }

    /**
     * Revokes a key: the credential is refused from this instant. Revoking
     * an unknown or already-revoked key is a no-op — the same idempotent
     * semantics as {@link InMemoryApiKeyIdentityResolver#revoke}.
     *
     * <p>This is the second half of rotation-with-grace: {@link #mint} the
     * replacement key first, then revoke the old one when the grace window
     * ends. While both are live, both resolve — two live rows for one
     * account IS the grace window.
     *
     * @param credential the key material to revoke
     * @throws IllegalStateException when the store itself fails
     */
    public void revoke(String credential) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement revoke = c.prepareStatement(REVOKE_KEY)) {
            revoke.setString(1, sha256Hex(credential));
            revoke.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("intake key store revoke failed", e);
        }
    }

    /** Drains the pool. */
    @Override
    public void close() {
        dataSource.close();
    }

    /**
     * The lowercase hex SHA-256 of {@code credential} — the only form of the
     * key material this store ever persists or queries by.
     *
     * @param credential the key material
     * @return the 64-character lowercase hex digest
     */
    static String sha256Hex(String credential) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every conforming JRE.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        return HexFormat.of().formatHex(digest.digest(credential.getBytes(StandardCharsets.UTF_8)));
    }

    private static Set<String> textArray(ResultSet row, String column) throws SQLException {
        Array array = row.getArray(column);
        // The columns are NOT NULL DEFAULT '{}', so null only means a schema
        // drifted out from under us — surface it as the store failure it is.
        if (array == null) {
            throw new SQLException("column " + column + " unexpectedly NULL");
        }
        return Set.of((String[]) array.getArray());
    }

    private static Array textArrayOf(Connection c, Set<String> values) throws SQLException {
        return c.createArrayOf("text", values.toArray(String[]::new));
    }
}

package ai.pipestream.proto.authz.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.actions.Scopes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * {@link JdbcCallerResolver} against a real testcontainers PostgreSQL 17: mint → resolve
 * mapping, the unknown/revoked → empty contract, the two-live-credentials rotation-grace
 * shape, the operator-mint refusal, the misconfigured-row refusal, and — most
 * load-bearing — the guarantee that raw credential material never lands in the table.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcCallerResolverIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static JdbcCallerResolver resolver;

    @BeforeAll
    static void boot() {
        resolver = new JdbcCallerResolver(new CallerStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    @AfterAll
    static void tearDown() {
        resolver.close();
    }

    @BeforeEach
    void clean() throws SQLException {
        try (Connection c = verificationConnection()) {
            c.createStatement().execute("DELETE FROM authz_principal");
        }
    }

    /** A connection outside the resolver's pool, for looking at the raw table. */
    private static Connection verificationConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    @Test
    void mintThenResolveMapsThePrincipalAndScopes() {
        resolver.mint("querier-credential", Caller.scoped("querier",
                Set.of(Scopes.SEARCH_QUERY, Scopes.METRICS_QUERY)));
        Caller caller = resolver.resolve("querier-credential").orElseThrow();
        assertThat(caller.name()).isEqualTo("querier");
        assertThat(caller.unrestricted()).isFalse();
        assertThat(caller.scopes())
                .containsExactlyInAnyOrder(Scopes.SEARCH_QUERY, Scopes.METRICS_QUERY);
    }

    @Test
    void unknownAndRevokedCredentialsResolveEmpty() {
        assertThat(resolver.resolve("never-minted")).isEmpty();
        resolver.mint("short-lived", Caller.scoped("temp", Set.of(Scopes.SCHEMA_READ)));
        resolver.revoke("short-lived");
        assertThat(resolver.resolve("short-lived")).isEmpty();
        // Idempotent: revoking again (or revoking the unknown) is a no-op.
        resolver.revoke("short-lived");
        resolver.revoke("never-minted");
    }

    @Test
    void twoLiveCredentialsForOnePrincipalIsTheRotationGraceWindow() {
        Caller principal = Caller.scoped("rotating", Set.of(Scopes.SCHEMA_READ));
        resolver.mint("old-credential", principal);
        resolver.mint("new-credential", principal);
        assertThat(resolver.resolve("old-credential").orElseThrow().name())
                .isEqualTo("rotating");
        assertThat(resolver.resolve("new-credential").orElseThrow().name())
                .isEqualTo("rotating");
        resolver.revoke("old-credential");
        assertThat(resolver.resolve("old-credential")).isEmpty();
        assertThat(resolver.resolve("new-credential")).isPresent();
    }

    @Test
    void theStoreNeverMintsTheOperatorAndNeverDuplicatesAHash() {
        assertThatThrownBy(() -> resolver.mint("root-credential", Caller.operator()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never mints the operator");
        resolver.mint("taken", Caller.scoped("first", Set.of(Scopes.SCHEMA_READ)));
        assertThatThrownBy(() ->
                resolver.mint("taken", Caller.scoped("second", Set.of(Scopes.SCHEMA_READ))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mint fresh material");
    }

    @Test
    void aRowOutsideTheVocabularyRefusesInsteadOfAuthenticating() throws SQLException {
        try (Connection c = verificationConnection();
                PreparedStatement insert = c.prepareStatement(
                        "INSERT INTO authz_principal"
                                + " (credential_sha256, principal_name, scopes)"
                                + " VALUES (?, ?, ?)")) {
            insert.setString(1, "a".repeat(64));
            insert.setString(2, "drifted");
            insert.setArray(3, c.createArrayOf("text", new String[] {"rule-the-world"}));
            insert.executeUpdate();
        }
        // The presented credential whose sha256 we cannot forge never matches that
        // hash, so plant a real credential's row with a bad scope instead.
        try (Connection c = verificationConnection();
                PreparedStatement update = c.prepareStatement(
                        "UPDATE authz_principal SET credential_sha256 = ("
                                + "SELECT encode(sha256('bad-scope-credential'::bytea),"
                                + " 'hex')) WHERE principal_name = 'drifted'")) {
            update.executeUpdate();
        }
        assertThat(resolver.resolve("bad-scope-credential")).isEmpty();
    }

    @Test
    void rawCredentialMaterialNeverLandsInTheTable() throws SQLException {
        resolver.mint("super-secret-credential",
                Caller.scoped("careful", Set.of(Scopes.SCHEMA_READ)));
        try (Connection c = verificationConnection();
                PreparedStatement select = c.prepareStatement(
                        "SELECT credential_sha256, principal_name, scopes::text"
                                + " FROM authz_principal");
                ResultSet rows = select.executeQuery()) {
            while (rows.next()) {
                for (int column = 1; column <= 3; column++) {
                    assertThat(rows.getString(column))
                            .doesNotContain("super-secret-credential");
                }
            }
        }
    }
}

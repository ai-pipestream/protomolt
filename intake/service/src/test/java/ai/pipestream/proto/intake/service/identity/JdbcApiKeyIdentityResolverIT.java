package ai.pipestream.proto.intake.service.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * {@link JdbcApiKeyIdentityResolver} against a real testcontainers
 * PostgreSQL 17: the mint → resolve field mapping (arrays, the
 * empty-set-means-unrestricted convention, the payload cap), the
 * unknown/revoked → empty contract, the two-live-keys rotation-grace shape,
 * the duplicate-mint rejection, and — most load-bearing — the guarantee
 * that raw key material never lands in the table in any column.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcApiKeyIdentityResolverIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static JdbcApiKeyIdentityResolver resolver;

    @BeforeAll
    static void boot() {
        resolver = new JdbcApiKeyIdentityResolver(new IntakeKeyStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    @AfterAll
    static void tearDown() {
        resolver.close();
    }

    @BeforeEach
    void clean() throws SQLException {
        try (Connection c = verificationConnection()) {
            c.createStatement().execute("DELETE FROM intake_api_key");
        }
    }

    /** A connection outside the resolver's pool, for looking at the raw table. */
    private static Connection verificationConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    @Test
    void mintThenResolveMapsEveryScopeField() {
        IntakeScope minted = new IntakeScope(
                "acct-full",
                Set.of("ds-1", "ds-2"),
                Set.of("drive-a"),
                Set.of("application/pdf", "text/plain"),
                1_048_576L);
        resolver.mint("key-full", minted);

        assertThat(resolver.resolve("key-full")).hasValueSatisfying(scope -> {
            assertThat(scope.accountId()).isEqualTo("acct-full");
            assertThat(scope.datasourceIds()).containsExactlyInAnyOrder("ds-1", "ds-2");
            assertThat(scope.drives()).containsExactly("drive-a");
            assertThat(scope.mimeTypes())
                    .containsExactlyInAnyOrder("application/pdf", "text/plain");
            assertThat(scope.maxPayloadBytes()).isEqualTo(1_048_576L);
        });
    }

    @Test
    void emptySetsRoundTripAsUnrestricted() {
        resolver.mint("key-open", IntakeScope.unrestricted("acct-open"));

        assertThat(resolver.resolve("key-open")).hasValueSatisfying(scope -> {
            assertThat(scope.datasourceIds()).isEmpty();
            assertThat(scope.drives()).isEmpty();
            assertThat(scope.mimeTypes()).isEmpty();
            assertThat(scope.maxPayloadBytes()).isZero();
            // Empty means unrestricted within the account, and the mapped
            // scope must behave that way, not just look that way.
            assertThat(scope.allowsDatasource("any-ds")).isTrue();
            assertThat(scope.allowsDrive("any-drive")).isTrue();
            assertThat(scope.allowsMimeType("application/anything")).isTrue();
            assertThat(scope.allowsPayloadSize(Long.MAX_VALUE)).isTrue();
        });
    }

    @Test
    void unknownKeyResolvesEmpty() {
        assertThat(resolver.resolve("never-minted")).isEmpty();
    }

    @Test
    void revokedKeyResolvesEmptyFromThatInstant() {
        resolver.mint("key-doomed", IntakeScope.unrestricted("acct-r"));
        assertThat(resolver.resolve("key-doomed")).isPresent();

        resolver.revoke("key-doomed");
        assertThat(resolver.resolve("key-doomed")).isEmpty();

        // Revoking again, or revoking a key that never existed, is a no-op —
        // the same idempotent semantics as the in-memory resolver.
        resolver.revoke("key-doomed");
        resolver.revoke("never-minted");
    }

    @Test
    void twoLiveKeysForOneAccountIsTheRotationGraceWindow() {
        resolver.mint("key-old", IntakeScope.unrestricted("acct-rotating"));
        resolver.mint("key-new", IntakeScope.unrestricted("acct-rotating"));

        // The grace window: both credentials are valid, both carry the same
        // account.
        Optional<IntakeScope> old = resolver.resolve("key-old");
        Optional<IntakeScope> fresh = resolver.resolve("key-new");
        assertThat(old).isPresent();
        assertThat(fresh).isPresent();
        assertThat(old.orElseThrow().accountId())
                .isEqualTo(fresh.orElseThrow().accountId());

        // The window ends: revoke the old key, the new one stays live.
        resolver.revoke("key-old");
        assertThat(resolver.resolve("key-old")).isEmpty();
        assertThat(resolver.resolve("key-new")).isPresent();
    }

    @Test
    void duplicateMintIsRejectedWithoutNamingAnythingSecret() {
        resolver.mint("key-dupe", IntakeScope.unrestricted("acct-a"));

        assertThatThrownBy(() -> resolver.mint("key-dupe", IntakeScope.unrestricted("acct-b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("key-dupe")
                .hasMessageNotContaining(JdbcApiKeyIdentityResolver.sha256Hex("key-dupe"));
    }

    @Test
    void rawKeyMaterialNeverLandsInTheTable() throws SQLException {
        String credential = "raw-credential-that-must-never-be-stored";
        resolver.mint(credential, IntakeScope.unrestricted("acct-hash"));

        try (Connection c = verificationConnection()) {
            // The stored key is exactly the lowercase hex SHA-256 of the
            // credential, nothing else.
            try (PreparedStatement select =
                            c.prepareStatement("SELECT key_hash FROM intake_api_key");
                    ResultSet row = select.executeQuery()) {
                assertThat(row.next()).isTrue();
                assertThat(row.getString("key_hash"))
                        .isEqualTo(JdbcApiKeyIdentityResolver.sha256Hex(credential));
                assertThat(row.next()).isFalse();
            }

            // And the raw credential appears NOWHERE in the row — every
            // column rendered as text, searched as one string.
            try (PreparedStatement search = c.prepareStatement(
                            "SELECT count(*) FROM intake_api_key t"
                                    + " WHERE t::text LIKE '%' || ? || '%'")) {
                search.setString(1, credential);
                try (ResultSet count = search.executeQuery()) {
                    assertThat(count.next()).isTrue();
                    assertThat(count.getLong(1)).isZero();
                }
            }
        }
    }

    @Test
    void blankCredentialAndNullScopeAreRejectedAtMint() {
        assertThatThrownBy(() -> resolver.mint("  ", IntakeScope.unrestricted("acct")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credential");
        assertThatThrownBy(() -> resolver.mint("key-x", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scope");
    }

    @Test
    void bootFailsFastWhenTheDatabaseIsUnreachable() {
        // Port 1 answers nothing: Flyway's migrate must blow up construction,
        // and the half-started pool must not leak (the constructor closes it).
        IntakeKeyStoreConfig dead = new IntakeKeyStoreConfig(
                "jdbc:postgresql://localhost:1/intakekeys", "keys", "keys");
        assertThatThrownBy(() -> new JdbcApiKeyIdentityResolver(dead))
                .isInstanceOf(RuntimeException.class);
    }
}

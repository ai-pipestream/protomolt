package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ManagedBackendLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    static LedgerDatabase database;
    static Tx tx;
    static ManagedBackendLedger ledger;

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new ManagedBackendLedger(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void restartingWithSamePhysicalProfileIsIdempotentButRedirectIsRejected() {
        String id = UUID.randomUUID().toString();
        var original = profile("https://STORE.example:443/");
        ledger.bind(id, original);
        new ManagedBackendLedger(tx).bind(id, profile("https://store.example"));
        assertThatThrownBy(() -> ledger.bind(id, profile("https://other.example")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("another physical profile");
        assertThat(ledger.find(id)).contains(original);
    }

    @Test void providerOwnedIdentityNeedsNoS3FieldsAndCannotBeRedirected() {
        String id = UUID.randomUUID().toString();
        // A persistence fixture, not a qualified filesystem provider.
        var original = new ManagedBackendLedger.Profile(new ai.protomolt.proto.repo.blob.spi.BackendIdentity(
                "filesystem", "filesystem/v1", java.util.Map.of("root", "/archive")), "filesystem-realm");
        ledger.bind(id, original);
        assertThat(ledger.find(id)).contains(original);
        ledger.bind(id, original);
        assertThatThrownBy(() -> ledger.bind(id, new ManagedBackendLedger.Profile(
                new ai.protomolt.proto.repo.blob.spi.BackendIdentity("filesystem", "filesystem/v1",
                        java.util.Map.of("root", "/other")), "filesystem-realm")))
                .isInstanceOf(IllegalStateException.class);
        tx.readOnly(em -> {
            var row = (Object[]) em.createNativeQuery("SELECT endpoint,region,path_style,identity_schema FROM managed_backend_profiles WHERE generation=:id")
                    .setParameter("id", id).getSingleResult();
            assertThat(row).containsExactly(null, null, null, "filesystem/v1");
            return null;
        });
    }

    @Test void conflictingConcurrentRegistrationsHaveOneWinner() throws Exception {
        String id = UUID.randomUUID().toString();
        var start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Callable<Boolean> first = () -> bindAfter(start, id, profile("https://one.example"));
            Callable<Boolean> second = () -> bindAfter(start, id, profile("https://two.example"));
            var a = executor.submit(first);
            var b = executor.submit(second);
            start.countDown();
            assertThat(java.util.List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
            assertThat(ledger.find(id)).isPresent();
        }
    }

    @Test void databaseRejectsUpdatingOrDeletingBoundGeneration() {
        String id = UUID.randomUUID().toString();
        var original = profile("http://store.example");
        ledger.bind(id, original);
        for (String sql : java.util.List.of("DELETE FROM managed_backend_profiles WHERE generation=:id",
                "UPDATE managed_backend_profiles SET storage_realm='different' WHERE generation=:id")) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery(sql).setParameter("id", id).executeUpdate();
            })).isInstanceOf(RuntimeException.class).hasStackTraceContaining("Managed backend generations are immutable");
        }
        assertThat(ledger.find(id)).contains(original);
    }

    @Test void sdkDefaultIsPinnedSeparatelyFromAnExplicitEndpoint() {
        String id = UUID.randomUUID().toString();
        var defaults = profile(ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.SDK_DEFAULT);
        ledger.bind(id, defaults);
        ledger.bind(id, defaults);
        assertThatThrownBy(() -> ledger.bind(id, profile("https://s3.us-east-1.amazonaws.com")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(ledger.find(id)).contains(defaults);
    }

    @Test void sensitiveEndpointComponentsAreRejectedWithoutEchoingValues() {
        for (String endpoint : java.util.List.of("https://user:secret@store.example", "https://store.example?token=secret",
                "https://store.example#secret", "https://store.example/secret")) {
            assertThatThrownBy(() -> profile(endpoint)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageNotContaining("secret");
        }
    }

    private static boolean bindAfter(CountDownLatch start, String id, ManagedBackendLedger.Profile profile) throws Exception {
        if (!start.await(5, TimeUnit.SECONDS)) throw new AssertionError("registration barrier timed out");
        try { ledger.bind(id, profile); return true; }
        catch (IllegalStateException conflict) {
            assertThat(conflict).hasMessage("Managed backend generation is already bound to another physical profile");
            return false;
        }
    }

    private static ManagedBackendLedger.Profile profile(String endpoint) {
        return new ManagedBackendLedger.Profile(ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of(endpoint, "us-east-1", true), "test-realm");
    }
}

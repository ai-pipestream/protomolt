package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RepositoryCredentialAuthoritiesIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("operator", true);
    private static RepositoryCredentialBinding key() { return new RepositoryCredentialBinding("test-issuer", UUID.randomUUID(), 1); }
    private static RepositoryCaller caller(RepositoryCredentialBinding key) {
        return new RepositoryCaller("principal", false, Set.of("account"), Set.of(), Optional.of(key));
    }
    private static void live(Context c, RepositoryCredentialBinding key) {
        check(c, caller(key));
    }
    private static void check(Context c, RepositoryCaller caller) {
        c.tx().inTransaction(em -> { RepositoryCredentialAuthorities.requireLive(em, caller); });
    }
    private static void failure(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, RepositoryException.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(RepositoryException.class, error -> assertThat(error.code()).isEqualTo(code));
    }

    @Test void exactRegistrationDoesNotChangeIdentityOrReviveRevocation() {
        try (var c = context(POSTGRES)) {
            var ledger = new RepositoryCredentialAuthorities(c.tx()); var key = key();
            ledger.register(ADMIN, key, "principal"); ledger.register(ADMIN, key, "principal"); live(c, key);
            failure(() -> ledger.register(ADMIN, key, "other"), RepositoryException.Code.CONFLICT);
            ledger.revoke(ADMIN, key, "principal"); ledger.revoke(ADMIN, key, "principal");
            failure(() -> live(c, key), RepositoryException.Code.UNAUTHENTICATED);
            failure(() -> ledger.register(ADMIN, key, "principal"), RepositoryException.Code.UNAUTHENTICATED);
            assertThat(count(c, "repository_credential_authorities")).isEqualTo(1);
        }
    }

    @Test void rotationFencesOldGenerationAndStaleRequestsCannotRevokeTheNewOne() {
        try (var c = context(POSTGRES)) {
            var ledger = new RepositoryCredentialAuthorities(c.tx()); var old = key();
            ledger.register(ADMIN, old, "principal"); ledger.revoke(ADMIN, old, "principal");
            var next = ledger.rotate(ADMIN, old, "principal");
            assertThat(next.generation()).isEqualTo(2); live(c, next);
            failure(() -> live(c, old), RepositoryException.Code.UNAUTHENTICATED);
            failure(() -> ledger.rotate(ADMIN, old, "principal"), RepositoryException.Code.CONFLICT);
            failure(() -> ledger.revoke(ADMIN, old, "principal"), RepositoryException.Code.NOT_FOUND);
            live(c, next);
        }
    }

    @Test void absentCredentialsAndUntrustedProvisioningAreRefused() {
        try (var c = context(POSTGRES)) {
            var ledger = new RepositoryCredentialAuthorities(c.tx()); var key = key();
            failure(() -> ledger.revoke(ADMIN, key, "principal"), RepositoryException.Code.NOT_FOUND);
            failure(() -> ledger.register(caller(key), key, "principal"), RepositoryException.Code.PERMISSION_DENIED);
            failure(() -> live(c, key), RepositoryException.Code.UNAUTHENTICATED);
            ledger.register(ADMIN, key, "principal");
            failure(() -> check(c, new RepositoryCaller("other", false, Set.of("account"), Set.of(), Optional.of(key))), RepositoryException.Code.UNAUTHENTICATED);
            failure(() -> check(c, new RepositoryCaller("principal", false, Set.of("account"), Set.of())), RepositoryException.Code.UNAUTHENTICATED);
            failure(() -> check(c, ADMIN), RepositoryException.Code.UNAUTHENTICATED);
        }
    }

    @Test void migrationDoesNotInventAuthorityForExistingOperations() {
        try (var c = context(POSTGRES, "98")) {
            var prepared = prepare(c, 1);
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertThat(count(c, "repository_credential_authorities")).isZero();
            assertThat(new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal", true), prepared.command()).state())
                    .isEqualTo(DocumentPublicationReplay.State.PENDING);
        }
    }

    @Test void databaseRejectsIdentityRewriteGenerationRollbackAndTombstoneDeletion() {
        try (var c = context(POSTGRES)) {
            var ledger = new RepositoryCredentialAuthorities(c.tx()); var key = key();
            ledger.register(ADMIN, key, "principal"); var next = ledger.rotate(ADMIN, key, "principal");
            ledger.revoke(ADMIN, next, "principal");
            var faults = java.util.Map.of(
                    "UPDATE repository_credential_authorities SET principal='other'", "identity is immutable",
                    "UPDATE repository_credential_authorities SET generation=1", "next active generation",
                    "UPDATE repository_credential_authorities SET revoked=false", "cannot be revived",
                    "UPDATE repository_credential_authorities SET generation=4,revoked=false", "next active generation",
                    "DELETE FROM repository_credential_authorities", "cannot be deleted");
            for (var fault : faults.entrySet()) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(fault.getKey()).executeUpdate(); }))
                        .isInstanceOf(RuntimeException.class).hasStackTraceContaining(fault.getValue());
            }
            failure(() -> live(c, next), RepositoryException.Code.UNAUTHENTICATED);
        }
    }

    @Test void unsupportedIsolationCannotCheckOrRegisterEvenAnAbsentKey() {
        try (var c = context(POSTGRES)) {
            var key = key();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").executeUpdate();
                RepositoryCredentialAuthorities.requireLive(em, caller(key));
            })).hasStackTraceContaining("READ COMMITTED");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").executeUpdate();
                em.createNativeQuery("INSERT INTO repository_credential_authorities VALUES ('issuer',:id,1,'principal',false)")
                        .setParameter("id", key.credentialId()).executeUpdate();
            })).hasStackTraceContaining("READ COMMITTED");
            assertThat(count(c, "repository_credential_authorities")).isZero();
        }
    }

    @Test void concurrentReadersOverlapAndRevocationWaitsForTheirDecision() throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var ledger = new RepositoryCredentialAuthorities(c.tx()); var key = key(); ledger.register(ADMIN, key, "principal");
            var locked = new java.util.concurrent.CompletableFuture<Integer>();
            var release = new java.util.concurrent.CompletableFuture<Void>();
            var first = workers.submit(() -> c.tx().inTransaction(em -> {
                RepositoryCredentialAuthorities.requireLive(em, caller(key));
                locked.complete(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                release.join(); return null;
            }));
            try {
                int pid = locked.get(5, TimeUnit.SECONDS);
                workers.submit(() -> live(c, key)).get(5, TimeUnit.SECONDS); // Must not wait on the first shared reader.
                var revoke = workers.submit(() -> ledger.revoke(ADMIN, key, "principal"));
                long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(5); boolean waiting = false;
                while (!waiting && System.nanoTime()<deadline) {
                    waiting = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))")
                            .setParameter("pid", pid).getSingleResult()).longValue()>0);
                    if (!waiting) Thread.sleep(10);
                }
                assertThat(waiting).as("revoke is blocked by the real shared authorization transaction").isTrue();
                assertThat(revoke.isDone()).isFalse();
                release.complete(null); first.get(5, TimeUnit.SECONDS); revoke.get(5, TimeUnit.SECONDS);
                failure(() -> live(c, key), RepositoryException.Code.UNAUTHENTICATED);
            } finally { release.complete(null); }
        }
    }
}

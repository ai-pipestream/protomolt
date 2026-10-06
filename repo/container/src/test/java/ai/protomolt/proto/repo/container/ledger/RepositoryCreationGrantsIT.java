package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL authorization records and admission races; no provider execution is implied. */
@Testcontainers
class RepositoryCreationGrantsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("operator", true);
    record Fixture(RepositoryCreationGrants grants, RepositoryCreationGrants.Prepared grant) {}

    private static Fixture fixture(Context c) {
        var source = prepare(c, 1);
        var member = source.command().intent().getMembers(0);
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .setMembers(0, member.toBuilder().setDestination(member.getDestination().toBuilder().setIfAbsent(true)
                        .setAddress(member.getDestination().getAddress().toBuilder().setDocId("created-by-grant")))).build());
        var key = new RepositoryCredentialBinding("test-issuer", UUID.randomUUID(), 1);
        var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of(), Optional.of(key));
        new RepositoryCredentialAuthorities(c.tx()).register(ADMIN, key, caller.principalName());
        var drives = new DriveLedger(c.tx()); var drive = drives.findById(UUID.fromString(member.getDriveId())).orElseThrow();
        var placement = DocumentUploadPlan.Placement.sample(drive, "native-test", new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        return new Fixture(new RepositoryCreationGrants(c.tx(), drives), RepositoryCreationGrants.prepare(caller, command,
                Map.of(drive.driveId, placement), now(c)+TimeUnit.MINUTES.toMicros(5)));
    }
    private static long now(Context c) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint")
                .getSingleResult()).longValue());
    }
    private static void live(Context c, RepositoryCreationGrants.Prepared grant) {
        c.tx().inTransaction(em -> { RepositoryCreationGrants.requireLive(em, grant.grantee(), grant.command(), grant.placementSha256()); });
    }
    private static void denied(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOf(RepositoryException.class);
    }

    @Test void migrationPreservesExistingScopesWithoutInventingCreationAuthority() {
        try (var c = context(POSTGRES, "99")) {
            var existing = prepare(c, 1);
            long scopes = count(c, "repository_execution_scopes");
            assertThat(scopes).isPositive();
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertThat(count(c, "repository_execution_scopes")).isEqualTo(scopes);
            long stamped = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_execution_scopes WHERE scope_created_xid IS NOT NULL")
                    .getSingleResult()).longValue());
            assertThat(stamped).isZero();
            assertThat(count(c, "repository_creation_grants")).isZero();
            assertThat(new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal", true), existing.command()).state())
                    .isEqualTo(DocumentPublicationReplay.State.PENDING);
        }
    }

    @Test void exactGrantSurvivesClaimAdmissionButCannotChangeIdentityOrReviveRevocation() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var g = f.grant();
            f.grants().install(ADMIN, g); live(c, g);
            new RepositoryExecutionClaimLedger(c.tx()).acquire(g.key(), g.command(), UUID.randomUUID(), Duration.ofMinutes(1));
            f.grants().install(ADMIN, g);
            var extended = RepositoryCreationGrants.prepare(g.grantee(), g.command(), g.placements(), g.expiresAtEpochMicros()+1);
            denied(() -> f.grants().install(ADMIN, extended));
            var changed = new DocumentPublicationCommand(g.command().intent().toBuilder().setMembers(0,
                    g.command().intent().getMembers(0).toBuilder().setMemberId("changed")).build());
            denied(() -> f.grants().install(ADMIN, RepositoryCreationGrants.prepare(g.grantee(), changed, g.placements(), g.expiresAtEpochMicros())));
            f.grants().revoke(ADMIN, g.key()); f.grants().revoke(ADMIN, g.key());
            denied(() -> live(c, g)); denied(() -> f.grants().install(ADMIN, g));
            assertThat(count(c, "repository_creation_grants")).isEqualTo(1);
        }
    }

    @Test void installationRefusesUntrustedCallerAndRollsBackNewScopeOnRevokedKeyOrChangedPlacement() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var g = f.grant(); long scopes = count(c, "repository_execution_scopes");
            denied(() -> f.grants().install(g.grantee(), g));
            new RepositoryCredentialAuthorities(c.tx()).revoke(ADMIN, g.grantee().credentialBinding().orElseThrow(), "principal");
            denied(() -> f.grants().install(ADMIN, g));
            assertThat(count(c, "repository_execution_scopes")).isEqualTo(scopes);
            assertThat(count(c, "repository_creation_grants")).isZero();
        }
        try (var c = context(POSTGRES)) {
            var f = fixture(c); long scopes = count(c, "repository_execution_scopes");
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE drives SET prefix='changed'").executeUpdate(); });
            assertThatThrownBy(() -> f.grants().install(ADMIN, f.grant())).hasMessageContaining("drive changed");
            assertThat(count(c, "repository_execution_scopes")).isEqualTo(scopes);
        }
    }

    @Test void orphanScopeCannotGainAGrantAndSuppliedTransactionStampIsOverwritten() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var g = f.grant();
            c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO repository_execution_scopes VALUES (:a,:p,:o,true,'1'::xid8)")
                        .setParameter("a", g.key().account()).setParameter("p", g.key().principal()).setParameter("o", g.key().operationId()).executeUpdate();
                assertThat(em.createNativeQuery("SELECT scope_created_xid=pg_current_xact_id() FROM repository_execution_scopes WHERE operation_id=:o")
                        .setParameter("o", g.key().operationId()).getSingleResult()).isEqualTo(true);
            });
            denied(() -> f.grants().install(ADMIN, g));
            assertThat(count(c, "repository_creation_grants")).isZero();
        }
    }

    @Test void expiryAndRotationInvalidateLiveGrantWithoutRewritingIt() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var g = f.grant();
            var expired = RepositoryCreationGrants.prepare(g.grantee(), g.command(), g.placements(), now(c)-1);
            assertThatThrownBy(() -> f.grants().install(ADMIN, expired)).hasStackTraceContaining("future expiry");
            f.grants().install(ADMIN, g);
            var next = new RepositoryCredentialAuthorities(c.tx()).rotate(ADMIN, g.grantee().credentialBinding().orElseThrow(), "principal");
            denied(() -> live(c, g));
            var rotated = new RepositoryCaller("principal", false, Set.of("account"), Set.of(), Optional.of(next));
            denied(() -> f.grants().install(ADMIN, RepositoryCreationGrants.prepare(rotated, g.command(), g.placements(), g.expiresAtEpochMicros())));
            assertThat(count(c, "repository_creation_grants")).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void firstAdmissionWinnerPreventsRetroactiveGrant(boolean claimed) throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            c.pool().setMaximumPoolSize(5);
            var f = fixture(c); var g = f.grant();
            var ready = new java.util.concurrent.CompletableFuture<Integer>(); var release = new java.util.concurrent.CompletableFuture<Void>();
            var admission = workers.submit(() -> c.tx().inTransaction(em -> {
                if (claimed) RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, g.key(), g.command(), UUID.randomUUID(), Duration.ofMinutes(1));
                else RepositoryOperationLedger.prepareAdmission(g.key(), g.command(), UUID.randomUUID(), Duration.ofMinutes(1)).apply(em, null);
                ready.complete(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                release.join(); return null;
            }));
            try {
                int blocker = ready.get(5, TimeUnit.SECONDS);
                var install = workers.submit(() -> f.grants().install(ADMIN, g));
                awaitBlocked(c, blocker); release.complete(null); admission.get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> install.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(RepositoryException.class);
                assertThat(count(c, "repository_creation_grants")).isZero();
            } finally { release.complete(null); }
        }
    }

    @Test void grantInsertionWinnerOrdersClaimAdmissionWithoutLockingExistingScopes() throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
                var barrier = c.pool().getConnection()) {
            c.pool().setMaximumPoolSize(5);
            var f = fixture(c); var g = f.grant();
            barrier.setAutoCommit(false);
            int blocker;
            try (var statement = barrier.createStatement()) {
                var row = statement.executeQuery("SELECT pg_backend_pid()"); row.next(); blocker = row.getInt(1);
                statement.executeUpdate("UPDATE repository_credential_authorities SET revoked=false");
            }
            try {
                var install = workers.submit(() -> f.grants().install(ADMIN, g));
                int installer = awaitBlocked(c, blocker);
                var claim = workers.submit(() -> new RepositoryExecutionClaimLedger(c.tx()).acquire(g.key(), g.command(), UUID.randomUUID(), Duration.ofMinutes(1)));
                awaitBlocked(c, installer);
                barrier.commit(); install.get(5, TimeUnit.SECONDS); claim.get(5, TimeUnit.SECONDS);
                f.grants().install(ADMIN, g); live(c, g);
                assertThat(count(c, "repository_creation_grants")).isEqualTo(1);
            } finally { barrier.rollback(); }
        }
    }

    @Test void expiryIsCheckedAfterWaitingForTheGrantRow() throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
                var barrier = c.pool().getConnection()) {
            c.pool().setMaximumPoolSize(5);
            var f = fixture(c); var original = f.grant();
            var g = RepositoryCreationGrants.prepare(original.grantee(), original.command(), original.placements(),
                    now(c)+TimeUnit.SECONDS.toMicros(10));
            f.grants().install(ADMIN, g);
            live(c, g);
            barrier.setAutoCommit(false);
            int blocker;
            try (var statement = barrier.createStatement()) {
                var row = statement.executeQuery("SELECT pg_backend_pid()"); row.next(); blocker = row.getInt(1);
                statement.executeUpdate("UPDATE repository_creation_grants SET revoked=false");
            }
            try {
                var check = workers.submit(() -> live(c, g));
                awaitBlocked(c, blocker);
                assertThat(now(c)).isLessThan(g.expiresAtEpochMicros());
                // Wait against the database clock while the reader is proven blocked on the grant.
                long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
                while (now(c)<=g.expiresAtEpochMicros() && System.nanoTime()<deadline) Thread.sleep(10);
                assertThat(now(c)).isGreaterThan(g.expiresAtEpochMicros());
                barrier.commit();
                assertThatThrownBy(() -> check.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(RepositoryException.class)
                        .satisfies(error -> assertThat(((RepositoryException) error.getCause()).code())
                                .isEqualTo(RepositoryException.Code.NOT_FOUND));
            } finally { barrier.rollback(); }
        }
    }

    @Test void sqlGuardRejectsLateInstallationMutationDeletionAndRevival() {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var g = f.grant(); f.grants().install(ADMIN, g);
            var late = UUID.randomUUID();
            c.tx().inTransaction(em -> { em.createNativeQuery("""
                    INSERT INTO repository_execution_scopes(account_id,principal,operation_id,claim_required)
                    VALUES ('account','principal',:o,true)
                    """).setParameter("o", late).executeUpdate(); });
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery("""
                    INSERT INTO repository_creation_grants
                    SELECT account_id,principal,:o,issuer,credential_id,credential_generation,command_codec,
                      command_version,command_sha256,placement_version,placement_sha256,expires_at_epoch_micros,revoked
                    FROM repository_creation_grants
                    """).setParameter("o", late).executeUpdate(); }))
                    .hasStackTraceContaining("requires a new unadmitted scope");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_creation_grants SET expires_at_epoch_micros=expires_at_epoch_micros+1").executeUpdate();
            })).hasStackTraceContaining("identity is immutable");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM repository_creation_grants").executeUpdate();
            })).hasStackTraceContaining("tombstones cannot be deleted");
            f.grants().revoke(ADMIN, g.key());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_creation_grants SET revoked=false").executeUpdate();
            })).hasStackTraceContaining("revocation is terminal");
            assertThat(count(c, "repository_creation_grants")).isEqualTo(1);
        }
    }

    private static int awaitBlocked(Context c, int blocker) throws Exception {
        long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime()<deadline) {
            var rows = c.tx().readOnly(em -> em.createNativeQuery("SELECT pid FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))")
                    .setParameter("pid", blocker).getResultList());
            if (!rows.isEmpty()) return ((Number) rows.getFirst()).intValue();
            Thread.sleep(10);
        }
        throw new AssertionError("No PostgreSQL waiter for expected blocker " + blocker);
    }
}

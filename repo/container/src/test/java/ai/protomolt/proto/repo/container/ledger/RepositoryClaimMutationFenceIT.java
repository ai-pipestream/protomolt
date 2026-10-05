package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RepositoryClaimMutationFenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final Duration LEASE = Duration.ofMinutes(1);

    @Test void sameOwnerNonceCannotBypassTransferredClaim() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claims = new RepositoryExecutionClaimLedger(c.tx()); var operations = new RepositoryOperationLedger(c.tx());
            var first = claims.acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1));
            var owner = operations.admit(key, command, UUID.randomUUID(), LEASE, first).owner().orElseThrow();
            assertThat(operations.renew(owner, LEASE).executionClaim()).contains(first);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var next = claims.takeOver(key, command, 1, UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> operations.renew(owner, LEASE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var stripped = new RepositoryOperationLedger.Owner(key, owner.generation(), owner.token(), owner.leaseUntil());
            assertThatThrownBy(() -> operations.renew(stripped, LEASE)).hasStackTraceContaining("live execution claim write fence");
            var current = owner.withClaim(next);
            assertThat(operations.renew(current, LEASE).token()).isEqualTo(owner.token());
            c.tx().inTransaction(em -> { RepositoryOperationLedger.fenceLiveOwner(em, current); return null; });
            assertThatThrownBy(() -> operations.admit(key, command, owner.token(), LEASE))
                    .hasStackTraceContaining("live execution claim write fence");
        }
    }

    @Test void currentClaimCannotResurrectExpiredOperationOwner() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claims = new RepositoryExecutionClaimLedger(c.tx()); var operations = new RepositoryOperationLedger(c.tx());
            var claim = claims.acquire(key, command, UUID.randomUUID(), LEASE);
            var owner = operations.admit(key, command, UUID.randomUUID(), Duration.ofSeconds(1), claim).owner().orElseThrow();
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThatThrownBy(() -> operations.renew(owner, LEASE)).isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            var nextToken = UUID.randomUUID();
            var taken = operations.takeOver(key, command, 1, nextToken, LEASE, claim);
            assertThat(taken.generation()).isEqualTo(2); assertThat(taken.executionClaim()).contains(claim);
            assertThat(operations.takeOver(key, command, 1, nextToken, Duration.ofHours(1), claim)).isEqualTo(taken);
        }
    }

    @Test void tokenlessSqlUpdateCannotMintAClaimFenceAfterTransfer() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claims = new RepositoryExecutionClaimLedger(c.tx());
            var first = claims.acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1));
            var owner = new RepositoryOperationLedger(c.tx()).admit(key, command, UUID.randomUUID(), LEASE, first).owner().orElseThrow();
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var current = claims.takeOver(key, command, 1, UUID.randomUUID(), LEASE);
            var stripped = new RepositoryOperationLedger.Owner(key, owner.generation(), owner.token(), owner.leaseUntil());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_execution_claims SET lease_until=lease_until WHERE operation_id=:id")
                        .setParameter("id", key.operationId()).executeUpdate();
                return RepositoryOperationLedger.fenceLiveOwner(em, stripped);
            })).hasStackTraceContaining("explicit claim identity");
            for (String invalid : java.util.List.of("write_fence_xid=write_fence_xid", "write_fence_xid=pg_current_xact_id()",
                    "fence_epoch=1,fence_token='" + current.token() + "'",
                    "fence_epoch=2,fence_token='" + first.token() + "'")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    return em.createNativeQuery("UPDATE repository_execution_claims SET " + invalid + " WHERE operation_id=:id")
                            .setParameter("id", key.operationId()).executeUpdate();
                })).hasStackTraceContaining("explicit claim identity");
            }
            c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_execution_claims SET fence_epoch=:epoch,fence_token=:token WHERE operation_id=:id")
                        .setParameter("epoch", current.epoch()).setParameter("token", current.token())
                        .setParameter("id", key.operationId()).executeUpdate();
                RepositoryOperationLedger.fenceLiveOwner(em, stripped);
                assertThat(em.createNativeQuery("SELECT fence_epoch IS NULL AND fence_token IS NULL FROM repository_execution_claims WHERE operation_id=:id")
                        .setParameter("id", key.operationId()).getSingleResult()).isEqualTo(true);
                return null;
            });
        }
    }

    @Test void previouslyStampedTransactionCannotWriteAfterClaimExpiry() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1));
            var owner = new RepositoryOperationLedger(c.tx()).admit(key, command, UUID.randomUUID(), LEASE, claim).owner().orElseThrow();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult();
                return em.createNativeQuery("SELECT require_repository_operation_write_fence(:a,:p,:o,1)")
                        .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getSingleResult();
            })).hasStackTraceContaining("live execution claim write fence");
        }
    }

    @Test void ownerRowLockWaitRechecksClaimExpiry() throws Exception {
        try (var c = context(POSTGRES); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
                var blocker = c.emf().createEntityManager()) {
            var command = command(c); var key = key(command);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1));
            var owner = new RepositoryOperationLedger(c.tx()).admit(key, command, UUID.randomUUID(), LEASE, claim).owner().orElseThrow();
            blocker.getTransaction().begin();
            blocker.createNativeQuery("SELECT 1 FROM repository_operation_owners WHERE operation_id=:id FOR UPDATE")
                    .setParameter("id", key.operationId()).getSingleResult();
            int blockerPid = ((Number) blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
            var pid = new java.util.concurrent.CompletableFuture<Integer>();
            var write = executor.submit(() -> c.tx().inTransaction(em -> {
                pid.complete(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                return RepositoryOperationLedger.fenceLiveOwner(em, owner);
            }));
            try {
                int waiter = pid.get(10, TimeUnit.SECONDS); boolean blocked = false;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (System.nanoTime() < deadline) {
                    blocked = c.tx().readOnly(em -> Boolean.TRUE.equals(em.createNativeQuery(
                            "SELECT :blocker = ANY(pg_blocking_pids(:waiter))")
                            .setParameter("blocker", blockerPid).setParameter("waiter", waiter).getSingleResult()));
                    if (blocked) break;
                    Thread.sleep(10);
                }
                assertThat(blocked).isTrue();
                c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            } finally { blocker.getTransaction().rollback(); }
            assertThatThrownBy(() -> write.get(10, TimeUnit.SECONDS)).hasStackTraceContaining("live execution claim write fence");
        }
    }

    @Test void unclaimedOperationCannotBeRetroactivelyAdopted() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var operations = new RepositoryOperationLedger(c.tx());
            var owner = operations.admit(key, command, UUID.randomUUID(), LEASE).owner().orElseThrow();
            assertThatThrownBy(() -> new RepositoryExecutionClaimLedger(c.tx()).acquire(key, command, UUID.randomUUID(), LEASE))
                    .hasStackTraceContaining("Unclaimed operation cannot acquire");
            assertThat(operations.renew(owner, LEASE).executionClaim()).isEmpty();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                return em.createNativeQuery("UPDATE repository_execution_scopes SET claim_required=true").executeUpdate();
            })).hasStackTraceContaining("execution scope is immutable");
        }
    }

    @Test void deferredSuccessFinalizationRechecksClaimBeforeCommit() {
        try (var c = context(POSTGRES)) {
            var source = prepare(c, 1);
            var command = new DocumentPublicationCommand(source.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key(command), command, UUID.randomUUID(), Duration.ofSeconds(1));
            var owner = new RepositoryOperationLedger(c.tx()).admit(key(command), command, UUID.randomUUID(), LEASE, claim).owner().orElseThrow();
            var driveId = UUID.fromString(command.intent().getMembers(0).getDriveId());
            var drives = new DriveLedger(c.tx());
            var placement = DocumentUploadPlan.Placement.sample(drives.findById(driveId).orElseThrow(), "native-test",
                    new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
            new DocumentOperationUploadAdmission(c.tx(), drives).admit(
                    new ai.protomolt.proto.repo.spi.RepositoryCaller("principal", true), owner,
                    DocumentOperationUploadAdmission.prepare(command, java.util.Map.of(driveId, placement), java.util.Map.of(), LEASE));
            var prepared = new Prepared(command, owner, source.sources(), source.uploads());
            assertThatThrownBy(() -> publish(c, prepared, Fault.NONE, false, (em, revision) -> {}, em -> {}, em -> {
                em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult();
            })).hasStackTraceContaining("live execution claim write fence");
            long successes = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_operation_success WHERE operation_id=:id")
                    .setParameter("id", command.operationId()).getSingleResult()).longValue());
            assertThat(successes).isZero();
        }
    }

    @Test void firstAdmissionAndClaimRegistrationRaceHasOnePermanentMode() throws Exception {
        for (boolean claimWins : new boolean[]{true, false}) {
            try (var c = context(POSTGRES); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
                    var winner = c.emf().createEntityManager()) {
                var command = command(c);
                winner.getTransaction().begin();
                if (claimWins) rawClaim(winner, command); else rawOperation(winner, command);
                int winnerPid = ((Number) winner.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                var pid = new java.util.concurrent.CompletableFuture<Integer>();
                var loser = executor.submit(() -> c.tx().inTransaction(em -> {
                    pid.complete(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                    if (claimWins) rawOperation(em, command); else rawClaim(em, command);
                    return null;
                }));
                try {
                    int waiter = pid.get(10, TimeUnit.SECONDS); boolean blocked = false;
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> Boolean.TRUE.equals(em.createNativeQuery(
                                "SELECT :blocker = ANY(pg_blocking_pids(:waiter))")
                                .setParameter("blocker", winnerPid).setParameter("waiter", waiter).getSingleResult()));
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).isTrue(); winner.getTransaction().commit();
                    assertThatThrownBy(() -> loser.get(10, TimeUnit.SECONDS)).hasStackTraceContaining(claimWins
                            ? "live execution claim write fence" : "Unclaimed operation cannot acquire");
                    boolean mode = c.tx().readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT claim_required FROM repository_execution_scopes WHERE operation_id=:id")
                            .setParameter("id", command.operationId()).getSingleResult());
                    assertThat(mode).isEqualTo(claimWins);
                } finally { if (winner.getTransaction().isActive()) winner.getTransaction().rollback(); }
            }
        }
    }

    @Test void claimedUploadAdmissionRequiresTheCurrentClaimAndOwnerStamp() {
        try (var c = context(POSTGRES)) {
            var source = prepare(c, 2, true);
            var command = new DocumentPublicationCommand(source.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            var key = key(command);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key, command, UUID.randomUUID(), LEASE);
            var owner = new RepositoryOperationLedger(c.tx()).admit(key, command, UUID.randomUUID(), LEASE, claim).owner().orElseThrow();
            var driveId = UUID.fromString(command.intent().getMembers(0).getDriveId());
            var drives = new DriveLedger(c.tx());
            var placement = DocumentUploadPlan.Placement.sample(drives.findById(driveId).orElseThrow(), "native-test",
                    new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
            var prepared = DocumentOperationUploadAdmission.prepare(command, java.util.Map.of(driveId, placement),
                    java.util.Map.of("member-0", UUID.randomUUID()), LEASE);
            var attempts = new DocumentOperationUploadAdmission(c.tx(), drives).admit(
                    new ai.protomolt.proto.repo.spi.RepositoryCaller("principal", true), owner, prepared);
            assertThat(attempts).hasSize(1);
            var attempt = attempts.getFirst();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                return em.createNativeQuery("UPDATE document_part_attempts SET lease_until=lease_until WHERE attempt_id=:id")
                        .setParameter("id", attempt.id()).executeUpdate();
            })).hasStackTraceContaining("live owner write fence");
            c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                assertThat(em.createNativeQuery("UPDATE document_part_attempts SET lease_until=lease_until WHERE attempt_id=:id")
                        .setParameter("id", attempt.id()).executeUpdate()).isEqualTo(1);
                return null;
            });
        }
    }

    @Test void migrationBackfillsExistingOperationsAndSeparateClaims() {
        try (var c = context(POSTGRES, "78")) {
            var existing = prepare(c, 1);
            var command = new DocumentPublicationCommand(existing.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            var token = c.tx().inTransaction(em -> { return rawClaim(em, command, false); });
            migrate(c);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key(command), command, token, LEASE);
            assertThat(new RepositoryOperationLedger(c.tx()).renew(existing.owner(), LEASE).executionClaim()).isEmpty();
            assertThat(new RepositoryOperationLedger(c.tx()).admit(key(command), command, UUID.randomUUID(), LEASE, claim)
                    .owner().orElseThrow().executionClaim()).contains(claim);
        }
    }

    @Test void migrationRefusesAmbiguousPreviouslyUnprotectedOverlap() {
        try (var c = context(POSTGRES, "78")) {
            var existing = prepare(c, 1);
            c.tx().inTransaction(em -> { return rawClaim(em, existing.command(), false); });
            assertThatThrownBy(() -> migrate(c)).hasStackTraceContaining("explicit recovery migration required");
            assertThat(new RepositoryOperationLedger(c.tx()).find(key(existing.command()))).isPresent();
        }
    }

    private static void migrate(Context c) {
        org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
    }

    private static void rawClaim(EntityManager em, DocumentPublicationCommand command) {
        rawClaim(em, command, true);
    }
    private static UUID rawClaim(EntityManager em, DocumentPublicationCommand command, boolean proof) {
        var token = UUID.randomUUID();
        String sql = proof ? """
                INSERT INTO repository_execution_claims(account_id,principal,operation_id,command_sha256,claim_epoch,claim_token,lease_until,fence_epoch,fence_token)
                VALUES (:a,'principal',:o,:digest,1,:token,clock_timestamp()+interval '1 minute',1,:token)
                """ : """
                INSERT INTO repository_execution_claims(account_id,principal,operation_id,command_sha256,claim_epoch,claim_token,lease_until)
                VALUES (:a,'principal',:o,:digest,1,:token,clock_timestamp()+interval '1 minute')
                """;
        bind(em.createNativeQuery(sql), command).setParameter("token", token).executeUpdate();
        return token;
    }
    private static void rawOperation(EntityManager em, DocumentPublicationCommand command) {
        bind(em.createNativeQuery("""
                INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,command_version,command,command_sha256)
                VALUES (:a,'principal',:o,:codec,:version,:bytes,:digest)
                """), command).setParameter("codec", DocumentPublicationCommand.CODEC)
                .setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                .setParameter("bytes", command.canonical().toByteArray()).executeUpdate();
        em.createNativeQuery("""
                INSERT INTO repository_operation_owners(account_id,principal,operation_id,owner_token,owner_generation,lease_until)
                VALUES (:a,'principal',:o,:token,1,clock_timestamp()+interval '1 minute')
                """).setParameter("a", command.intent().getAccountId()).setParameter("o", command.operationId())
                .setParameter("token", UUID.randomUUID()).executeUpdate();
    }
    private static jakarta.persistence.Query bind(jakarta.persistence.Query q, DocumentPublicationCommand c) {
        return q.setParameter("a", c.intent().getAccountId()).setParameter("o", c.operationId())
                .setParameter("digest", HexFormat.of().parseHex(c.sha256()));
    }
    private static DocumentPublicationCommand command(Context c) {
        return new DocumentPublicationCommand(prepare(c, 1).command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
    }
    private static RepositoryOperationLedger.Key key(DocumentPublicationCommand command) {
        return new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
    }
}

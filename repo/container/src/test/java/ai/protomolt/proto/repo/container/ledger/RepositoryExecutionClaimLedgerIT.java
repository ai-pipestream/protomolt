package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL claim primitive tests; no publication failover or external provider qualification. */
@Testcontainers
class RepositoryExecutionClaimLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final Duration LEASE = Duration.ofMinutes(1);

    @Test void ownerHeartbeatRenewsItsClaimWithoutChangingIdentity() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claims = new RepositoryExecutionClaimLedger(c.tx());
            var claim = claims.acquire(key, command, UUID.randomUUID(), LEASE);
            var operations = new RepositoryOperationLedger(c.tx());
            var owner = operations.admit(key, command, UUID.randomUUID(), LEASE, claim).owner().orElseThrow();
            var renewed = operations.renew(owner, Duration.ofMinutes(2));
            var renewedClaim = renewed.executionClaim().orElseThrow();
            assertThat(renewedClaim.leaseUntil()).isAfter(claim.leaseUntil());
            assertThat(renewedClaim.token()).isEqualTo(claim.token());
            assertThat(renewedClaim.epoch()).isEqualTo(claim.epoch());
            assertThat(renewedClaim.commandSha256()).isEqualTo(claim.commandSha256());
            assertThat(renewed.token()).isEqualTo(owner.token());
            assertThat(renewed.leaseUntil()).isAfter(owner.leaseUntil());
            var observed = c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, claim); });
            assertThat(observed).isEqualTo(renewedClaim);
            var wrong = new RepositoryOperationLedger.Owner(key, owner.generation(), UUID.randomUUID(),
                    owner.leaseUntil(), owner.executionClaim());
            assertThatThrownBy(() -> operations.renew(wrong, Duration.ofHours(1)))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            var afterRefusal = c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, claim); });
            assertThat(afterRefusal).isEqualTo(renewedClaim);
        }
    }

    @Test void expiredClaimCannotBeRevivedByAnOtherwiseLiveOwner() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claims = new RepositoryExecutionClaimLedger(c.tx());
            var claim = claims.acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1));
            var operations = new RepositoryOperationLedger(c.tx());
            var owner = operations.admit(key, command, UUID.randomUUID(), LEASE, claim).owner().orElseThrow();
            expire(c);
            assertThatThrownBy(() -> operations.renew(owner, Duration.ofHours(1)))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(operations.find(key).orElseThrow().leaseUntil()).isEqualTo(owner.leaseUntil());
            var successor = claims.takeOver(key, command, claim.epoch(), UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> operations.renew(owner, Duration.ofHours(1)))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var observed = c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, successor); });
            assertThat(observed).isEqualTo(successor);
        }
    }

    @Test void expiredOwnerCannotExtendItsStillLiveClaim() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command);
            var claims = new RepositoryExecutionClaimLedger(c.tx());
            var claim = claims.acquire(key, command, UUID.randomUUID(), LEASE);
            var operations = new RepositoryOperationLedger(c.tx());
            var owner = operations.admit(key, command, UUID.randomUUID(), Duration.ofSeconds(1), claim).owner().orElseThrow();
            expire(c);
            assertThatThrownBy(() -> operations.renew(owner, Duration.ofHours(1)))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            var observed = c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, claim); });
            assertThat(observed).isEqualTo(claim);
        }
    }

    @Test void exactAdmissionReplayNeverRenewsAndCannotChangeCommandOrToken() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command); var claims = new RepositoryExecutionClaimLedger(c.tx());
            var token = UUID.randomUUID(); var first = claims.acquire(key, command, token, LEASE);
            assertThat(claims.acquire(key, command, token, Duration.ofHours(1))).isEqualTo(first);
            assertThatThrownBy(() -> claims.acquire(key, command, UUID.randomUUID(), LEASE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var changed = new DocumentPublicationCommand(command.intent().toBuilder()
                    .setMembers(0, command.intent().getMembers(0).toBuilder().setClusterId("changed")).build());
            assertThatThrownBy(() -> claims.acquire(key, changed, token, LEASE))
                    .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
            assertThat(first.toString()).doesNotContain(token.toString(), key.account(), key.principal());
            assertThat(new RepositoryOperationLedger(c.tx()).find(key)).isEmpty();
        }
    }

    @Test void expiredTransferIsExplicitAndLostAcknowledgmentReplayKeepsTheLease() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command); var claims = new RepositoryExecutionClaimLedger(c.tx());
            var first = claims.acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1));
            var next = UUID.randomUUID();
            assertThatThrownBy(() -> claims.takeOver(key, command, 1, next, LEASE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            expire(c);
            assertThatThrownBy(() -> claims.acquire(key, command, first.token(), LEASE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> claims.renew(first, LEASE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> claims.takeOver(key, command, 1, first.token(), LEASE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var second = claims.takeOver(key, command, 1, next, LEASE);
            assertThat(second.epoch()).isEqualTo(2);
            assertThat(claims.takeOver(key, command, 1, next, Duration.ofHours(1))).isEqualTo(second);
            assertThatThrownBy(() -> claims.renew(first, LEASE)).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(claims.renew(second, Duration.ofMinutes(2)).leaseUntil()).isAfter(second.leaseUntil());
        }
    }

    @Test void failedFencePoisonsTransactionEvenIfCaught() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var claims = new RepositoryExecutionClaimLedger(c.tx());
            var claim = claims.acquire(key(command), command, UUID.randomUUID(), LEASE);
            var wrong = new RepositoryExecutionClaimLedger.Claim(claim.key(), claim.commandSha256(), claim.epoch(),
                    UUID.randomUUID(), claim.leaseUntil());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                assertThatThrownBy(() -> RepositoryExecutionClaimLedger.lockLive(em, wrong))
                        .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                assertThat(em.getTransaction().getRollbackOnly()).isTrue();
                return null;
            })).isInstanceOf(jakarta.persistence.RollbackException.class);
            assertThat(claims.renew(claim, LEASE).token()).isEqualTo(claim.token());
        }
    }

    @Test void liveFenceUsesOnePreparedStatementWithoutRenewingTheLease() {
        try (var c = context(POSTGRES)) {
            var command = command(c);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key(command), command, UUID.randomUUID(), LEASE);
            var statistics = c.emf().unwrap(org.hibernate.SessionFactory.class).getStatistics();
            statistics.setStatisticsEnabled(true);
            statistics.clear();
            var observed = c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, claim); });
            assertThat(observed).isEqualTo(claim);
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(1);
        }
    }

    @Test void matchingTokenAndEpochCannotFenceADifferentCommand() {
        try (var c = context(POSTGRES)) {
            var command = command(c);
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key(command), command, UUID.randomUUID(), LEASE);
            var wrongDigest = new RepositoryExecutionClaimLedger.Claim(claim.key(), "0".repeat(64),
                    claim.epoch(), claim.token(), claim.leaseUntil());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                return RepositoryExecutionClaimLedger.lockLive(em, wrongDigest);
            })).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
        }
    }

    @Test void lockWaitMustNotUseLivenessObservedBeforeWaiting() throws Exception {
        try (var c = context(POSTGRES); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var command = command(c); var claims = new RepositoryExecutionClaimLedger(c.tx());
            var claim = claims.acquire(key(command), command, UUID.randomUUID(), Duration.ofSeconds(1));
            try (var blocker = c.emf().createEntityManager()) {
                blocker.getTransaction().begin();
                RepositoryExecutionClaimLedger.lockLive(blocker, claim);
                int blockerPid = ((Number) blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                var waiterPid = new java.util.concurrent.CompletableFuture<Integer>();
                var waiting = executor.submit(() -> c.tx().inTransaction(em -> {
                    waiterPid.complete(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                    RepositoryExecutionClaimLedger.lockLive(em, claim); return null;
                }));
                try {
                    // Verify a real PostgreSQL waiter before allowing the lease to expire.
                    int pid = waiterPid.get(10, TimeUnit.SECONDS);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    boolean blocked = false;
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> Boolean.TRUE.equals(em.createNativeQuery("""
                                SELECT :blocker = ANY(pg_blocking_pids(:waiter))
                                """).setParameter("blocker", blockerPid).setParameter("waiter", pid).getSingleResult()));
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).isTrue();
                    expire(c);
                } finally { blocker.getTransaction().rollback(); }
                assertThatThrownBy(() -> waiting.get(10, TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class)
                        .hasCauseInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            }
        }
    }

    @Test void simultaneousTakeoversHaveExactlyOneWinner() throws Exception {
        try (var c = context(POSTGRES); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var command = command(c); var key = key(command); var claims = new RepositoryExecutionClaimLedger(c.tx());
            claims.acquire(key, command, UUID.randomUUID(), Duration.ofSeconds(1)); expire(c);
            var start = new java.util.concurrent.CountDownLatch(1);
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < 4; i++) tasks.add(executor.submit(() -> {
                start.await();
                try { claims.takeOver(key, command, 1, UUID.randomUUID(), LEASE); return true; }
                catch (RepositoryExecutionClaimLedger.Fenced expected) { return false; }
            }));
            start.countDown(); int winners = 0;
            for (var task : tasks) if (task.get(10, TimeUnit.SECONDS)) winners++;
            assertThat(winners).isEqualTo(1);
        }
    }

    @Test void databaseGuardRejectsIdentityChangesLiveTransferAndDeletion() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var key = key(command); var claims = new RepositoryExecutionClaimLedger(c.tx());
            claims.acquire(key, command, UUID.randomUUID(), LEASE);
            var rejected = java.util.Map.of(
                    "SET claim_epoch=claim_epoch+1,claim_token=gen_random_uuid()", "transfer requires expired predecessor",
                    "SET command_sha256=decode(repeat('00',32),'hex')", "identity is immutable",
                    "SET lease_until='infinity'", "requires a bounded live lease");
            for (var mutation : rejected.entrySet())
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    return em.createNativeQuery("UPDATE repository_execution_claims " + mutation.getKey()).executeUpdate();
                })).isInstanceOf(RuntimeException.class).hasStackTraceContaining(mutation.getValue());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                return em.createNativeQuery("DELETE FROM repository_execution_claims").executeUpdate();
            })).isInstanceOf(RuntimeException.class).hasStackTraceContaining("deletion requires a future retention protocol");
            assertThatThrownBy(() -> claims.takeOver(key, command, Long.MAX_VALUE, UUID.randomUUID(), LEASE))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void wrongScopeAndSnapshotIsolationCannotObtainAFence() {
        try (var c = context(POSTGRES)) {
            var command = command(c); var claims = new RepositoryExecutionClaimLedger(c.tx());
            var claim = claims.acquire(key(command), command, UUID.randomUUID(), LEASE);
            var wrongScope = new RepositoryExecutionClaimLedger.Claim(
                    new RepositoryOperationLedger.Key(claim.key().account(), "other-principal", command.operationId()),
                    claim.commandSha256(), claim.epoch(), claim.token(), claim.leaseUntil());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                return RepositoryExecutionClaimLedger.lockLive(em, wrongScope);
            })).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").executeUpdate();
                return RepositoryExecutionClaimLedger.lockLive(em, claim);
            })).isInstanceOf(RuntimeException.class).hasStackTraceContaining("READ COMMITTED");
        }
    }

    private static DocumentPublicationCommand command(Context c) {
        var prepared = prepare(c, 1);
        return new DocumentPublicationCommand(prepared.command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).build());
    }
    private static RepositoryOperationLedger.Key key(DocumentPublicationCommand command) {
        return new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
    }
    private static void expire(Context c) {
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
    }
}

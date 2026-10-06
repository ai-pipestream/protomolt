package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** SQL reservation only. Does not prove process death, provider quiescence or recovered publication. */
@Testcontainers
class RepositoryCoordinatorExpirationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private record Input(DocumentPublicationPreparationRecord preparation, RepositoryExecutionClaimLedger.Claim claim,
                         UUID incarnation, RepositoryOperationLedger.Owner owner) {}

    private static Input inputFor(Context c, Duration claimLease, Duration ownerLease, boolean admit) {
        var original = input(c);
        var preparation = new DocumentPublicationPreparationRecord(original.key(), original.command(), original.seeds(),
                original.placements(), claimLease, 0);
        var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
        var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                .acquireInitial(CALLER, preparation, UUID.randomUUID(), incarnation, NONE);
        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
        var owner = admit ? new RepositoryOperationLedger(c.tx()).admit(preparation.key(), preparation.command(),
                preparation.seeds().ownerNonce(), ownerLease, claim).owner().orElseThrow() : null;
        assertThat(budget.reservedBytes()).isZero();
        return new Input(preparation, claim, incarnation, owner);
    }

    private static void expire(Context c) {
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
    }

    private static void insert(EntityManager em, Input input, UUID token, UUID incarnation, String mismatch) {
        var claim = input.claim();
        em.createNativeQuery("""
                INSERT INTO repository_coordinator_expirations
                (account_id,principal,operation_id,predecessor_epoch,predecessor_token,predecessor_incarnation,
                 command_sha256,successor_epoch,successor_token,successor_incarnation,lease_millis,
                 predecessor_owner_generation,predecessor_owner_nonce)
                VALUES(:a,:p,:o,:e,:t,:i,:d,:next,:nt,:ni,300000,:g,:n)
                """).setParameter("a", claim.key().account()).setParameter("p", claim.key().principal())
                .setParameter("o", claim.key().operationId()).setParameter("e", mismatch.equals("epoch") ? 2 : claim.epoch())
                .setParameter("t", mismatch.equals("token") ? UUID.randomUUID() : claim.token())
                .setParameter("i", mismatch.equals("incarnation") ? UUID.randomUUID() : input.incarnation())
                .setParameter("d", HexFormat.of().parseHex(mismatch.equals("digest") ? "0".repeat(64) : claim.commandSha256()))
                .setParameter("next", mismatch.equals("epoch") ? 3 : claim.epoch()+1)
                .setParameter("nt", token).setParameter("ni", incarnation)
                .setParameter("g", mismatch.equals("generation") ? 2 : 1)
                .setParameter("n", mismatch.equals("nonce") ? UUID.randomUUID() : input.preparation().seeds().ownerNonce())
                .executeUpdate();
    }

    private static void unchanged(Context c, Input input) {
        var row = c.tx().readOnly(em -> (Object[]) em.createNativeQuery(
                "SELECT claim_epoch,claim_token FROM repository_execution_claims WHERE operation_id=:o")
                .setParameter("o", input.claim().key().operationId()).getSingleResult());
        assertThat(((Number) row[0]).longValue()).isEqualTo(1);
        assertThat(row[1]).isEqualTo(input.claim().token());
        for (var table : java.util.List.of("repository_coordinator_expirations", "repository_coordinator_reservations")) {
            int count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table).getSingleResult()).intValue());
            assertThat(count).isZero();
        }
    }

    @Test void shutdownRequiresCompleteHandoffChainAndHonorsPostCommitCancellation() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, Duration.ofSeconds(1), Duration.ofSeconds(1), true);
            var identity = new RepositoryCoordinatorDrain.Identity(input.claim().key(), input.claim().commandSha256(),
                    1, input.claim().token(), input.incarnation());
            expire(c);
            var first = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity, UUID.randomUUID(), UUID.randomUUID(),
                    Duration.ofSeconds(1), new RepositoryCoordinatorReservation.OwnerIdentity(1, input.owner().token()));
            RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, first, NONE);
            expire(c);
            var timeouts = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
            var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), timeouts)
                    .inspect(CALLER, identity.key(), identity.commandSha256(), NONE).unactivated().orElseThrow();
            var second = new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),
                    UUID.randomUUID(), UUID.randomUUID(), Duration.ofSeconds(1), observed.owner(),
                    observed.preparationSha256(), observed.installation());
            RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, second, NONE);
            assertThat(RepositoryShutdownClaim.inspect(c.tx().withTimeouts(timeouts), CALLER, identity, NONE))
                    .isEqualTo(RepositoryShutdownClaim.State.FENCED_REGISTERED);
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> cancelled.set(true));
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    java.util.Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var control = new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                assertThatThrownBy(() -> RepositoryShutdownClaim.inspect(new Tx(emf).withTimeouts(timeouts), CALLER, identity, control))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.CANCELLED));
            }
            assertThat(RepositoryShutdownClaim.inspect(c.tx().withTimeouts(timeouts), CALLER, identity, NONE))
                    .isEqualTo(RepositoryShutdownClaim.State.FENCED_REGISTERED);
            expire(c);
            new RepositoryExecutionClaimLedger(c.tx()).takeOver(identity.key(), input.preparation().command(), 3, UUID.randomUUID(), LEASE);
            assertThat(RepositoryShutdownClaim.inspect(c.tx().withTimeouts(timeouts), CALLER, identity, NONE))
                    .isEqualTo(RepositoryShutdownClaim.State.UNRESOLVED);
            var localDrains = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT count(*) FROM repository_coordinator_local_drains").getSingleResult());
            assertThat(localDrains).isEqualTo(0L);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void reservesWithoutInventingLocalDrainAndKeepsExecutionClosed(boolean admissionDrained) {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, Duration.ofSeconds(1), Duration.ofSeconds(1), true);
            if (admissionDrained) RepositoryCoordinatorDrain.begin(c.tx(), CALLER, input.claim(), input.incarnation(), NONE);
            expire(c);
            var token = UUID.randomUUID(); var successor = UUID.randomUUID();
            c.tx().inTransaction(em -> { insert(em, input, token, successor, ""); });
            var row = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT kind,predecessor_remote_state,predecessor_owner_generation,predecessor_owner_nonce,
                     successor_token,successor_incarnation FROM repository_coordinator_reservations
                    """).getSingleResult());
            assertThat(row[0]).isEqualTo("EXPIRED_UNQUIESCED"); assertThat(row[1]).isEqualTo("UNKNOWN");
            assertThat(((Number) row[2]).longValue()).isEqualTo(1);
            assertThat(row[3]).isEqualTo(input.owner().token());
            assertThat(row[4]).isEqualTo(token); assertThat(row[5]).isEqualTo(successor);
            int drains = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_coordinator_local_drains")
                    .getSingleResult()).intValue());
            assertThat(drains).isZero();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, input.claim()); }))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, input.claim().key(), input.claim().commandSha256(), 2, token);
                return em.createNativeQuery("SELECT require_repository_execution_claim(:a,:p,:o)")
                        .setParameter("a", input.claim().key().account()).setParameter("p", input.claim().key().principal())
                        .setParameter("o", input.claim().key().operationId()).getSingleResult();
            })).hasStackTraceContaining("requires exact activation");
            for (var sql : java.util.List.of("DELETE FROM repository_coordinator_expirations",
                    "UPDATE repository_coordinator_expirations SET lease_millis=1000")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("expiration is immutable");
            }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"epoch", "token", "incarnation", "digest", "generation", "nonce"})
    void wrongIdentityCannotTransfer(String mismatch) {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, Duration.ofSeconds(1), Duration.ofSeconds(1), true); expire(c);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em, input, UUID.randomUUID(), UUID.randomUUID(), mismatch); }))
                    .hasStackTraceContaining("requires exact");
            unchanged(c, input);
        }
    }

    @ParameterizedTest @ValueSource(strings = {"claim-live", "owner-live", "no-owner", "local-drain", "terminal"})
    void unsuitablePredecessorCannotTransfer(String state) {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, state.equals("claim-live") ? LEASE : Duration.ofSeconds(1),
                    state.equals("owner-live") ? LEASE : Duration.ofSeconds(1), !state.equals("no-owner"));
            if (state.equals("local-drain")) {
                RepositoryCoordinatorDrain.begin(c.tx(), CALLER, input.claim(), input.incarnation(), NONE);
                RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, new RepositoryCoordinatorDrain.Identity(input.claim().key(),
                        input.claim().commandSha256(), 1, input.claim().token(), input.incarnation()), NONE);
            }
            if (state.equals("terminal")) new DocumentPublicationRejections(c.tx()).cancel(CALLER, input.owner(), input.preparation().command(), NONE);
            expire(c);
            String message = switch (state) {
                case "claim-live" -> "exact expired predecessor claim";
                case "owner-live", "no-owner" -> "exact expired owner";
                case "local-drain" -> "requires graceful handoff";
                case "terminal" -> "Terminal operation cannot reserve expiration";
                default -> throw new AssertionError(state);
            };
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em, input, UUID.randomUUID(), UUID.randomUUID(), ""); }))
                    .hasStackTraceContaining(message);
            unchanged(c, input);
        }
    }

    @Test void rollbackLeavesOriginalClaimAndRetryCanReserve() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, Duration.ofSeconds(1), Duration.ofSeconds(1), true); expire(c);
            var token = UUID.randomUUID(); var incarnation = UUID.randomUUID();
            var failure = new IllegalStateException("injected rollback after reservation");
            assertThat(catchThrowable(() -> c.tx().inTransaction((java.util.function.Consumer<EntityManager>) em -> {
                insert(em, input, token, incarnation, ""); throw failure;
            }))).isSameAs(failure);
            unchanged(c, input);
            c.tx().inTransaction(em -> { insert(em, input, token, incarnation, ""); });
        }
    }

    @Test void parentPublicationFailureRollsBackExpirationAndClaim() {
        try (var c = context(POSTGRES)) {
            var input = inputFor(c, Duration.ofSeconds(1), Duration.ofSeconds(1), true); expire(c);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION fail_expired_parent() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'injected expired parent failure'; END; $$
                        """).executeUpdate();
                em.createNativeQuery("""
                        CREATE TRIGGER fail_expired_parent AFTER INSERT ON repository_coordinator_reservations
                        FOR EACH ROW EXECUTE FUNCTION fail_expired_parent()
                        """).executeUpdate();
            });
            var token = UUID.randomUUID(); var incarnation = UUID.randomUUID();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em, input, token, incarnation, ""); }))
                    .hasStackTraceContaining("injected expired parent failure");
            unchanged(c, input);
            c.tx().inTransaction(em -> { em.createNativeQuery(
                    "DROP TRIGGER fail_expired_parent ON repository_coordinator_reservations").executeUpdate(); });
            c.tx().inTransaction(em -> { insert(em, input, token, incarnation, ""); });
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void concurrentReservationsHaveOneImmutableWinner(boolean identical) throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var input = inputFor(c, Duration.ofSeconds(1), Duration.ofSeconds(1), true); expire(c);
            var firstToken = UUID.randomUUID(); var firstIncarnation = UUID.randomUUID();
            var secondToken = identical ? firstToken : UUID.randomUUID();
            var secondIncarnation = identical ? firstIncarnation : UUID.randomUUID();
            var start = new java.util.concurrent.CountDownLatch(1);
            var first = workers.submit(() -> { start.await(); return catchThrowable(() -> c.tx().inTransaction(em -> {
                insert(em, input, firstToken, firstIncarnation, "");
            })); });
            var second = workers.submit(() -> { start.await(); return catchThrowable(() -> c.tx().inTransaction(em -> {
                insert(em, input, secondToken, secondIncarnation, "");
            })); });
            start.countDown();
            var a = first.get(10, java.util.concurrent.TimeUnit.SECONDS);
            var b = second.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat((a == null) != (b == null)).isTrue();
            assertThat(a == null ? b : a).hasStackTraceContaining("exact expired predecessor claim");
            var winningToken = a == null ? firstToken : secondToken;
            var winningIncarnation = a == null ? firstIncarnation : secondIncarnation;
            var row = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT c.claim_epoch,c.claim_token,r.successor_token,r.successor_incarnation,c.lease_until
                    FROM repository_execution_claims c JOIN repository_coordinator_reservations r
                    USING(account_id,principal,operation_id)
                    """).getSingleResult());
            assertThat(((Number) row[0]).longValue()).isEqualTo(2);
            assertThat(row[1]).isEqualTo(winningToken); assertThat(row[2]).isEqualTo(winningToken);
            assertThat(row[3]).isEqualTo(winningIncarnation);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em, input, winningToken, winningIncarnation, ""); }))
                    .hasStackTraceContaining("exact expired predecessor claim");
            var lease = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims").getSingleResult());
            assertThat(lease).isEqualTo(row[4]);
        }
    }
}

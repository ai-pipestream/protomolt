package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL fencing. No provider work or session eviction is exercised here. */
@Testcontainers
class DocumentPublicationAbandonmentIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void upgradesExistingRegistrationsWithoutChangingTheirIdentity(boolean admitted) {
        try (var c = context(POSTGRES, "84")) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var claim = journal.acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            if (admitted) new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim);
            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("85").load().migrate();
            try (var loaded = journal.load(CALLER, claim, 0, NONE).orElseThrow()) {
                assertThat(DocumentPublicationPreparationCodec.encode(loaded.record()))
                        .isEqualTo(DocumentPublicationPreparationCodec.encode(value));
            }
            if (admitted) assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, NONE))
                    .hasStackTraceContaining("cannot be abandoned");
            else DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, NONE);
            assertThat(count(c, "repository_operation_owners", value)).isEqualTo(admitted ? 1 : 0);
            assertThat(count(c, "repository_publication_abandonments", value)).isEqualTo(admitted ? 0 : 1);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationBeforeCommitRollsBackAndCommittedMarkerCannotBeDeleted() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            var calls = new AtomicInteger();
            RepositoryReadControl control = new RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (calls.incrementAndGet() == 4) throw new IllegalStateException("before commit"); }
            };
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, control))
                    .hasMessage("before commit");
            assertThat(count(c, "repository_publication_abandonments", value)).isZero();
            DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM repository_publication_abandonments").executeUpdate(); return null;
            }))
                    .hasStackTraceContaining("immutable");
            assertThat(count(c, "repository_publication_abandonments", value)).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void commandWithoutOwnerAlsoPreventsAbandonment() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            // The existing deferred constraint forbids committing a command without
            // its owner. Exercise the transient state inside that same transaction.
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                em.createNativeQuery("""
                        INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,command_version,command,command_sha256)
                        VALUES(:a,:p,:o,:codec,:version,:bytes,:digest)
                        """).setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                        .setParameter("o", value.key().operationId()).setParameter("codec", DocumentPublicationCommand.CODEC)
                        .setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                        .setParameter("bytes", value.command().canonical().toByteArray())
                        .setParameter("digest", java.util.HexFormat.of().parseHex(value.command().sha256())).executeUpdate();
                em.createNativeQuery("""
                        INSERT INTO repository_publication_abandonments(account_id,principal,operation_id,
                          predecessor_generation,owner_nonce,preparation_sha256,claim_epoch,claim_token)
                        SELECT account_id,principal,operation_id,0,owner_nonce,preparation_sha256,1,:token
                        FROM repository_publication_preparations WHERE operation_id=:o AND predecessor_generation=0
                        """).setParameter("token", claim.token()).setParameter("o", value.key().operationId()).executeUpdate();
                return null;
            })).hasStackTraceContaining("cannot be abandoned");
            assertThat(count(c, "repository_operation_owners", value)).isZero();
            assertThat(count(c, "repository_operations", value)).isZero();
            assertThat(count(c, "repository_publication_abandonments", value)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void exactRetryAfterAbandonmentRefusesAllRegistrationAdvancement() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var claim = journal.acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            var modes = new DocumentPublicationModesJournal(c.tx(), budget);
            modes.bind(CALLER, claim, 0, MODES, NONE);
            DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, NONE);
            DocumentPublicationAbandonment.abandon(new Tx(c.emf()), budget, CALLER, claim, value, NONE);
            assertThat(inspect(c, budget, claim)).isEqualTo(DocumentPublicationRegistrationInspection.Phase.ABANDONED);
            assertThatThrownBy(() -> journal.save(CALLER, claim, value, NONE)).hasStackTraceContaining("was abandoned");
            assertThatThrownBy(() -> modes.bind(CALLER, claim, 0, MODES, NONE)).hasStackTraceContaining("was abandoned");
            assertThatThrownBy(() -> new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(),
                    value.seeds().ownerNonce(), LEASE, claim)).hasStackTraceContaining("was abandoned");
            assertThat(count(c, "repository_operations", value)).isZero();
            assertThat(count(c, "repository_publication_abandonments", value)).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void admittedOwnerAndWrongIdentityCannotBeAbandoned() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            var wrong = new RepositoryExecutionClaimLedger.Claim(claim.key(), claim.commandSha256(), 1, UUID.randomUUID(), claim.leaseUntil());
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, wrong, value, NONE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var changed = new DocumentPublicationPreparationRecord(value.key(), value.command(),
                    DocumentPublicationSeeds.mint(value.key(), value.command()), value.placements(), value.lease(), 0);
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, changed, NONE))
                    .hasStackTraceContaining("differs from initial preparation");
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
            new RepositoryOperationLedger(c.tx()).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim);
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, NONE))
                    .hasStackTraceContaining("cannot be abandoned");
            assertThat(count(c, "repository_publication_abandonments", value)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationAfterCommitIsReconciledByExactRetry() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var budget = new PayloadBudget(64_000_000);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            var calls = new AtomicInteger();
            RepositoryReadControl control = new RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (calls.incrementAndGet() == 5) throw new IllegalStateException("lost completion"); }
            };
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, control))
                    .hasMessage("lost completion");
            assertThat(count(c, "repository_publication_abandonments", value)).isEqualTo(1);
            DocumentPublicationAbandonment.abandon(new Tx(c.emf()), budget, CALLER, claim, value, NONE);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void privateAuthorityAndOriginalLiveClaimAreRequired() {
        try (var c = context(POSTGRES)) {
            var original = input(c); var budget = new PayloadBudget(64_000_000);
            var value = new DocumentPublicationPreparationRecord(original.key(), original.command(), original.seeds(),
                    original.placements(), java.time.Duration.ofSeconds(1), 0);
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
            var scoped = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, scoped, claim, value, NONE))
                    .hasMessageContaining("private process authority");
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, claim, value, NONE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var next = new RepositoryExecutionClaimLedger(c.tx()).takeOver(value.key(), value.command(), 1, UUID.randomUUID(), LEASE);
            assertThatThrownBy(() -> DocumentPublicationAbandonment.abandon(c.tx(), budget, CALLER, next, value, NONE))
                    .hasMessageContaining("original claim");
            assertThat(count(c, "repository_publication_abandonments", value)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void racingAdmissionAndAbandonmentHaveExactlyOneWinner() throws Exception {
        try (var c = context(POSTGRES); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var original = input(c);
            for (int i = 0; i < 8; i++) {
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).build());
                var key = new RepositoryOperationLedger.Key(original.key().account(), original.key().principal(), command.operationId());
                var value = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        original.placements(), LEASE, 0);
                var budget = new PayloadBudget(64_000_000);
                var claim = new DocumentPublicationPreparationJournal(c.tx(), budget).acquireInitial(CALLER, value, UUID.randomUUID(), NONE);
                new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
                var start = new CountDownLatch(1);
                var mark = executor.submit(() -> { start.await(); try {
                    DocumentPublicationAbandonment.abandon(new Tx(c.emf()), budget, CALLER, claim, value, NONE); return true;
                } catch (RuntimeException e) { assertThat(e).hasStackTraceContaining("cannot be abandoned"); return false; } });
                var admit = executor.submit(() -> { start.await(); try {
                    new RepositoryOperationLedger(new Tx(c.emf())).admit(value.key(), value.command(), value.seeds().ownerNonce(), LEASE, claim); return true;
                } catch (RuntimeException e) { assertThat(e).hasStackTraceContaining("was abandoned"); return false; } });
                start.countDown();
                assertThat(mark.get(15, TimeUnit.SECONDS)).isNotEqualTo(admit.get(15, TimeUnit.SECONDS));
                assertThat(count(c, "repository_operation_owners", value) + count(c, "repository_publication_abandonments", value)).isEqualTo(1);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    private static long count(Context c, String table, DocumentPublicationPreparationRecord value) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                .setParameter("o", value.key().operationId()).getSingleResult()).longValue());
    }
}

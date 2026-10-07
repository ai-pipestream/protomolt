package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual pre-owner preparation, capture and abandonment transactions; source provider observations are synthetic. */
@Testcontainers
class DocumentCaptureAdmissionClosureIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void abandonedPreparationCannotAddAnotherCaptureButOpenPreparationCan(boolean migrateExisting) throws Exception {
        try (var c = migrateExisting ? context(POSTGRES, "107") : context(POSTGRES); var rig = prepare(c)) {
            // A positive control: an open preparation can add a fresh capture.
            append(c, rig);
            assertThat(batches(c, rig)).isEqualTo(2);
            DocumentPublicationAbandonment.abandon(c.tx(), rig.budget(), CALLER, rig.claim(), rig.record(), NONE);
            if (migrateExisting) org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").target("108").load().migrate();
            assertThatThrownBy(() -> append(c, rig)).hasStackTraceContaining("Publication capture admission is closed");
            assertThat(batches(c, rig)).isEqualTo(2);
            var existing = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPreparationSourcePins.insert(em, rig.record(), existing, rig.claim(), rig.coordinator(), () -> {});
                int inserted = em.createNativeQuery("""
                        INSERT INTO repository_preparation_pin_batches(account_id,principal,operation_id,predecessor_generation,
                         pins_sha256,expected_count,initial_capture) VALUES('account','principal',:o,0,:d,:count,false)
                        ON CONFLICT DO NOTHING
                        """).setParameter("o", rig.record().key().operationId()).setParameter("d", existing.digest())
                        .setParameter("count", existing.pins().size()).executeUpdate();
                assertThat(inserted).isZero();
            });
            assertThat(batches(c, rig)).isEqualTo(2);
        }
    }

    @Test void ownerInsertionRechecksAbandonmentEvenWhenBatchBeganOpen() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c)) {
            // Insert the actual V85 marker between batch insertion and owner insertion.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION test_abandon_before_capture_owner() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                         INSERT INTO repository_publication_abandonments(account_id,principal,operation_id,predecessor_generation,
                          owner_nonce,preparation_sha256,claim_epoch,claim_token)
                         SELECT p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.owner_nonce,p.preparation_sha256,
                          NEW.claim_epoch,NEW.claim_token FROM repository_publication_preparations p
                          WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id;
                         RETURN NEW;
                        END; $$
                        """).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER a_test_abandon BEFORE INSERT ON repository_preparation_pin_owners "
                        + "FOR EACH ROW EXECUTE FUNCTION test_abandon_before_capture_owner()").executeUpdate();
            });
            assertThatThrownBy(() -> append(c, rig)).hasStackTraceContaining("Publication capture admission is closed");
            assertThat(batches(c, rig)).isEqualTo(1);
            long abandonments = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_publication_abandonments WHERE operation_id=:o")
                    .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
            assertThat(abandonments).isZero(); // The failed capture transaction rolls back the interposed marker too.
        }
    }

    @Test void captureWaitingOnAbandonmentClaimLockSeesCommittedClosure() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c)) {
            var entered = new java.util.concurrent.CountDownLatch(1);
            var finish = new java.util.concurrent.CountDownLatch(1);
            var captureEntered = new java.util.concurrent.CountDownLatch(1);
            var blocker = new java.util.concurrent.atomic.AtomicInteger();
            var waiter = new java.util.concurrent.atomic.AtomicInteger();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                    rows.next(); blocker.set(rows.getInt(1));
                }
                entered.countDown();
                try {
                    if (!finish.await(15, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("Abandonment gate timeout");
                } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(failure); }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var abandoning = executor.submit(() -> DocumentPublicationAbandonment.abandon(new Tx(emf), rig.budget(), CALLER,
                        rig.claim(), rig.record(), NONE));
                try {
                    assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    var capturing = executor.submit(() -> {
                        append(c, rig, em -> {
                            waiter.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                            captureEntered.countDown();
                        });
                        return true;
                    });
                    assertThat(captureEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    boolean blocked = false;
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery(
                                "SELECT :blocker=ANY(pg_blocking_pids(:waiter))")
                                .setParameter("blocker", blocker.get()).setParameter("waiter", waiter.get()).getSingleResult());
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).as("capture waits for exact abandonment transaction").isTrue();
                    finish.countDown();
                    abandoning.get(10, java.util.concurrent.TimeUnit.SECONDS);
                    assertThatThrownBy(() -> capturing.get(10, java.util.concurrent.TimeUnit.SECONDS))
                            .hasStackTraceContaining("Publication capture admission is closed");
                    assertThat(batches(c, rig)).isEqualTo(1);
                } finally { finish.countDown(); }
            }
        }
    }

    private record Rig(Fixture fixture, DocumentReadLedger reads, DocumentReadLedger.PinnedHistory history,
            DocumentHistoricalAssessmentSources sources, DocumentPublicationPreparationRecord record,
            RepositoryExecutionClaimLedger.Claim claim, UUID coordinator, PayloadBudget budget) implements AutoCloseable {
        @Override public void close() throws Exception {
            sources.close(); release(reads, history); assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static Rig prepare(Context c) throws Exception {
        var original = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
        var fixture = new Fixture(original, DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {})));
        var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
        var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
        var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var placement = original.prepared().members().getFirst().placement();
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), LEASE, 0);
        var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), NONE);
        var coordinator = UUID.randomUUID();
        var budget = new PayloadBudget(64L * 1024 * 1024);
        boolean delivered = false;
        try {
            var pins = DocumentPreparationSourcePins.prepare(command, sources.references(command, () -> {}), () -> {});
            var claim = c.tx().inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(em, key, command, UUID.randomUUID(), LEASE, sources);
                RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
                var bytes = DocumentPublicationPreparationCodec.encode(record);
                DocumentPublicationPreparationJournal.insert(em, acquired.claim(), record, bytes,
                        DocumentPublicationPreparationJournal.digest(bytes), sources.references(command, () -> {}));
                lockSources(em, fixture, pins);
                DocumentPreparationSourcePins.insert(em, record, pins, acquired.claim(), coordinator, () -> {});
                return acquired.claim();
            });
            delivered = true; return new Rig(fixture, reads, history, sources, record, claim, coordinator, budget);
        } finally { if (!delivered) { sources.close(); release(reads, history); } }
    }

    private static void append(Context c, Rig rig) throws Exception {
        append(c, rig, em -> {});
    }

    private static void append(Context c, Rig rig, java.util.function.Consumer<jakarta.persistence.EntityManager> beforeLock) throws Exception {
        var history = rig.reads().captureHistorical(CALLER, rig.fixture().address(), rig.fixture().revision());
        try (var sources = DocumentHistoricalAssessmentSources.open(rig.record().command(), CALLER, List.of(history), NONE)) {
            var pins = DocumentPreparationSourcePins.prepare(rig.record().command(), sources.references(rig.record().command(), () -> {}), () -> {});
            c.tx().inTransaction(em -> {
                beforeLock.accept(em);
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                lockSources(em, rig.fixture(), pins);
                DocumentPreparationSourcePins.insert(em, rig.record(), pins, rig.claim(), rig.coordinator(), () -> {});
            });
        } finally { history.close(); history.release(); }
    }

    private static void lockSources(jakarta.persistence.EntityManager em, Fixture fixture, DocumentPreparationSourcePins.Prepared pins) {
        var objects = pins.pins().stream().map(DocumentHistoricalSourcePin::object).collect(java.util.stream.Collectors.toSet());
        var origins = DocumentPublicationLocks.lockIndependentOrigins(em, Set.of(DocumentIds.nodeId(fixture.address())), objects, Set.of());
        DocumentPublicationLocks.lockIndependentRetention(em, origins);
    }

    private static long batches(Context c, Rig rig) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_preparation_pin_batches WHERE operation_id=:o")
                .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
    }
}

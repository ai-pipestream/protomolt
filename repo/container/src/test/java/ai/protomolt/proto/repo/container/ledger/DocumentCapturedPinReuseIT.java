package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual PostgreSQL pin release/reinsertion; publication provider observations are fixture supplied. */
@Testcontainers
class DocumentCapturedPinReuseIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void capturedPinCannotBeReinsertedWhileFreshCapturesAndUnrelatedHandlesStillWork(boolean migrateExisting) throws Exception {
        try (var c = migrateExisting ? context(POSTGRES, "105") : context(POSTGRES)) {
            var fixture = retained(c);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
            var unrelated = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
            try {
                var pin = register(c, fixture, history);
                if (migrateExisting) {
                    org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                            .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo")
                            .target("106").load().migrate();
                    assertThat(rows(c, "document_read_pins", pin.pin())).isEqualTo(1);
                }
                history.close(); history.release();
                try (var connection = c.pool().getConnection()) {
                    assertThatThrownBy(() -> insert(connection, pin)).hasMessageContaining("Captured document pin identity cannot be reused");
                }
                // Normal cleanup does not fence the reader incarnation or its unrelated handles.
                try (var use = unrelated.use()) { assertThat(use.plan().revision()).isEqualTo(fixture.revision()); }
                var fresh = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
                try {
                    var next = register(c, fixture, fresh);
                    assertThat(next.pin()).isNotEqualTo(pin.pin());
                    assertThat(next.reader()).isEqualTo(pin.reader());
                } finally { fresh.close(); fresh.release(); }
                assertThat(rows(c, "document_read_pins", pin.pin())).isZero();
                long retainedPins = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_preparation_source_pins WHERE pin_id=:pin")
                        .setParameter("pin", pin.pin()).getSingleResult()).longValue());
                assertThat(retainedPins).isEqualTo(1);
            } finally {
                history.close(); history.release(); unrelated.close(); unrelated.release();
                reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }

    @Test void insertBlockedOnUncommittedDeletionCannotResurrectCapturedPinAfterReleaseCommits() throws Exception {
        try (var c = context(POSTGRES)) {
            var fixture = retained(c);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
            try {
                var pin = register(c, fixture, history);
                history.close();
                assertThat(history.isDrained()).isTrue();
                try (var deleting = c.pool().getConnection(); var inserting = c.pool().getConnection();
                     var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    deleting.setAutoCommit(false);
                    try (var statement = deleting.prepareStatement("SELECT release_document_read_pins(?,CAST(? AS jsonb))")) {
                        statement.setObject(1, pin.reader());
                        statement.setString(2, "[{\"pin\":\"" + pin.pin() + "\",\"object\":\"" + pin.object() + "\"}]");
                        statement.execute();
                    }
                    int pid = backend(inserting), blocker = backend(deleting);
                    // Bound the database wait too, so a failed assertion cannot strand executor shutdown.
                    try (var statement = inserting.createStatement()) { statement.execute("SET statement_timeout='15s'"); }
                    var future = executor.submit(() -> { insert(inserting, pin); return true; });
                    try {
                        assertBlocked(c, pid, blocker);
                        assertThat(future.isDone()).isFalse();
                        deleting.commit();
                        assertThatThrownBy(() -> future.get(15, TimeUnit.SECONDS))
                                .hasStackTraceContaining("Captured document pin identity cannot be reused");
                    } finally { deleting.rollback(); }
                }
                assertThat(rows(c, "document_read_pins", pin.pin())).isZero();
            } finally {
                history.close(); history.release(); reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }

    private static int backend(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
            result.next(); return result.getInt(1);
        }
    }

    private static void assertBlocked(Context c, int pid, int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            boolean blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid AND wait_event_type='Lock' AND :blocker=ANY(pg_blocking_pids(pid)))")
                    .setParameter("pid", pid).setParameter("blocker", blocker).getSingleResult());
            if (blocked) return;
            Thread.sleep(10);
        }
        throw new AssertionError("Insert never reached a verified PostgreSQL lock wait");
    }

    private static void insert(Connection connection, DocumentHistoricalSourcePin pin) throws Exception {
        try (var statement = connection.prepareStatement("""
                INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision,read_scope)
                VALUES(?,?,?,?,?,?,'HISTORICAL')
                """)) {
            statement.setObject(1, pin.pin()); statement.setObject(2, pin.reader()); statement.setObject(3, pin.object());
            statement.setObject(4, pin.node()); statement.setObject(5, pin.revision()); statement.setLong(6, pin.publicationRevision());
            statement.executeUpdate();
        }
    }

    private static DocumentHistoricalSourcePin register(Context c, Fixture fixture, DocumentReadLedger.PinnedHistory history) {
        var command = new DocumentPublicationCommand(fixture.original().command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var placement = fixture.original().prepared().members().getFirst().placement();
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
        var budget = new PayloadBudget(64L * 1024 * 1024);
        try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), RepositoryReadControl.NONE)) {
            var pin = sources.references(command, () -> {}).getFirst().pins().getFirst();
            var registration = DocumentPublicationRegistration.historical(c.tx(), budget, record, sources,
                    UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), RepositoryReadControl.NONE);
            assertThat(registration.admitInitial(CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED),
                    RepositoryReadControl.NONE)).isPresent();
            return pin;
        } finally { assertThat(budget.reservedBytes()).isZero(); }
    }

    private static Fixture retained(Context c) throws Exception {
        var original = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
        return new Fixture(original, DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {})));
    }

    private static long rows(Context c, String table, UUID pin) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE pin_id=:pin")
                .setParameter("pin", pin).getSingleResult()).longValue());
    }
}

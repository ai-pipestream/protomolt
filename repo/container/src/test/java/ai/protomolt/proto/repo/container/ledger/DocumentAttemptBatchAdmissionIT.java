package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.DocumentPart;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real SQL admission tests; declared sizes/digests are synthetic, unverified claims. */
@Testcontainers
class DocumentAttemptBatchAdmissionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static final Duration LEASE = Duration.ofMinutes(5);

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @ParameterizedTest @CsvSource({"1,0", "256,256", "513,513", "10000,10000"})
    void admissionUsesBoundedStatementsAndPreservesExactPlan(int objectCount, int sourceCount) {
        var plan = plan(objectCount, sourceCount);
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();
        long started = System.nanoTime();
        try {
            var admitted = new DocumentPartAttemptLedger(tx).begin(plan, LEASE);
            assertThat(admitted.state()).isEqualTo("STAGING");
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
            // Counts client statements, not trigger-internal SQL or provider latency.
            int budget = 5 + (objectCount + 255) / 256 + (sourceCount + 255) / 256;
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(budget);
            System.out.printf("admission objects=%d sources=%d client_statements=%d transactions=%d elapsed_ms=%.3f%n",
                    objectCount, sourceCount, statistics.getPrepareStatementCount(), statistics.getTransactionCount(),
                    (System.nanoTime() - started) / 1_000_000.0);
        } finally { statistics.setStatisticsEnabled(false); }
        var storedSources = tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery(
                "SELECT source_node_id,revision FROM document_part_attempt_sources WHERE attempt_id=:id", Object[].class)
                .setParameter("id", plan.attemptId()).getResultList());
        var actualSources = new HashMap<UUID, Long>();
        for (var values : storedSources) actualSources.put((UUID) values[0], ((Number) values[1]).longValue());
        assertThat(actualSources).isEqualTo(plan.sources());
        var storedObjects = tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT ordinal,part,sub_key,object_key,expected_size,expected_sha256,content_type,verified,physical_object_id
                FROM document_part_attempt_objects WHERE attempt_id=:id ORDER BY ordinal
                """, Object[].class).setParameter("id", plan.attemptId()).getResultList());
        assertThat(storedObjects).hasSize(plan.objects().size());
        for (int i = 0; i < storedObjects.size(); i++) {
            var row = storedObjects.get(i); var expected = plan.objects().get(i);
            assertThat(((Number) row[0]).intValue()).isEqualTo(i);
            assertThat(((Number) row[1]).intValue()).isEqualTo(expected.part().getNumber());
            assertThat(row[2]).isEqualTo(expected.subKey());
            assertThat(row[3]).isEqualTo(expected.objectKey());
            assertThat(((Number) row[4]).longValue()).isEqualTo(expected.size());
            assertThat(row[5]).isEqualTo(expected.sha256());
            assertThat(row[6]).isEqualTo(expected.contentType());
            assertThat(row[7]).isEqualTo(false);
            assertThat(row[8]).isInstanceOf(UUID.class);
        }
        assertThat(count("repository_physical_locations", "source_id", plan.attemptId())).isEqualTo(objectCount);
        assertThat(count("document_part_key_reservations", "attempt_id", plan.attemptId())).isEqualTo(objectCount);
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void failureAfterEarlierBatchesRollsBackAllAdmissionState(boolean sourceFailure) {
        var plan = plan(513, 513);
        String table = sourceFailure ? "document_part_attempt_sources" : "document_part_attempt_objects";
        String condition = sourceFailure
                ? "(SELECT count(*) FROM document_part_attempt_sources WHERE attempt_id=NEW.attempt_id) > 256"
                : "NEW.ordinal = 256";
        // Inject failure through the real database after a complete earlier batch.
        // Names are fixed test-only identifiers; plan UUID is generated locally.
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION fail_test_admission_batch() RETURNS trigger LANGUAGE plpgsql AS $body$ BEGIN "
                    + "IF NEW.attempt_id = '" + plan.attemptId() + "' AND " + condition
                    + " THEN RAISE EXCEPTION 'injected later admission batch failure'; END IF; RETURN NEW; END; $body$")
                    .executeUpdate();
            em.createNativeQuery("CREATE TRIGGER zz_test_admission_batch AFTER INSERT ON " + table
                    + " FOR EACH ROW EXECUTE FUNCTION fail_test_admission_batch()").executeUpdate();
        });
        try {
            assertThatThrownBy(() -> new DocumentPartAttemptLedger(tx).begin(plan, LEASE))
                    .hasStackTraceContaining("injected later admission batch failure");
            assertThat(new DocumentPartAttemptLedger(tx).find(plan.attemptId())).isEmpty();
            for (String child : new String[]{"document_part_attempt_objects", "document_part_attempt_sources", "document_part_key_reservations"})
                assertThat(count(child, "attempt_id", plan.attemptId())).isZero();
            assertThat(count("repository_physical_locations", "source_id", plan.attemptId())).isZero();
        } finally {
            tx.inTransaction(em -> { em.createNativeQuery("DROP FUNCTION fail_test_admission_batch() CASCADE").executeUpdate(); });
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void twoAdmissionsShareOuterCommitOrRollback(boolean rollback) {
        var first = plan(2, 1);
        var second = plan(3, 1);
        var preparedFirst = DocumentPartAttemptLedger.prepareAdmission(first, LEASE);
        var preparedSecond = DocumentPartAttemptLedger.prepareAdmission(second, LEASE);
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();
        try {
            Runnable work = () -> tx.inTransaction(em -> {
                DocumentPartAttemptLedger.beginInTransaction(em, preparedFirst);
                DocumentPartAttemptLedger.beginInTransaction(em, preparedSecond);
                if (rollback) throw new IllegalStateException("cancel before admission commit");
            });
            if (rollback) assertThatThrownBy(work::run).hasMessage("cancel before admission commit");
            else work.run();
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
        } finally { statistics.setStatisticsEnabled(false); }
        for (var plan : java.util.List.of(first, second)) {
            assertThat(new DocumentPartAttemptLedger(tx).find(plan.attemptId()).isPresent()).isEqualTo(!rollback);
            assertThat(count("document_part_attempt_objects", "attempt_id", plan.attemptId()))
                    .isEqualTo(rollback ? 0 : plan.objects().size());
            assertThat(count("document_part_attempt_sources", "attempt_id", plan.attemptId()))
                    .isEqualTo(rollback ? 0 : plan.sources().size());
            assertThat(count("document_part_key_reservations", "attempt_id", plan.attemptId()))
                    .isEqualTo(rollback ? 0 : plan.objects().size());
            assertThat(count("repository_physical_locations", "source_id", plan.attemptId()))
                    .isEqualTo(rollback ? 0 : plan.objects().size());
        }
    }

    @Test void caughtAdmissionFailureCannotCommitEarlierMember() {
        var first = plan(2, 1);
        var original = plan(1, 0);
        var missing = new DocumentPartAttemptLedger.Plan(original.attemptId(),
                new DocumentPartAttemptLedger.Location(original.location().nodeId(), "account", "missing-profile", "container"),
                original.sampledRevision(), original.sources(), original.objects());
        var preparedFirst = DocumentPartAttemptLedger.prepareAdmission(first, LEASE);
        var preparedMissing = DocumentPartAttemptLedger.prepareAdmission(missing, LEASE);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            DocumentPartAttemptLedger.beginInTransaction(em, preparedFirst);
            assertThatThrownBy(() -> DocumentPartAttemptLedger.beginInTransaction(em, preparedMissing))
                    .hasMessage("Original backend generation is not registered");
            return "must not report committed";
        })).isInstanceOf(jakarta.persistence.RollbackException.class);
        assertThat(new DocumentPartAttemptLedger(tx).find(first.attemptId())).isEmpty();
        assertThat(count("repository_physical_locations", "source_id", first.attemptId())).isZero();
        assertThat(count("document_part_key_reservations", "attempt_id", first.attemptId())).isZero();
    }

    @Test void participantRequiresAnActiveTransaction() {
        var prepared = DocumentPartAttemptLedger.prepareAdmission(plan(1, 0), LEASE);
        assertThatThrownBy(() -> tx.readOnly(em -> DocumentPartAttemptLedger.beginInTransaction(em, prepared)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("active writable transaction");
    }

    private static long count(String table, String column, UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE " + column + "=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }

    private static DocumentPartAttemptLedger.Plan plan(int objectCount, int sourceCount) {
        String generation = "batch-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://storage.example")), generation));
        UUID node = UUID.randomUUID(); UUID attempt = UUID.randomUUID();
        var location = new DocumentPartAttemptLedger.Location(node, "account", generation, "original-container");
        String prefix = "documents/account/" + node + "/attempts/" + attempt + "/";
        var objects = new ArrayList<DocumentPartAttemptLedger.PlannedObject>();
        for (int i = 0; i < objectCount; i++) {
            String suffix = i == 0 ? "core" : "part-\"\\\t😀-" + i;
            objects.add(new DocumentPartAttemptLedger.PlannedObject(i == 0 ? DocumentPart.DOCUMENT_PART_CORE : DocumentPart.DOCUMENT_PART_CHUNKS,
                    i == 0 ? "" : suffix, prefix + suffix, i == 0 ? Long.MAX_VALUE : 9007199254740993L + i,
                    "ab".repeat(32), "application/protobuf"));
        }
        var sources = new HashMap<UUID, Long>();
        for (int i = 0; i < sourceCount; i++) sources.put(UUID.randomUUID(), i == 0 ? Long.MAX_VALUE : 9007199254740993L + i);
        return new DocumentPartAttemptLedger.Plan(attempt, location, 7, sources, objects);
    }
}

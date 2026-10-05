package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@Timeout(60)
class ArchiveStatisticsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void concurrentFirstAndExistingUpdatesKeepExactTotalsIncludingDecrements() throws Exception {
        try (var database = database(); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            String account = UUID.randomUUID().toString();
            var increment = new ArchiveLedger.StatsDelta(1, 2, 30, 20,
                    Map.of("original", 1L, "text", 1L), Map.of("original", 20L, "text", 10L));
            for (int round = 0; round < 2; round++) {
                var start = new CyclicBarrier(8);
                var futures = new ArrayList<Future<?>>();
                for (int worker = 0; worker < 8; worker++) futures.add(workers.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < 8; i++) apply(tx, account, increment);
                    return null;
                }));
                for (var future : futures) future.get(20, TimeUnit.SECONDS);
            }
            apply(tx, account, new ArchiveLedger.StatsDelta(-1, -2, -30, -20,
                    Map.of("original", -1L, "text", -1L), Map.of("original", -20L, "text", -10L)));
            var ledger = new ArchiveLedger(tx);
            var actual = ledger.findStats(account, "records").orElseThrow();
            assertThat(actual.entries).isEqualTo(127);
            assertThat(actual.versions).isEqualTo(254);
            assertThat(actual.retainedBytes).isEqualTo(3810);
            assertThat(actual.currentBytes).isEqualTo(2540);
            var renditions = ledger.findRenditionStats(account, "records");
            assertThat(renditions).extracting(r -> r.renditionName).containsExactly("original", "text");
            assertThat(renditions).extracting(r -> r.objectCount).containsExactly(127L, 127L);
            assertThat(renditions).extracting(r -> r.totalBytes).containsExactly(2540L, 1270L);
        }
    }

    @Test void laterFailureRollsBackArchiveAndEveryRendition() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            String account = UUID.randomUUID().toString();
            var delta = new ArchiveLedger.StatsDelta(1, 1, 7, 7, Map.of("original", 1L), Map.of("original", 7L));
            apply(tx, account, delta);
            var failure = new IllegalStateException("failure after real statistics writes");
            assertThatThrownBy(() -> tx.inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
                ArchiveLedger.applyDelta(em, account, "records", delta);
                ArchiveLedger.applyDelta(em, account, "records", new ArchiveLedger.StatsDelta(0, 0, 2, 0,
                        Map.of("new", 1L), Map.of("new", 2L)));
                em.flush();
                throw failure;
            })).isSameAs(failure);
            var ledger = new ArchiveLedger(tx);
            assertThat(ledger.findStats(account, "records").orElseThrow().retainedBytes).isEqualTo(7);
            assertThat(ledger.findRenditionStats(account, "records")).singleElement()
                    .satisfies(row -> { assertThat(row.objectCount).isEqualTo(1); assertThat(row.totalBytes).isEqualTo(7); });
        }
    }

    @Test void byteOnlyRenditionAdjustmentIsNotDropped() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            String account = UUID.randomUUID().toString();
            apply(tx, account, new ArchiveLedger.StatsDelta(1, 1, 7, 7, Map.of("original", 1L), Map.of("original", 7L)));
            apply(tx, account, new ArchiveLedger.StatsDelta(0, 0, 5, 5, Map.of(), Map.of("original", 5L)));
            assertThat(new ArchiveLedger(tx).findRenditionStats(account, "records")).singleElement()
                    .satisfies(row -> { assertThat(row.objectCount).isEqualTo(1); assertThat(row.totalBytes).isEqualTo(12); });
            String byteOnlyAccount = UUID.randomUUID().toString();
            apply(tx, byteOnlyAccount, new ArchiveLedger.StatsDelta(0, 0, 0, 0, Map.of(), Map.of("original", 5L)));
            apply(tx, byteOnlyAccount, new ArchiveLedger.StatsDelta(0, 0, 0, 0, Map.of(), Map.of("original", 3L)));
            assertThat(new ArchiveLedger(tx).findRenditionStats(byteOnlyAccount, "records")).singleElement()
                    .satisfies(row -> { assertThat(row.objectCount).isZero(); assertThat(row.totalBytes).isEqualTo(8); });
        }
    }

    @Test void renditionOverflowRollsBackTheEarlierArchiveUpdate() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            String account = UUID.randomUUID().toString();
            apply(tx, account, new ArchiveLedger.StatsDelta(1, 1, Long.MAX_VALUE, 0,
                    Map.of("original", 1L), Map.of("original", Long.MAX_VALUE)));
            assertThatThrownBy(() -> apply(tx, account, new ArchiveLedger.StatsDelta(1, 1, 0, 0,
                    Map.of("original", 1L), Map.of("original", 1L)))).rootCause().isInstanceOfSatisfying(java.sql.SQLException.class,
                            failure -> assertThat(failure.getSQLState()).isEqualTo("22003"));
            var ledger = new ArchiveLedger(tx);
            assertThat(ledger.findStats(account, "records").orElseThrow().entries).isEqualTo(1);
            assertThat(ledger.findRenditionStats(account, "records")).singleElement().satisfies(row -> {
                assertThat(row.objectCount).isEqualTo(1);
                assertThat(row.totalBytes).isEqualTo(Long.MAX_VALUE);
            });
        }
    }

    @Test void overflowingCountersFailWithoutWrappingOrCommittingOtherChanges() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            String account = UUID.randomUUID().toString();
            apply(tx, account, new ArchiveLedger.StatsDelta(Long.MAX_VALUE, 1, 0, 0, Map.of(), Map.of()));
            assertThatThrownBy(() -> apply(tx, account,
                    new ArchiveLedger.StatsDelta(1, 1, 0, 0, Map.of(), Map.of()))).rootCause().isInstanceOfSatisfying(java.sql.SQLException.class,
                            failure -> assertThat(failure.getSQLState()).isEqualTo("22003"));
            var actual = new ArchiveLedger(tx).findStats(account, "records").orElseThrow();
            assertThat(actual.entries).isEqualTo(Long.MAX_VALUE);
            assertThat(actual.versions).isEqualTo(1);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {0, 1, 2, 3, 4, 5})
    void underflowFailsAtomicallyForNewAndExistingCounters(int coordinate) {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            for (boolean existing : new boolean[]{false, true}) {
                String account = UUID.randomUUID().toString();
                if (existing) apply(tx, account, new ArchiveLedger.StatsDelta(1, 1, 1, 1,
                        Map.of("original", 1L), Map.of("original", 1L)));
                long[] values = {1, 1, 1, 1, 1, 1};
                values[coordinate] = existing ? -2 : -1;
                var delta = new ArchiveLedger.StatsDelta(values[0], values[1], values[2], values[3],
                        Map.of("original", values[4]), Map.of("original", values[5]));
                assertThatThrownBy(() -> apply(tx, account, delta)).rootCause()
                        .isInstanceOfSatisfying(java.sql.SQLException.class,
                                failure -> assertThat(failure.getSQLState()).isEqualTo("23514"));
                if (existing) {
                    var stats = ledger.findStats(account, "records").orElseThrow();
                    assertThat(new long[]{stats.entries, stats.versions, stats.retainedBytes, stats.currentBytes})
                            .containsExactly(1, 1, 1, 1);
                    assertThat(ledger.findRenditionStats(account, "records")).singleElement().satisfies(row -> {
                        assertThat(row.objectCount).isEqualTo(1); assertThat(row.totalBytes).isEqualTo(1);
                    });
                } else {
                    assertThat(ledger.findStats(account, "records")).isEmpty();
                    assertThat(ledger.findRenditionStats(account, "records")).isEmpty();
                }
            }
        }
    }

    @Test void legitimateDecrementsCanReachExactlyZero() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            String account = UUID.randomUUID().toString();
            apply(tx, account, new ArchiveLedger.StatsDelta(1, 2, 3, 4, Map.of("original", 5L), Map.of("original", 6L)));
            apply(tx, account, new ArchiveLedger.StatsDelta(-1, -2, -3, -4, Map.of("original", -5L), Map.of("original", -6L)));
            var ledger = new ArchiveLedger(tx);
            var stats = ledger.findStats(account, "records").orElseThrow();
            assertThat(new long[]{stats.entries, stats.versions, stats.retainedBytes, stats.currentBytes}).containsOnly(0);
            assertThat(ledger.findRenditionStats(account, "records")).singleElement().satisfies(row -> {
                assertThat(row.objectCount).isZero(); assertThat(row.totalBytes).isZero();
            });
        }
    }

    private static void apply(Tx tx, String account, ArchiveLedger.StatsDelta delta) {
        tx.inTransaction(em -> { ArchiveLedger.applyDelta(em, account, "records", delta); });
    }

    private static LedgerDatabase database() {
        return new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }
}

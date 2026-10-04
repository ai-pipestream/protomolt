package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Reconciliation against real SQL state, including explicitly injected catalog faults. */
@Testcontainers
class DocumentReadReconciliationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("reader", true);

    @Test void pendingFirstHandleDoesNotStarveLaterReleasedHandle() {
        try (var c = context(POSTGRES)) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var incarnation = UUID.randomUUID();
            var ledger = new DocumentReadLedger(c.tx(), incarnation, 2);
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            var firstPins = c.tx().readOnly(em -> em.createNativeQuery("SELECT pin_id FROM document_read_pins").getResultList());
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            ledger.fence(); ledger.attestLocalQuiescence();
            c.tx().inTransaction(em -> {
                assertThat(em.createNativeQuery("""
                        SELECT release_document_read_pins(:reader,
                          (SELECT jsonb_agg(jsonb_build_object('pin',pin_id,'object',object_id))
                           FROM document_read_pins WHERE reader_incarnation=:reader AND pin_id NOT IN (:first)))
                        """).setParameter("reader", incarnation).setParameter("first", firstPins).getSingleResult()).isEqualTo(true);
            });
            assertThat(ledger.reconcileDrained(1)).isZero();
            assertThat(ledger.reconcileDrained(1)).isEqualTo(1);
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            assertThat(new DocumentReadRecovery(c.tx()).recoverBatch(incarnation, 10)).isEqualTo(2);
            assertThat(ledger.reconcileDrained(1)).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"capture", "release"})
    void recoversAfterRealJdbcCommitAcknowledgmentIsLost(String operation) {
        try (var c = context(POSTGRES)) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var source = loseCommitAcknowledgment(c.pool(), armed);
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    java.util.Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var incarnation = UUID.randomUUID();
                var ledger = new DocumentReadLedger(new Tx(emf), incarnation, 1);
                if (operation.equals("capture")) {
                    armed.set(true);
                    assertThatThrownBy(() -> ledger.captureHistorical(ADMIN, published.getAddress(), revision))
                            .hasStackTraceContaining("Injected lost SQL commit acknowledgment");
                    assertThat(armed).isFalse();
                    assertThat(ledger.outstandingReads()).isZero(); // no handle was returned
                    assertThat(count(c, "document_read_pins")).isEqualTo(2); // commit really happened
                } else {
                    var read = ledger.captureHistorical(ADMIN, published.getAddress(), revision);
                    read.close(); armed.set(true);
                    assertThatThrownBy(read::release).hasStackTraceContaining("Injected lost SQL commit acknowledgment");
                    assertThat(armed).isFalse();
                    assertThat(ledger.outstandingReads()).isEqualTo(1); // failed acknowledgment retains ownership
                    assertThat(count(c, "document_read_pins")).isZero();
                }
                var recovery = new DocumentReadRecovery(c.tx());
                assertThatThrownBy(() -> recovery.recoverBatch(incarnation, 10)).hasStackTraceContaining("requires proven quiescence");
                ledger.fence(); ledger.attestLocalQuiescence();
                assertThat(recovery.recoverBatch(incarnation, 10)).isEqualTo(operation.equals("capture") ? 2 : 0);
                assertThat(recovery.recoverBatch(incarnation, 10)).isZero();
                assertThat(ledger.reconcileDrained(1)).isEqualTo(operation.equals("capture") ? 0 : 1);
                assertThat(ledger.outstandingReads()).isZero();
                assertThat(count(c, "document_read_pins")).isZero();
            }
        }
    }

    @Test void reconciliationRequiresQuiescenceAndEveryExactPinAbsent() {
        try (var c = context(POSTGRES)) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var incarnation = UUID.randomUUID();
            var ledger = new DocumentReadLedger(c.tx(), incarnation, 1);
            var other = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var otherRead = other.captureHistorical(ADMIN, published.getAddress(), revision);
            ledger.captureHistorical(ADMIN, published.getAddress(), revision).close();
            assertThatThrownBy(() -> ledger.reconcileDrained(1)).hasStackTraceContaining("requires proven quiescence");
            ledger.fence();
            assertThatThrownBy(() -> ledger.reconcileDrained(1)).hasStackTraceContaining("requires proven quiescence");
            ledger.attestLocalQuiescence();
            assertThat(ledger.reconcileDrained(1)).isZero();
            var recovery = new DocumentReadRecovery(c.tx());
            assertThat(recovery.recoverBatch(incarnation, 1)).isEqualTo(1);
            assertThat(ledger.reconcileDrained(1)).isZero();
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            assertThat(recovery.recoverBatch(incarnation, 1)).isEqualTo(1);
            assertThat(recovery.recoverBatch(incarnation, 1)).isZero();
            assertThat(ledger.reconcileDrained(1)).isEqualTo(1);
            assertThat(ledger.reconcileDrained(1)).isZero();
            assertThat(ledger.outstandingReads()).isZero();
            assertThat(count(c, "document_read_pins")).isEqualTo(2); // other incarnation untouched
            otherRead.close(); otherRead.release(); other.fence(); other.attestLocalQuiescence();
        }
    }

    @Test void confirmationDoesNotNeedPhysicalCatalogAfterAcknowledgmentWasLost() {
        try (var c = context(POSTGRES)) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var incarnation = UUID.randomUUID();
            var ledger = new DocumentReadLedger(c.tx(), incarnation, 1);
            var read = ledger.captureHistorical(ADMIN, published.getAddress(), UUID.fromString(published.getRevisionId()));
            read.close(); ledger.fence(); ledger.attestLocalQuiescence();
            // Release commits through the real SQL adapter without updating the local
            // handle, reproducing the state left by a lost acknowledgment.
            var recovery = new DocumentReadRecovery(c.tx());
            assertThat(recovery.recoverBatch(incarnation, 10)).isEqualTo(2);
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            c.tx().inTransaction(em -> {
                // Isolated catalog fault: simulate physical-row disappearance after
                // release. This does not claim to exercise production history pruning.
                em.createNativeQuery("SET LOCAL session_replication_role='replica'").executeUpdate();
                em.createNativeQuery("DELETE FROM repository_physical_locations").executeUpdate();
            });
            assertThatThrownBy(read::release).hasStackTraceContaining("not registered");
            assertThat(ledger.outstandingReads()).isEqualTo(1);
            assertThat(ledger.reconcileDrained(1)).isEqualTo(1);
            assertThat(ledger.outstandingReads()).isZero();
            read.release(); // already confirmed; does not replay the stale object claims
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"orphan-mirror", "wrong-native-reader", "wrong-mirror-object"})
    void corruptedOrMismatchedPinRepresentationsNeverConfirmRelease(String fault) {
        try (var c = context(POSTGRES)) {
            var published = publish(c, prepare(c, 1), Fault.NONE, em -> {}).getMembers(0);
            var incarnation = UUID.randomUUID();
            var ledger = new DocumentReadLedger(c.tx(), incarnation, 1);
            ledger.captureHistorical(ADMIN, published.getAddress(), UUID.fromString(published.getRevisionId())).close();
            ledger.fence(); ledger.attestLocalQuiescence();
            c.tx().inTransaction(em -> {
                // Inject corruption only after valid capture; all ordinary writes stay guarded.
                em.createNativeQuery("SET LOCAL session_replication_role='replica'").executeUpdate();
                switch (fault) {
                    case "orphan-mirror" -> em.createNativeQuery("DELETE FROM document_read_pins").executeUpdate();
                    case "wrong-native-reader" -> em.createNativeQuery("UPDATE document_read_pins SET reader_incarnation=:id")
                            .setParameter("id", UUID.randomUUID()).executeUpdate();
                    case "wrong-mirror-object" -> em.createNativeQuery("UPDATE repository_object_references SET object_id=:id WHERE owner_kind='DOCUMENT_READER'")
                            .setParameter("id", UUID.randomUUID()).executeUpdate();
                    default -> throw new AssertionError(fault);
                }
            });
            if (fault.equals("orphan-mirror")) {
                assertThat(new DocumentReadRecovery(c.tx()).recoverBatch(incarnation, 10)).isZero();
                assertThat(ledger.reconcileDrained(1)).isZero();
            } else {
                assertThatThrownBy(() -> ledger.reconcileDrained(1)).hasStackTraceContaining("mismatched identities");
            }
            assertThat(ledger.outstandingReads()).isEqualTo(1);
        }
    }

    /** Delegates every operation to PostgreSQL; only the response after a real commit is lost. */
    private static javax.sql.DataSource loseCommitAcknowledgment(javax.sql.DataSource delegate,
            java.util.concurrent.atomic.AtomicBoolean armed) {
        return DocumentJdbcFaults.afterCommit(delegate, () -> {
            if (armed.compareAndSet(true, false))
                throw new java.sql.SQLException("Injected lost SQL commit acknowledgment", "08006");
        });
    }
}

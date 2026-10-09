package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryOperationControl;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL recovery orchestration; fixture objects have no running provider workers. */
@Testcontainers
class ReaderHostRecoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void failedReaderDoesNotStopOtherReadersAndCancellationNeverReportsSuccess() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild()) {
            var host = child.identity.execution();
            ReaderHostExecutions.register(c.tx(), host, child.identity.host(), child.identity.boot());
            var readers = List.of(new UUID(0, 1), new UUID(0, 2), new UUID(0, 3));
            for (var reader : readers) ReaderRegistration.register(c.tx(), reader, host);
            var foreign = UUID.randomUUID(); ReaderRegistration.register(c.tx(), foreign);
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {}).getMembers(0);
            var object = UUID.fromString(prepared.sources().getFirst().identities().getFirst().getObjectId());
            var revision = UUID.fromString(published.getRevisionId());
            var firstPin = UUID.randomUUID();
            insert(c.tx(), readers.getFirst(), firstPin, object, revision);
            insert(c.tx(), readers.get(1), UUID.randomUUID(), object, revision);
            insert(c.tx(), readers.get(2), UUID.randomUUID(), object, revision);
            insert(c.tx(), foreign, UUID.randomUUID(), object, revision);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", readers.get(2)).getSingleResult();
                em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", readers.get(2)).getSingleResult();
            });
            ReaderHostExecutions.fence(c.tx(), host); child.stop();
            var receipt = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify)).record(child.identity, child.proof());
            var recovery = new ReaderHostRecovery(c.tx());
            assertThatThrownBy(() -> recovery.recoverPage(host, receipt.id(), Optional.empty(), 1000, 2, RepositoryOperationControl.NONE))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> recovery.recoverPage(host, UUID.randomUUID(), Optional.empty(), 1, 1, RepositoryOperationControl.NONE))
                    .hasMessageContaining("verified host termination");
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION reject_supervisor_release() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                          IF OLD.pin_id='%s'::uuid THEN RAISE EXCEPTION 'injected supervisor release failure'; END IF;
                          RETURN OLD;
                        END $$
                        """.formatted(firstPin)).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER reject_supervisor_release BEFORE DELETE ON document_read_pins FOR EACH ROW EXECUTE FUNCTION reject_supervisor_release()")
                        .executeUpdate();
            });
            try {
                assertThatThrownBy(() -> recovery.recoverPage(host, receipt.id(), Optional.empty(), 2, 1, RepositoryOperationControl.NONE))
                        .isInstanceOfSatisfying(ReaderHostRecovery.PageFailure.class, failure -> {
                            assertThat(failure).hasStackTraceContaining("injected supervisor release failure");
                            assertThat(failure.page().failed()).extracting(ReaderHostRecovery.Failed::reader).containsExactly(readers.getFirst());
                            assertThat(failure.page().recovered()).extracting(ReaderHostRecovery.Recovered::reader).containsExactly(readers.get(1));
                            assertThat(failure.page().recovered().getFirst().documentPins()).isEqualTo(1);
                            assertThat(failure.page().recovered().getFirst().readerResourcesDrained()).isFalse();
                            assertThat(failure.page().nextCursor()).contains(readers.get(1));
                        });
                assertThat(pins(c.tx(), readers.getFirst())).isEqualTo(1);
                assertThat(pins(c.tx(), readers.get(1))).isZero();
                var cancelled = new RepositoryException(RepositoryException.Code.CANCELLED, "cancel after failed page");
                var calls = new java.util.concurrent.atomic.AtomicInteger();
                var cancelling = new RepositoryOperationControl() {
                    public boolean isCancelled() { return false; }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public void check() { if (calls.incrementAndGet() == 4) throw cancelled; }
                };
                assertThatThrownBy(() -> recovery.recoverPage(host, receipt.id(), Optional.empty(), 2, 1, cancelling))
                        .isSameAs(cancelled).satisfies(failure -> {
                            assertThat(failure.getSuppressed()).hasSize(1);
                            assertThat(failure.getSuppressed()[0]).hasStackTraceContaining("injected supervisor release failure");
                        });
            } finally {
                c.tx().inTransaction(em -> {
                    em.createNativeQuery("DROP TRIGGER reject_supervisor_release ON document_read_pins").executeUpdate();
                    em.createNativeQuery("DROP FUNCTION reject_supervisor_release()").executeUpdate();
                });
            }
            var local = recovery.recoverPage(host, receipt.id(), Optional.of(readers.get(1)), 1, 1, RepositoryOperationControl.NONE);
            assertThat(local.recovered().getFirst().documentPins()).isEqualTo(1);
            assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery(
                    "SELECT quiescence_source FROM repository_reader_incarnations WHERE incarnation=:id")
                    .setParameter("id", readers.get(2)).getSingleResult())).isEqualTo("LOCAL_DRAIN");
            var end = recovery.recoverPage(host, receipt.id(), local.nextCursor(), 1, 1, RepositoryOperationControl.NONE);
            assertThat(end.recovered()).isEmpty(); assertThat(end.nextCursor()).isEmpty();
            assertThat(pins(c.tx(), readers.getFirst())).isEqualTo(1); // Traversal end did not imply completion.
            var stopped = new RepositoryException(RepositoryException.Code.CANCELLED, "test supervisor cancelled");
            var checks = new java.util.concurrent.atomic.AtomicInteger();
            var control = new RepositoryOperationControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public void check() { if (checks.incrementAndGet() == 3) throw stopped; }
            };
            assertThatThrownBy(() -> recovery.recoverPage(host, receipt.id(), Optional.empty(), 1, 1, control)).isSameAs(stopped);
            assertThat(pins(c.tx(), readers.getFirst())).isZero(); // Earlier commit survives final cancellation.
            var retry = recovery.recoverPage(host, receipt.id(), Optional.empty(), 3, 1, RepositoryOperationControl.NONE);
            assertThat(retry.recovered()).hasSize(3).allMatch(ReaderHostRecovery.Recovered::readerResourcesDrained);
            assertThat(pins(c.tx(), foreign)).isEqualTo(1);
        }
    }

    private static void insert(Tx tx, UUID reader, UUID pin, UUID object, UUID revision) {
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision)
                    SELECT :pin,:reader,:object,node_id,revision_id,publication_revision
                    FROM document_revision_publications WHERE revision_id=:revision
                    """).setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object)
                    .setParameter("revision", revision).executeUpdate();
        });
    }

    private static long pins(Tx tx, UUID reader) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_read_pins WHERE reader_incarnation=:id")
                .setParameter("id", reader).getSingleResult()).longValue());
    }
}

package ai.protomolt.proto.repo.container.ledger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL pin lifecycle with synthetic object fixtures; no provider read is claimed. */
@Testcontainers
class ReaderExternalQuiescenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void exactReadersRecoverSeparatelyAfterVerifiedTermination() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild()) {
            var host = child.identity.execution();
            ReaderHostExecutions.register(c.tx(), host, child.identity.host(), child.identity.boot());
            var first = UUID.randomUUID(); var second = UUID.randomUUID();
            ReaderRegistration.register(c.tx(), first, host); ReaderRegistration.register(c.tx(), second, host);
            var publication = prepare(c, 1);
            var published = publish(c, publication, Fault.NONE, em -> {}).getMembers(0);
            var revision = UUID.fromString(published.getRevisionId());
            var object = UUID.fromString(publication.sources().getFirst().identities().getFirst().getObjectId());
            for (var reader : java.util.List.of(first, second)) c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                    INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision)
                    SELECT :pin,:reader,:object,node_id,revision_id,publication_revision
                    FROM document_revision_publications WHERE revision_id=:revision
                    """).setParameter("pin", UUID.randomUUID()).setParameter("reader", reader)
                    .setParameter("object", object).setParameter("revision", revision).executeUpdate();
            });
            ReaderHostExecutions.fence(c.tx(), host);
            var recovery = new DocumentReadRecovery(c.tx());
            assertThatThrownBy(() -> recovery.recoverBatch(first, 1)).hasStackTraceContaining("proven quiescence");
            child.stop();
            var termination = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify))
                .record(child.identity, child.proof());
            assertThatThrownBy(() -> recovery.recoverBatch(first, 1)).hasStackTraceContaining("proven quiescence");
            assertThat(pins(c.tx())).isEqualTo(2);
            var service = new ReaderExternalQuiescence(c.tx());
            var receipt = service.quiesce(first, nonce(c.tx(), first), host, termination.id());
            assertThat(service.quiesce(first, nonce(c.tx(), first), host, termination.id())).isEqualTo(receipt);
            assertThat(pins(c.tx())).isEqualTo(2);
            assertThat(recovery.recoverBatch(first, 1)).isEqualTo(1);
            assertThat(recovery.recoverBatch(first, 1)).isZero();
            assertThat(pins(c.tx())).isEqualTo(1);
            assertThatThrownBy(() -> recovery.recoverBatch(second, 1)).hasStackTraceContaining("proven quiescence");
            service.quiesce(second, nonce(c.tx(), second), host, termination.id());
            assertThat(recovery.recoverBatch(second, 1)).isEqualTo(1);
            assertThat(pins(c.tx())).isZero();
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_READER'").getSingleResult()).longValue())).isZero();
        }
    }

    @Test void wrongIdentityAndLocalProvenanceCannotBeReplaced() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild()) {
            var host = child.identity.execution();
            ReaderHostExecutions.register(c.tx(), host, child.identity.host(), child.identity.boot());
            var reader = UUID.randomUUID(); var local = UUID.randomUUID(); var drained = UUID.randomUUID();
            ReaderRegistration.register(c.tx(), reader, host); ReaderRegistration.register(c.tx(), local);
            var ledger = new DocumentReadLedger(c.tx(), drained, host); ledger.fence(); ledger.attestLocalQuiescence();
            ReaderHostExecutions.fence(c.tx(), host); child.stop();
            var termination = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify)).record(child.identity, child.proof());
            var service = new ReaderExternalQuiescence(c.tx()); var nonce = nonce(c.tx(), reader);
            assertThatThrownBy(() -> service.quiesce(reader, nonce, host, UUID.randomUUID())).hasStackTraceContaining("verified host termination");
            assertThatThrownBy(() -> service.quiesce(reader, nonce, UUID.randomUUID(), termination.id())).hasStackTraceContaining("verified host termination");
            assertThatThrownBy(() -> service.quiesce(reader, UUID.randomUUID(), host, termination.id())).hasStackTraceContaining("registration mismatch");
            assertThatThrownBy(() -> service.quiesce(local, nonce(c.tx(), local), host, termination.id())).hasStackTraceContaining("registration mismatch");
            assertThatThrownBy(() -> service.quiesce(drained, nonce(c.tx(), drained), host, termination.id())).hasStackTraceContaining("existing reader provenance");
            c.tx().inTransaction(em -> { em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult(); });
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                    INSERT INTO repository_reader_external_quiescence(incarnation,receipt_id,registration_nonce,host_execution,termination_receipt)
                    VALUES(:id,:receipt,:nonce,:host,:termination)
                    """).setParameter("id", reader).setParameter("receipt", UUID.randomUUID()).setParameter("nonce", nonce)
                    .setParameter("host", host).setParameter("termination", termination.id()).executeUpdate();
            })).hasStackTraceContaining("must commit with HOST_TERMINATION");
            service.quiesce(reader, nonce, host, termination.id());

            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", reader).getSingleResult();
            })).hasStackTraceContaining("cannot be claimed as local");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM repository_reader_external_quiescence WHERE incarnation=:id").setParameter("id", reader).executeUpdate();
            })).hasStackTraceContaining("immutable");
        }
    }

    @Test void lostAcknowledgmentAndConcurrentRetriesReturnSameReceipt() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild();
                var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var host = child.identity.execution(); var reader = UUID.randomUUID();
            ReaderHostExecutions.register(c.tx(), host, child.identity.host(), child.identity.boot());
            ReaderRegistration.register(c.tx(), reader, host); var nonce = nonce(c.tx(), reader);
            ReaderHostExecutions.fence(c.tx(), host); child.stop();
            var termination = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify)).record(child.identity, child.proof());
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("Reader quiescence acknowledgment lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var uncertain = new ReaderExternalQuiescence(new Tx(emf)); armed.set(true);
                assertThatThrownBy(() -> uncertain.quiesce(reader, nonce, host, termination.id())).hasStackTraceContaining("acknowledgment lost");
            }
            var normal = new ReaderExternalQuiescence(c.tx());
            var first = workers.submit(() -> normal.quiesce(reader, nonce, host, termination.id()));
            var second = workers.submit(() -> normal.quiesce(reader, nonce, host, termination.id()));
            var receipt = first.get(10, TimeUnit.SECONDS);
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(receipt);
            assertThat(c.tx().readOnly(em -> em.createNativeQuery("SELECT receipt_id FROM repository_reader_external_quiescence WHERE incarnation=:id")
                .setParameter("id", reader).getSingleResult()).toString()).isEqualTo(receipt.toString());
        }
    }

    @Test void externalProvenanceCannotBeSetWithoutReceipt() {
        try (var c = context(POSTGRES)) {
            var reader = UUID.randomUUID(); ReaderRegistration.register(c.tx(), reader);
            c.tx().inTransaction(em -> { em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult(); });
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_reader_incarnations SET state='QUIESCED',quiesced_at=clock_timestamp(),quiescence_source='HOST_TERMINATION' WHERE incarnation=:id")
                    .setParameter("id", reader).executeUpdate();
            })).hasStackTraceContaining("matching reader receipt");
        }
    }

    @Test void waitingReaderDoesNotLockHostOrOtherReaders() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild();
                var blocker = c.pool().getConnection(); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            // Dedicated blocker, two waiters and an observer need four connections.
            c.pool().setMaximumPoolSize(4);
            var host = child.identity.execution(); var first = UUID.randomUUID(); var second = UUID.randomUUID();
            ReaderHostExecutions.register(c.tx(), host, child.identity.host(), child.identity.boot());
            ReaderRegistration.register(c.tx(), first, host); ReaderRegistration.register(c.tx(), second, host);
            var firstNonce = nonce(c.tx(), first); var secondNonce = nonce(c.tx(), second);
            ReaderHostExecutions.fence(c.tx(), host); child.stop();
            var termination = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify)).record(child.identity, child.proof());
            var service = new ReaderExternalQuiescence(c.tx());
            blocker.setAutoCommit(false);
            int pid;
            try (var s = blocker.createStatement(); var r = s.executeQuery("SELECT pg_backend_pid()")) { r.next(); pid = r.getInt(1); }
            try {
                try (var s = blocker.prepareStatement("SELECT incarnation FROM repository_reader_incarnations WHERE incarnation=? FOR UPDATE")) {
                    s.setObject(1, first); s.execute();
                }
                var waiting = workers.submit(() -> service.quiesce(first, firstNonce, host, termination.id()));
                var competing = workers.submit(() -> service.quiesce(first, firstNonce, host, termination.id()));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                boolean blocked;
                do {
                    blocked = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        """
                        WITH RECURSIVE waiting(pid) AS (
                            SELECT pid FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))
                            UNION
                            SELECT a.pid FROM pg_stat_activity a JOIN waiting w ON w.pid=ANY(pg_blocking_pids(a.pid))
                        ) SELECT count(*) FROM waiting
                        """)
                        .setParameter("pid", pid).getSingleResult()).intValue()) >= 2;
                    if (!blocked) Thread.sleep(10);
                } while (!blocked && System.nanoTime() < deadline);
                assertThat(blocked).isTrue();
                // The first quiescence is waiting on its reader, without a host row lock.
                c.tx().inTransaction(em -> { em.createNativeQuery(
                    "SELECT execution FROM repository_reader_host_executions WHERE execution=:id FOR UPDATE NOWAIT")
                    .setParameter("id", host).getSingleResult(); });
                assertThat(service.quiesce(second, secondNonce, host, termination.id())).isNotNull();
                assertThat(waiting).isNotDone();
                blocker.commit();
                assertThat(waiting.get(10, TimeUnit.SECONDS)).isEqualTo(competing.get(10, TimeUnit.SECONDS));
            } finally { blocker.rollback(); }
        }
    }

    private static UUID nonce(Tx tx, UUID reader) {
        return tx.readOnly(em -> (UUID) em.createNativeQuery("SELECT registration_nonce FROM repository_reader_incarnations WHERE incarnation=:id")
            .setParameter("id", reader).getSingleResult());
    }
    private static long pins(Tx tx) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_read_pins").getSingleResult()).longValue());
    }
}

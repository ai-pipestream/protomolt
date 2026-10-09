package ai.protomolt.proto.repo.container.ledger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

/** SQL receipt correctness with a verifier for an actual managed child, not remote-host qualification. */
@Testcontainers
class ReaderHostTerminationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void requiresRealExitAndPreservesReadersUntilSeparateQuiescence() throws Exception {
        try (var c = context(POSTGRES); var child = new ManagedChild()) {
            var identity = child.identity;
            ReaderHostExecutions.register(c.tx(), identity.execution(), identity.host(), identity.boot());
            var reader = UUID.randomUUID(); ReaderRegistration.register(c.tx(), reader, identity.execution());
            var service = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify));
            var proof = child.proof();
            assertThatThrownBy(() -> new ReaderHostTermination(c.tx(), Map.of()).record(identity, proof))
                .hasMessageContaining("No trusted verifier");
            ReaderHostExecutions.fence(c.tx(), identity.execution());
            assertThatThrownBy(() -> service.record(identity, proof)).hasMessageContaining("still running");
            child.stop();
            assertThatThrownBy(() -> service.record(new ReaderHostTermination.Identity(identity.execution(), identity.host(), "wrong"), proof))
                .hasMessageContaining("identity");
            assertThatThrownBy(() -> service.record(identity, new ReaderHostTermination.Evidence("managed-child", 2, proof.attestation(), proof.bytes())))
                .hasMessageContaining("proof");
            var receipt = service.record(identity, proof);
            assertThat(service.record(identity, proof)).isEqualTo(receipt);
            assertThatThrownBy(() -> service.record(identity, child.proof())).hasMessageContaining("Conflicting");
            assertThat(state(c.tx(), identity.execution())).isEqualTo("TERMINATED");
            assertThat(c.tx().readOnly(em -> em.createNativeQuery("SELECT state FROM repository_reader_incarnations WHERE incarnation=:id")
                .setParameter("id", reader).getSingleResult()).toString()).isEqualTo("ACTIVE");
            assertThatThrownBy(() -> new DocumentReadRecovery(c.tx()).recoverBatch(reader, 1))
                .hasStackTraceContaining("proven quiescence");
            assertThatThrownBy(() -> ReaderRegistration.register(c.tx(), UUID.randomUUID(), identity.execution()))
                .hasStackTraceContaining("not ACTIVE");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM repository_reader_host_terminations WHERE execution=:id")
                    .setParameter("id", identity.execution()).executeUpdate();
            })).hasStackTraceContaining("immutable");
        }
    }

    @Test void uncertainCommitReplaysTheOriginalReceipt() throws Exception {
        try (var c = context(POSTGRES); var child = new ManagedChild()) {
            var identity = child.identity; var proof = child.proof();
            ReaderHostExecutions.register(c.tx(), identity.execution(), identity.host(), identity.boot());
            child.stop();
            var normal = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify));
            assertThatThrownBy(() -> normal.record(identity, proof)).hasMessageContaining("fenced");
            ReaderHostExecutions.fence(c.tx(), identity.execution());
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("Termination acknowledgment lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var uncertain = new ReaderHostTermination(new Tx(emf), Map.of("managed-child", child::verify));
                armed.set(true);
                assertThatThrownBy(() -> uncertain.record(identity, proof)).hasStackTraceContaining("acknowledgment lost");
            }
            var stored = c.tx().readOnly(em -> em.createNativeQuery("SELECT receipt_id FROM repository_reader_host_terminations WHERE execution=:id")
                .setParameter("id", identity.execution()).getSingleResult());
            assertThat(normal.record(identity, proof).id()).isEqualTo(stored);
            assertThat(normal.record(identity, proof).id()).isEqualTo(stored);
        }
    }

    @Test void databaseRequiresReceiptAndTransitionInSameCommit() {
        try (var c = context(POSTGRES)) {
            var id = UUID.randomUUID(); ReaderHostExecutions.register(c.tx(), id, "host", "boot");
            ReaderHostExecutions.fence(c.tx(), id);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_reader_host_executions SET state='TERMINATED' WHERE execution=:id")
                    .setParameter("id", id).executeUpdate();
            })).hasStackTraceContaining("matching receipt");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                    INSERT INTO repository_reader_host_terminations(execution,receipt_id,host_identity,boot_identity,
                        verifier,proof_format,attestation_id,evidence_sha256)
                    VALUES(:id,:receipt,'host','boot','sql-fixture',1,:attestation,:digest)
                    """).setParameter("id", id).setParameter("receipt", UUID.randomUUID())
                    .setParameter("attestation", UUID.randomUUID()).setParameter("digest", "a".repeat(64)).executeUpdate();
            })).hasStackTraceContaining("must commit with TERMINATED");
            assertThat(state(c.tx(), id)).isEqualTo("FENCED");
        }
    }

    @Test void concurrentVerifiedRetriesChooseOneReceipt() throws Exception {
        try (var c = context(POSTGRES); var child = new ManagedChild();
                var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var identity = child.identity; var proof = child.proof();
            ReaderHostExecutions.register(c.tx(), identity.execution(), identity.host(), identity.boot());
            ReaderHostExecutions.fence(c.tx(), identity.execution()); child.stop();
            var ready = new java.util.concurrent.CyclicBarrier(2);
            var service = new ReaderHostTermination(c.tx(), Map.of("managed-child", (id, evidence) -> {
                child.verify(id, evidence);
                try { ready.await(10, TimeUnit.SECONDS); }
                catch (Exception failure) { throw new IllegalStateException("Verification barrier failed", failure); }
            }));
            var first = workers.submit(() -> service.record(identity, proof));
            var second = workers.submit(() -> service.record(identity, proof));
            assertThat(first.get(15, TimeUnit.SECONDS)).isEqualTo(second.get(15, TimeUnit.SECONDS));
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_host_terminations WHERE execution=:id")
                .setParameter("id", identity.execution()).getSingleResult()).intValue())).isEqualTo(1);
        }
    }

    @Test void attestationCannotBeReassignedToAnotherExecution() throws Exception {
        try (var c = context(POSTGRES); var first = new ManagedChild(); var second = new ManagedChild()) {
            for (var child : java.util.List.of(first, second)) {
                ReaderHostExecutions.register(c.tx(), child.identity.execution(), child.identity.host(), child.identity.boot());
                ReaderHostExecutions.fence(c.tx(), child.identity.execution()); child.stop();
            }
            var service = new ReaderHostTermination(c.tx(), Map.of("managed-child", (id, evidence) -> {
                if (id.equals(first.identity)) first.verify(id, evidence); else second.verify(id, evidence);
            }));
            var proof = first.proof(); service.record(first.identity, proof);
            var reused = new ReaderHostTermination.Evidence("managed-child", 1, proof.attestation(), second.bytes(proof.attestation()));
            assertThatThrownBy(() -> service.record(second.identity, reused)).hasStackTraceContaining("duplicate key");
            assertThat(state(c.tx(), first.identity.execution())).isEqualTo("TERMINATED");
            assertThat(state(c.tx(), second.identity.execution())).isEqualTo("FENCED");
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_host_terminations WHERE execution=:id")
                .setParameter("id", second.identity.execution()).getSingleResult()).intValue())).isZero();
        }
    }

    private static String state(Tx tx, UUID id) {
        return tx.readOnly(em -> em.createNativeQuery("SELECT state FROM repository_reader_host_executions WHERE execution=:id")
            .setParameter("id", id).getSingleResult()).toString();
    }

    public static final class Child {
        public static void main(String[] args) throws Exception {
            System.out.println("READY"); System.out.flush(); System.in.read();
        }
    }

    /** Only this managed child is covered; it has no subprocess workers or database sessions. */
    static final class ManagedChild implements AutoCloseable {
        final ReaderHostTermination.Identity identity = new ReaderHostTermination.Identity(UUID.randomUUID(), "managed-test-host", UUID.randomUUID().toString());
        final Process process;
        ManagedChild() throws Exception {
            process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", Path.of(Child.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString(), Child.class.getName())
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                try {
                    var ready = workers.submit(() -> process.inputReader().readLine());
                    assertThat(ready.get(10, TimeUnit.SECONDS)).isEqualTo("READY");
                } catch (Exception | AssertionError failure) {
                    process.destroyForcibly();
                    if (!process.waitFor(10, TimeUnit.SECONDS))
                        failure.addSuppressed(new IllegalStateException("Failed test process did not stop"));
                    throw failure;
                }
            }

        }
        ReaderHostTermination.Evidence proof() {
            var id = UUID.randomUUID();
            return new ReaderHostTermination.Evidence("managed-child", 1, id, bytes(id));
        }
        private byte[] bytes(UUID id) {
            return (identity.execution() + ":" + process.pid() + ":" + id).getBytes(StandardCharsets.UTF_8);
        }
        void verify(ReaderHostTermination.Identity candidate, ReaderHostTermination.Evidence proof) {
            if (!identity.equals(candidate)) throw new IllegalArgumentException("Wrong managed execution identity");
            if (proof.format() != 1 || !"managed-child".equals(proof.verifier()) || !java.util.Arrays.equals(proof.bytes(), bytes(proof.attestation())))
                throw new IllegalArgumentException("Unsupported or mismatched managed proof");
            if (process.isAlive()) throw new IllegalStateException("Managed execution is still running");
            if (process.exitValue() != 0) throw new IllegalStateException("Unexpected managed execution exit");
        }
        void stop() throws Exception {
            process.getOutputStream().write(1); process.getOutputStream().flush();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
        @Override public void close() throws Exception {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(10, TimeUnit.SECONDS)) throw new IllegalStateException("Managed test process did not stop");
            }
        }
    }
}

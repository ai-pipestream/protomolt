package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL condition decisions; stored part observations come from the synthetic publication fixture. */
@Testcontainers
class DocumentPublicationPreconditionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"destination", "explicit-source", "reused-source", "if-absent"})
    void onlyCurrentlyFailedAuthorizedConditionsBecomeTerminal(String kind) {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            var member = p.command().intent().getMembers(0).toBuilder();
            var second = p.command().intent().getMembers(1);
            if (kind.equals("explicit-source")) member.addSources(second.getDestination());
            if (kind.equals("reused-source")) member.clearParts().addAllParts(second.getPartsList());
            if (kind.equals("if-absent")) member.setDestination(member.getDestination().toBuilder().setIfAbsent(true))
                    .clearParts().addAllParts(second.getPartsList());
            var command = new DocumentPublicationCommand(p.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                    .clearMembers().addMembers(member).build());
            var owner = admit(c, command);
            var decisions = new DocumentPublicationRejections(c.tx());
            if (!kind.equals("if-absent")) {
                var snapshot = new RepositoryOperationLedger(c.tx()).find(owner.key());
                assertThat(decisions.rejectRevisionPreconditions(CALLER, owner, command, RepositoryReadControl.NONE).state())
                        .isEqualTo(DocumentPublicationReplay.State.PENDING);
                assertThat(new RepositoryOperationLedger(c.tx()).find(owner.key())).isEqualTo(snapshot);
                assertThat(count(c, "repository_operation_rejection")).isZero();
                var node = p.sources().get(kind.equals("destination") ? 0 : 1).row().nodeId;
                change(c, node);
            }
            var receipt = decisions.rejectRevisionPreconditions(CALLER, owner, command, RepositoryReadControl.NONE).rejection().orElseThrow();
            assertThat(receipt.getDisposition()).isEqualTo(DocumentPublicationDisposition.DOCUMENT_PUBLICATION_DISPOSITION_REJECTED);
            assertThat(receipt.getReason()).isEqualTo(DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_PRECONDITION_NOT_MET);
            assertThat(receipt.getCommandSha256()).isEqualTo(command.sha256());
            assertThat(decisions.rejectRevisionPreconditions(CALLER, owner, command, RepositoryReadControl.NONE).rejection()).contains(receipt);
            assertThat(decisions.cancel(CALLER, owner, command, RepositoryReadControl.NONE).rejection()).contains(receipt);
            assertThat(count(c, "repository_operation_success")).isZero();
            assertThat(count(c, "document_events_outbox")).isZero();
        }
    }

    @Test void aVisibleConflictDoesNotBypassAnotherTargetsAuthorization() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            change(c, p.sources().getFirst().row().nodeId);
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .setParameter("id", p.sources().getFirst().row().nodeId).executeUpdate(); });
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> new DocumentPublicationRejections(c.tx())
                    .rejectRevisionPreconditions(caller, p.owner(), p.command(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(count(c, "repository_operation_rejection")).isZero();
        }
    }

    @Test void successAndReplacedOwnershipCannotBeOverwrittenByAConflict() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var result = publish(c, p, Fault.NONE, em -> {});
            change(c, p.sources().getFirst().row().nodeId);
            assertThat(new DocumentPublicationRejections(c.tx())
                    .rejectRevisionPreconditions(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).result()).contains(result);
            assertThat(count(c, "repository_operation_rejection")).isZero();
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1, false, Duration.ofSeconds(1));
            change(c, p.sources().getFirst().row().nodeId);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) FROM repository_operation_owners WHERE operation_id=:id")
                    .setParameter("id", p.command().operationId()).getSingleResult());
            var owner = new RepositoryOperationLedger(c.tx()).takeOver(p.owner().key(), p.command(), 1, UUID.randomUUID(), Duration.ofMinutes(5));
            var decisions = new DocumentPublicationRejections(c.tx());
            assertThatThrownBy(() -> decisions.rejectRevisionPreconditions(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThat(count(c, "repository_operation_rejection")).isZero();
            assertThat(decisions.rejectRevisionPreconditions(CALLER, owner, p.command(), RepositoryReadControl.NONE)
                    .rejection().orElseThrow().getOwnerGeneration()).isEqualTo(2);
        }
    }

    @Test void missingExpectedTargetDoesNotBecomeARejectionReceipt() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            c.tx().inTransaction(em -> { em.createNativeQuery("DELETE FROM documents").executeUpdate(); });
            assertThatThrownBy(() -> new DocumentPublicationRejections(c.tx())
                    .rejectRevisionPreconditions(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(count(c, "repository_operation_rejection")).isZero();
        }
    }

    private static RepositoryOperationLedger.Owner admit(Context c, DocumentPublicationCommand command) {
        return new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
    }

    @Test void decisionSeesARevisionChangeCommittedWhileWaitingForItsSharedLock() throws Exception {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            try (var connection = c.pool().getConnection(); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                connection.setAutoCommit(false);
                int pid;
                try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                    rows.next(); pid = rows.getInt(1);
                }
                try (var statement = connection.createStatement()) { statement.executeUpdate("UPDATE documents SET filename='concurrent-change'"); }
                var waiting = executor.submit(() -> new DocumentPublicationRejections(c.tx())
                        .rejectRevisionPreconditions(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE));
                try {
                    awaitBlocked(c, pid);
                    connection.commit();
                    assertThat(waiting.get(10, java.util.concurrent.TimeUnit.SECONDS).rejection().orElseThrow().getReason())
                            .isEqualTo(DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_PRECONDITION_NOT_MET);
                } finally { connection.rollback(); }
            }
        }
    }

    @Test void matchingObservationHoldsSharedLocksUntilCommitAndDoesNotPromiseFutureValidity() throws Exception {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var ready = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var pid = new java.util.concurrent.atomic.AtomicInteger();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (armed.compareAndSet(true, false)) {
                    try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                        rows.next(); pid.set(rows.getInt(1));
                    }
                    ready.countDown();
                    try {
                        if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("Condition gate timed out");
                    } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(interrupted); }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                armed.set(true);
                var observation = executor.submit(() -> new DocumentPublicationRejections(new Tx(emf))
                        .rejectRevisionPreconditions(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE));
                try {
                    assertThat(ready.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    var writer = executor.submit(() -> change(c, p.sources().getFirst().row().nodeId));
                    awaitBlocked(c, pid.get());
                    release.countDown();
                    assertThat(observation.get(10, java.util.concurrent.TimeUnit.SECONDS).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
                    writer.get(10, java.util.concurrent.TimeUnit.SECONDS);
                    assertThat(count(c, "repository_operation_rejection")).isZero();
                    assertThat(new DocumentPublicationRejections(c.tx())
                            .rejectRevisionPreconditions(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).rejection()).isPresent();
                } finally { release.countDown(); }
            }
        }
    }

    private static void awaitBlocked(Context c, int pid) throws InterruptedException {
        boolean blocked = false;
        long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!blocked && System.nanoTime() < end) {
            blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))")
                    .setParameter("pid", pid).getSingleResult());
            if (!blocked) Thread.sleep(10);
        }
        assertThat(blocked).as("actual PostgreSQL condition lock wait").isTrue();
    }

    private static void change(Context c, UUID node) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET filename='changed' WHERE node_id=:id")
                .setParameter("id", node).executeUpdate(); });
    }
}

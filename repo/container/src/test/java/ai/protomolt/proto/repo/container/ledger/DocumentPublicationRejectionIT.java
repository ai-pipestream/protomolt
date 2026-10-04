package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejectionReason;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL decision/replay tests; publication fixture observations are synthetic. */
@Testcontainers
class DocumentPublicationRejectionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void explicitCancellationIsImmutableAndFencesLaterWorkWithoutDeletingAttempts() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2, true);
            var decisions = new DocumentPublicationRejections(c.tx());
            var before = count(c, "document_revision_publications");
            var receipt = decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).rejection().orElseThrow();
            assertThat(receipt.getReason()).isEqualTo(DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_EXPLICIT_CANCELLATION);
            assertThat(decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).rejection()).contains(receipt);
            var observed = new DocumentPublicationReplay(c.tx()).observe(CALLER, p.command());
            assertThat(observed.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
            assertThatThrownBy(observed::requireNotTerminated).isInstanceOfSatisfying(DocumentPublicationReplay.Terminated.class,
                    failure -> assertThat(failure.receipt()).isEqualTo(receipt));
            var operations = new RepositoryOperationLedger(c.tx());
            assertThatThrownBy(() -> operations.renew(p.owner(), Duration.ofMinutes(1))).isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> operations.takeOver(p.owner().key(), p.command(), 1, UUID.randomUUID(), Duration.ofMinutes(1)))
                    .isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> operations.admit(p.owner().key(), p.command(), p.owner().token(), Duration.ofMinutes(1)))
                    .isInstanceOf(RepositoryOperationLedger.TerminalOperationException.class);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, em -> {})).hasStackTraceContaining("terminal");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery("DELETE FROM repository_operation_rejection").executeUpdate(); }))
                    .hasStackTraceContaining("immutable");
            assertThat(count(c, "repository_operation_rejection")).isEqualTo(1);
            assertThat(count(c, "repository_operation_success")).isZero();
            assertThat(count(c, "document_events_outbox")).isZero();
            assertThat(count(c, "document_revision_publications")).isEqualTo(before);
            assertThat(count(c, "document_part_attempt_objects")).isGreaterThan(0);
            c.tx().inTransaction(em -> { em.createNativeQuery("SELECT fence_repository_operation_recovery(:account,:principal,:id)")
                    .setParameter("account", "account").setParameter("principal", "principal").setParameter("id", p.command().operationId()).getSingleResult(); });
        }
    }

    @Test void committedPublicationWinsAndChangedCommandCannotCancelIt() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var result = publish(c, p, Fault.NONE, em -> {});
            var decisions = new DocumentPublicationRejections(c.tx());
            assertThat(decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).result()).contains(result);
            var changed = new DocumentPublicationCommand(p.command().intent().toBuilder().setMembers(0,
                    p.command().intent().getMembers(0).toBuilder().setMemberId("changed")).build());
            assertThatThrownBy(() -> decisions.cancel(CALLER, p.owner(), changed, RepositoryReadControl.NONE))
                    .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
            assertThat(count(c, "repository_operation_rejection")).isZero();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void committedDecisionSurvivesLostAcknowledgmentOrLateTransportCancellation(boolean cancel) {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var armed = new AtomicBoolean(true);
            var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) {
                    if (cancel) cancelled.set(true);
                    else throw new java.sql.SQLException("Rejection acknowledgment lost", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var control = new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() { return cancelled.get(); }
                };
                var decisions = new DocumentPublicationRejections(new Tx(emf));
                if (cancel) assertThat(decisions.cancel(CALLER, p.owner(), p.command(), control).rejection()).isPresent();
                else assertThatThrownBy(() -> decisions.cancel(CALLER, p.owner(), p.command(), control)).hasStackTraceContaining("Rejection acknowledgment lost");
                assertThat(armed).isFalse();
                var stored = new DocumentPublicationReplay(c.tx()).observe(CALLER, p.command()).rejection().orElseThrow();
                assertThat(decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).rejection()).contains(stored);
                assertThat(count(c, "repository_operation_rejection")).isEqualTo(1);
            }
        }
    }

    @Test void cancelledControlAndStaleOwnersDoNotCreateTerminalDecisions() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1, false, Duration.ofSeconds(1));
            var decisions = new DocumentPublicationRejections(c.tx());
            var cancelled = new RepositoryReadControl() {
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public boolean isCancelled() { return true; }
            };
            assertThatThrownBy(() -> decisions.cancel(CALLER, p.owner(), p.command(), cancelled))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) FROM repository_operation_owners WHERE operation_id=:id")
                    .setParameter("id", p.command().operationId()).getSingleResult());
            var owner = new RepositoryOperationLedger(c.tx()).takeOver(p.owner().key(), p.command(), 1, UUID.randomUUID(), Duration.ofMinutes(1));
            assertThatThrownBy(() -> decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            assertThat(count(c, "repository_operation_rejection")).isZero();
            assertThat(decisions.cancel(CALLER, owner, p.command(), RepositoryReadControl.NONE).rejection().orElseThrow().getOwnerGeneration()).isEqualTo(2);
        }
    }

    @Test void cancellationAndReplayRequireCurrentAccess() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            var decisions = new DocumentPublicationRejections(c.tx());
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> decisions.cancel(caller, p.owner(), p.command(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(count(c, "repository_operation_rejection")).isZero();
            decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE);
            assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx()).observe(caller, p.command()))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("other", true), p.command()).state())
                    .isEqualTo(DocumentPublicationReplay.State.NOT_OBSERVED);
        }
    }

    @Test void migrationPreservesExistingSuccessfulPublication() {
        try (var c = context(POSTGRES, "63")) {
            var p = prepare(c, 1);
            var result = publish(c, p, Fault.NONE, em -> {});
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, p.command()).result()).contains(result);
            assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE).result())
                    .contains(result);
            assertThat(count(c, "repository_operation_rejection")).isZero();
        }
    }

    @Test void cancellationWaitsForPublicationAndReturnsItsSuccess() throws Exception {
        try (var c = context(POSTGRES); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var p = prepare(c, 1);
            var entered = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CompletableFuture<Void>();
            var pid = new java.util.concurrent.atomic.AtomicInteger();
            var publication = executor.submit(() -> publish(c, p, Fault.NONE, em -> {
                pid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                entered.countDown(); release.join();
            }));
            try {
                assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var cancellation = executor.submit(() -> new DocumentPublicationRejections(c.tx())
                        .cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE));
                awaitBlocked(c, pid.get());
                release.complete(null);
                assertThat(cancellation.get(10, java.util.concurrent.TimeUnit.SECONDS).result())
                        .contains(publication.get(10, java.util.concurrent.TimeUnit.SECONDS));
                assertThat(count(c, "repository_operation_rejection")).isZero();
            } finally { release.complete(null); }
        }
    }

    @Test void publicationWaitsForCancellationAndCannotCommitAfterIt() throws Exception {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var entered = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var pid = new java.util.concurrent.atomic.AtomicInteger();
            var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (armed.compareAndSet(true, false)) {
                    try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                        result.next(); pid.set(result.getInt(1));
                    }
                    entered.countDown();
                    try {
                        if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("Cancellation gate timed out");
                    } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(interrupted); }
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"));
                    var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var cancellation = executor.submit(() -> new DocumentPublicationRejections(new Tx(emf))
                        .cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE));
                try {
                    assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    var publication = executor.submit(() -> publish(c, p, Fault.NONE, em -> {}));
                    awaitBlocked(c, pid.get());
                    release.countDown();
                    assertThat(cancellation.get(10, java.util.concurrent.TimeUnit.SECONDS).rejection()).isPresent();
                    assertThatThrownBy(() -> publication.get(10, java.util.concurrent.TimeUnit.SECONDS)).hasStackTraceContaining("terminal");
                    assertThat(count(c, "repository_operation_success")).isZero();
                    assertThat(count(c, "repository_operation_rejection")).isEqualTo(1);
                    assertThat(count(c, "document_events_outbox")).isZero();
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
        assertThat(blocked).as("real PostgreSQL owner lock wait").isTrue();
    }

    @Test void replayReauthorizesASeparateSourceAndRejectsCorruptReceiptHeaders() throws Exception {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            var command = new DocumentPublicationCommand(p.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                    .clearMembers().addMembers(p.command().intent().getMembers(0).toBuilder()
                            .addSources(p.command().intent().getMembers(1).getDestination())).build());
            var owner = new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                    command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
            var receipt = new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, command, RepositoryReadControl.NONE).rejection().orElseThrow();
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:id")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .setParameter("id", p.sources().getFirst().row().nodeId).executeUpdate(); });
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx()).observe(caller, command))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .executeUpdate(); });
            assertThat(new DocumentPublicationReplay(c.tx()).observe(caller, command).rejection()).contains(receipt);
            try (var connection = c.pool().getConnection(); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                connection.setAutoCommit(false);
                int pid;
                try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                    rows.next(); pid = rows.getInt(1);
                }
                try (var statement = connection.prepareStatement("UPDATE documents SET security='{}'::jsonb WHERE node_id=?")) {
                    statement.setObject(1, p.sources().get(1).row().nodeId); statement.executeUpdate();
                }
                var waiting = executor.submit(() -> new DocumentPublicationReplay(c.tx()).observe(caller, command));
                try {
                    awaitBlocked(c, pid);
                    connection.commit();
                    assertThatThrownBy(() -> waiting.get(10, java.util.concurrent.TimeUnit.SECONDS))
                            .cause().isInstanceOfSatisfying(RepositoryException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                } finally { connection.rollback(); }
            }
            // Isolated corruption injection: normal SQL cannot mutate this immutable row.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
                em.createNativeQuery("UPDATE repository_operation_rejection SET disposition=1,reason=1").executeUpdate();
            });
            assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx()).observe(CALLER, command))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
        }
    }

    @Test void terminalWriteFenceAppliesBeforeTheDecisionTransactionCommits() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (armed.compareAndSet(true, false)) try (var statement = connection.prepareStatement(
                        "SELECT require_repository_operation_write_fence(?,?,?,?)")) {
                    statement.setString(1, "account"); statement.setString(2, "principal");
                    statement.setObject(3, p.command().operationId()); statement.setLong(4, p.owner().generation());
                    statement.execute();
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                assertThatThrownBy(() -> new DocumentPublicationRejections(new Tx(emf))
                        .cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE)).hasStackTraceContaining("terminal");
                assertThat(armed).isFalse();
                assertThat(count(c, "repository_operation_rejection")).isZero();
                assertThat(count(c, "repository_operation_success")).isZero();
            }
        }
    }

    @Test void observationSupportsFullDestinationAndSourceUnionWithoutExpandingAdmissionLimits() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var nodes = new java.util.HashSet<UUID>();
            nodes.add(p.sources().getFirst().row().nodeId);
            while (nodes.size() < 10064) nodes.add(UUID.randomUUID());
            var observed = c.tx().inTransaction(em -> { return DocumentRevisionLocks.lockForObservation(em, nodes); });
            assertThat(observed.get(p.sources().getFirst().row().nodeId).accountId()).isEqualTo("account");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return DocumentRevisionLocks.lockForAdmission(em, Set.of(), nodes); }))
                    .hasMessageContaining("10000 read nodes");
            nodes.add(UUID.randomUUID());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return DocumentRevisionLocks.lockForObservation(em, nodes); }))
                    .hasMessageContaining("10064 read nodes");
        }
    }

    @Test void uncreatedDestinationRequiresProcessAuthorityAndCannotBeConfusedWithDeletedExistingTarget() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var member = p.command().intent().getMembers(0);
            var command = new DocumentPublicationCommand(p.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                    .setMembers(0, member.toBuilder().setDestination(member.getDestination().toBuilder().setIfAbsent(true)
                            .setAddress(member.getDestination().getAddress().toBuilder().setDocId("new-document")))).build());
            var owner = new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                    command, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
            var decisions = new DocumentPublicationRejections(c.tx());
            assertThatThrownBy(() -> decisions.cancel(new RepositoryCaller("principal", false, Set.of("other"), Set.of()),
                    owner, command, RepositoryReadControl.NONE)).isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            var receipt = decisions.cancel(CALLER, owner, command, RepositoryReadControl.NONE).rejection().orElseThrow();
            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, command).rejection()).contains(receipt);
            assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx()).observe(
                    new RepositoryCaller("principal", false, Set.of("account"), Set.of()), command))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            decisions.cancel(CALLER, p.owner(), p.command(), RepositoryReadControl.NONE);
            c.tx().inTransaction(em -> { em.createNativeQuery("DELETE FROM documents").executeUpdate(); });
            assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx()).observe(CALLER, p.command()))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        }
    }
}

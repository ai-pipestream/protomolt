package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.*;
import ai.protomolt.proto.repo.v1.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real activation transactions, current authorization and JDBC commit faults. */
@Testcontainers
class RepositorySuccessorActivationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(ints = {0, 95, 96})
    void exactRetryKeepsLeasesAndRejectsChangedPlan(int previousVersion) {
        try (var c = previousVersion > 0 ? context(POSTGRES, Integer.toString(previousVersion)) : context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            var before = leases(c, plan);
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            if (previousVersion > 0) {
                var schema = c.pool().getSchema();
                org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                        .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
                boolean validated = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                        SELECT convalidated FROM pg_constraint WHERE conrelid='repository_successor_installs'::regclass
                        AND conname='repository_successor_install_reservation'
                        """).getSingleResult());
                assertThat(validated).isTrue();
                boolean open = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                        SELECT repository_successor_execution_open(:a,:p,:o)
                        """).setParameter("a", plan.next().key().account()).setParameter("p", plan.next().key().principal())
                        .setParameter("o", plan.next().key().operationId()).getSingleResult());
                assertThat(open).isTrue();
            }
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
            assertThat(leases(c, plan)).containsExactly(before);
            assertThat(count(c)).isEqualTo(1);
            var other = RepositorySuccessorInstall.prepare(plan.reservation(), plan.previous(), LEASE, MODES);
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, other, NONE))
                    .hasMessageContaining("differs from committed proposal");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void requiresProcessCoordinatorAndDoesNotBorrowItsAclBypass() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, scoped, scoped, plan, NONE))
                    .isInstanceOf(RepositoryException.class).hasMessageContaining("private process authority");
            // Fixture documents have no ACL grants. The coordinator is an administrator,
            // but its authority must not replace the separately supplied execution caller.
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, scoped, plan, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThat(count(c)).isZero();
            assertThat(budget.reservedBytes()).isZero();
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
        }
    }

    @Test void noInstallCannotActivate() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c); var budget = new PayloadBudget(64_000_000);
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE))
                    .hasMessageContaining("install is not committed");
            assertThat(count(c)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void lostCommitReplyConfirmsExactGrantWithoutRenewal() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var before = leases(c, plan); var budget = new PayloadBudget(64_000_000);
            var armed = new AtomicBoolean(true);
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c)==1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("activation reply lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                RepositorySuccessorExecution.activate(new Tx(emf), budget, CALLER, CALLER, plan, NONE);
            }
            assertThat(armed).isFalse();
            assertThat(count(c)).isEqualTo(1);
            assertThat(leases(c, plan)).containsExactly(before);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(ints = {11, 12})
    void cancellationBeforeAndAfterCommitPreservesOutcome(int checkpoint) {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000); var checks = new AtomicInteger();
            var cancelled = new RepositoryException(RepositoryException.Code.CANCELLED, "cancel activation");
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return false; }
                public long remainingNanos() { return Long.MAX_VALUE; }
                public void check() { if (checks.incrementAndGet()>=checkpoint) throw cancelled; }
            };
            assertThat(catchThrowable(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, control)))
                    .isSameAs(cancelled);
            long committed = checkpoint == 12 ? 1 : 0;
            assertThat(count(c)).isEqualTo(committed);
            long bindings = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_coordinator_bindings WHERE claim_epoch=2").getSingleResult()).longValue());
            assertThat(bindings).isEqualTo(committed);
            assertThat(budget.reservedBytes()).isZero();
            RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
        }
    }

    @Test void competingExactActivationsConvergeWithoutRenewingLeases() throws Exception {
        try (var c = context(POSTGRES); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var plan = installed(c); var before = leases(c, plan); var budget = new PayloadBudget(128_000_000);
            var start = new java.util.concurrent.CountDownLatch(1);
            var tasks = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 2; i++) tasks.add(workers.submit(() -> {
                start.await();
                RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE);
                return null;
            }));
            start.countDown();
            for (var task : tasks) task.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(count(c)).isEqualTo(1);
            assertThat(leases(c, plan)).containsExactly(before);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void changedDocumentRevisionRollsBackActivation() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(64_000_000);
            var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(
                    plan.next().command().intent().getMembers(0).getDestination().getAddress());
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET filename='changed' WHERE node_id=:n")
                    .setParameter("n", node).executeUpdate(); });
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE))
                    .isInstanceOf(DocumentLedger.RevisionConflictException.class);
            assertThat(count(c)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void scopedExecutionUsesCurrentPolicyAndRetryIsOnlyACommittedFact(boolean revokeBeforeActivation) {
        try (var c = context(POSTGRES)) {
            var input = input(c);
            var security = DocumentSecurity.newBuilder();
            for (var access : List.of(Access.ACCESS_READ, Access.ACCESS_WRITE)) security.addPermissions(AccessRule.newBuilder()
                    .setIdentityType("public").setIdentity("public").setAccess(access));
            var address = input.command().intent().getMembers(0).getDestination().getAddress();
            var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address);
            setPolicy(c, node, com.google.protobuf.util.JsonFormat.printer().print(security.build()));
            var revision = new DocumentLedger(c.tx()).findByNodeId(node).orElseThrow().mutationRevision;
            var condition = DocumentRevisionCondition.newBuilder().setAddress(address).setExpectedMutationRevision(revision).build();
            var member = input.command().intent().getMembers(0).toBuilder().setDestination(condition);
            member.setOwnership(member.getOwnership().toBuilder().setSecurity(security));
            for (int i = 0; i < member.getPartsCount(); i++) if (member.getParts(i).hasReuse())
                member.setParts(i, member.getParts(i).toBuilder().setReuse(member.getParts(i).getReuse().toBuilder().setSource(condition)));
            var command = new DocumentPublicationCommand(input.command().intent().toBuilder().setMembers(0, member).build());
            var prepared = new DocumentPublicationPreparationRecord(input.key(), command,
                    DocumentPublicationSeeds.mint(input.key(), command), input.placements(), LEASE, 0);
            var plan = RepositorySuccessorInstallIT.plan(c, prepared, LEASE); var budget = new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            if (!revokeBeforeActivation) RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, scoped, plan, NONE);
            setPolicy(c, node, "{}");
            if (revokeBeforeActivation) {
                assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, scoped, plan, NONE))
                        .isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                assertThat(count(c)).isZero();
            } else {
                // Readback confirms the past commit. Current authorization still denies execution.
                RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, scoped, plan, NONE);
                var uploadPlan = plan.next().prepare().plan();
                var authorization = DocumentAdmissionAuthorization.prepare(uploadPlan, uploadPlan.historical());
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    DocumentAdmissionAuthorization.lockAndAuthorize(em, scoped, uploadPlan, authorization);
                })).isInstanceOfSatisfying(RepositoryException.class, e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                assertThat(count(c)).isEqualTo(1);
            }
            assertThat(budget.reservedBytes()).isZero();
        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new AssertionError(invalid);
        }
    }

    @Test void insufficientBudgetDoesNotCreateExecutionIdentity() {
        try (var c = context(POSTGRES)) {
            var plan = installed(c); var budget = new PayloadBudget(1);
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), budget, CALLER, CALLER, plan, NONE))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(count(c)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static void setPolicy(Context c, UUID node, String policy) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:n")
                .setParameter("n", node).setParameter("policy", policy).executeUpdate(); });
    }

    private static RepositorySuccessorInstall.Plan installed(Context c) {
        var plan = RepositorySuccessorInstallIT.plan(c);
        RepositorySuccessorInstall.install(c.tx(), new PayloadBudget(64_000_000), CALLER, plan, NONE);
        return plan;
    }
    private static long count(Context c) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_successor_executions")
                .getSingleResult()).longValue());
    }
    private static Object[] leases(Context c, RepositorySuccessorInstall.Plan plan) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.lease_until,w.lease_until FROM repository_execution_claims c
                JOIN repository_operation_owners w USING(account_id,principal,operation_id) WHERE c.operation_id=:o
                """).setParameter("o", plan.next().key().operationId()).getSingleResult());
    }
}

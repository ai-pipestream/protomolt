package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.count;
import static org.assertj.core.api.Assertions.*;

/** Fault injection around actual SQL commit; no terminal exception classification. */
@Testcontainers
class RepositoryHistoricalLimitDecisionsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void cancellationAtCommitBoundaryPreservesDurableDecision(boolean afterCommit) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c,rig);
            var plan = installedHistoricalSuccessor(c,rig,Duration.ofMinutes(5));
            var before = RepositoryHistoricalSuccessorActivationIT.leases(c,rig);
            var cancelled = new AtomicBoolean(true);
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return cancelled.get(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var clean = new RepositoryHistoricalLimitDecisions(c.tx(),rig.budget());
            assertThatThrownBy(() -> clean.decide(CALLER,CALLER,plan,rig.record(),control))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            assertThat(count(c,"repository_recovery_limit_decisions")).isZero();
            assertThat(count(c,"repository_operation_rejection")).isZero();
            cancelled.set(false);
            var datasource = afterCommit ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c,"repository_recovery_limit_decisions")==1) cancelled.set(true);
            }) : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement=connection.createStatement(); var rows=statement.executeQuery(
                        "SELECT count(*) FROM repository_recovery_limit_decisions")) {
                    rows.next();
                    if (rows.getInt(1)==1) {
                        cancelled.set(true);
                        // Inject cancellation at the actual JDBC commit boundary, while both rows are tentative.
                        control.check();
                    }
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"))) {
                var gated = new RepositoryHistoricalLimitDecisions(new Tx(emf),rig.budget());
                if (afterCommit) {
                    // A late cancellation must not turn a successful commit into an apparent rollback.
                    var receipt = gated.decide(CALLER,CALLER,plan,rig.record(),control).orElseThrow();
                    assertThat(clean.decide(CALLER,CALLER,plan,rig.record(),NONE)).contains(receipt);
                } else {
                    var failure = catchThrowable(() -> gated.decide(CALLER,CALLER,plan,rig.record(),control));
                    assertThat(failure).isNotNull();
                    while (failure != null && !(failure instanceof RepositoryException)) failure = failure.getCause();
                    assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                }
                assertThat(cancelled).isTrue();
                assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(afterCommit ? 1 : 0);
                assertThat(count(c,"repository_operation_rejection")).isEqualTo(afterCommit ? 1 : 0);
                assertThatThrownBy(() -> clean.decide(CALLER,CALLER,plan,rig.record(),control))
                        .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                var receipt = clean.decide(CALLER,CALLER,plan,rig.record(),NONE).orElseThrow();
                assertThat(clean.decide(CALLER,CALLER,plan,rig.record(),NONE)).contains(receipt);
                assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(1);
                assertThat(count(c,"repository_operation_rejection")).isEqualTo(1);
                assertThat(count(c,"repository_successor_executions")).isZero();
                assertThat(count(c,"repository_preparation_capture_drains")).isZero();
                assertThat(RepositoryHistoricalSuccessorActivationIT.leases(c,rig)).containsExactly(before);
                assertThat(rig.budget().reservedBytes()).isZero();
            }
        }
    }

    @Test void scopedReplayRechecksReadPolicyAndCredentialAfterCommit() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c, rig);
            var binding = new RepositoryCredentialBinding("limit-decision-test",java.util.UUID.randomUUID(),1);
            var caller = new RepositoryCaller("principal",false,Set.of("account"),Set.of(),java.util.Optional.of(binding));
            var authorities = new RepositoryCredentialAuthorities(c.tx());
            authorities.register(CALLER,binding,"principal");
            var publicRead = ai.protomolt.proto.repo.v1.DocumentSecurity.newBuilder()
                    .addPermissions(ai.protomolt.proto.repo.v1.AccessRule.newBuilder().setIdentityType("public").setIdentity("public")
                            .setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ)).build();
            var policy = com.google.protobuf.util.JsonFormat.printer().print(publicRead);
            java.util.function.Consumer<String> setPolicy = value -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                        .setParameter("policy",value).setParameter("node",ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(rig.fixture().address()))
                        .executeUpdate();
            });
            setPolicy.accept(policy);
            var plan = installedHistoricalSuccessor(c,rig,Duration.ofMinutes(5));
            var decisions = new RepositoryHistoricalLimitDecisions(c.tx(),rig.budget());
            var receipt = decisions.decide(CALLER,caller,plan,rig.record(),NONE).orElseThrow();
            setPolicy.accept("{}");
            assertThatThrownBy(() -> decisions.decide(CALLER,caller,plan,rig.record(),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            setPolicy.accept(policy);
            assertThat(decisions.decide(CALLER,caller,plan,rig.record(),NONE)).contains(receipt);
            authorities.revoke(CALLER,binding,"principal");
            assertThatThrownBy(() -> decisions.decide(CALLER,caller,plan,rig.record(),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
            assertThat(count(c,"repository_operation_rejection")).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void rollbackAndLostReplyReconcileOnlyExactDurableDecision(boolean afterCommit) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c, rig);
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofMinutes(5));
            var leases = RepositoryHistoricalSuccessorActivationIT.leases(c,rig);
            var decisions = new RepositoryHistoricalLimitDecisions(c.tx(), rig.budget());
            assertThatThrownBy(() -> decisions.decide(new RepositoryCaller("principal", false, Set.of("account"), Set.of()),
                    CALLER, plan, rig.record(), NONE)).hasMessageContaining("private process authority");
            assertThatThrownBy(() -> decisions.decide(CALLER,new RepositoryCaller("other",true),plan,rig.record(),NONE))
                    .hasMessageContaining("principal differs");
            var armed = new AtomicBoolean(true);
            var datasource = afterCommit ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c,"repository_recovery_limit_decisions")==1 && armed.compareAndSet(true,false))
                    throw new java.sql.SQLException("limit decision reply lost","08006");
            }) : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement=connection.createStatement(); var rows=statement.executeQuery("SELECT count(*) FROM repository_recovery_limit_decisions")) {
                    rows.next();
                    if (rows.getInt(1)==1 && armed.compareAndSet(true,false))
                        throw new java.sql.SQLException("limit decision commit refused","40001");
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"))) {
                var faulted = new RepositoryHistoricalLimitDecisions(new Tx(emf),rig.budget());
                assertThatThrownBy(() -> faulted.decide(CALLER,CALLER,plan,rig.record(),NONE))
                        .hasStackTraceContaining(afterCommit ? "limit decision reply lost" : "limit decision commit refused");
                assertThat(armed).isFalse();
                assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(afterCommit ? 1 : 0);
                assertThat(count(c,"repository_operation_rejection")).isEqualTo(afterCommit ? 1 : 0);
                assertThat(RepositoryHistoricalSuccessorActivationIT.leases(c,rig)).containsExactly(leases);
                var receipt = decisions.decide(CALLER,CALLER,plan,rig.record(),NONE).orElseThrow();
                assertThat(receipt.getReasonValue()).isEqualTo(4);
                assertThat(decisions.decide(CALLER,CALLER,plan,rig.record(),NONE)).contains(receipt);
                var wrongRetention = new DocumentPublicationPreparationRecord(rig.record().key(),rig.record().command(),
                        rig.record().seeds(),rig.record().placements(),Duration.ofSeconds(11),rig.record().predecessorGeneration());
                assertThatThrownBy(() -> decisions.decide(CALLER,CALLER,plan,wrongRetention,NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                assertThat(count(c,"repository_recovery_limit_decisions")).isEqualTo(1);
                assertThat(count(c,"repository_operation_rejection")).isEqualTo(1);
                assertThat(count(c,"repository_successor_executions")).isZero();
                assertThat(count(c,"repository_preparation_capture_drains")).isZero();
                assertThat(rig.budget().reservedBytes()).isZero();
                assertThat(RepositoryHistoricalSuccessorActivationIT.leases(c,rig)).containsExactly(leases);
            }
        }
    }
}

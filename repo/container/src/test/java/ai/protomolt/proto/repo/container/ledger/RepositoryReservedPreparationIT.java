package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Non-executing reserved preparation reads against real PostgreSQL. */
@Testcontainers
class RepositoryReservedPreparationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5));
    private static RepositoryCoordinatorReservation.OwnerIdentity owner(RepositorySuccessorInstall.Plan plan) {
        return new RepositoryCoordinatorReservation.OwnerIdentity(plan.previous().predecessorGeneration()+1,plan.previous().seeds().ownerNonce());
    }
    private static RepositorySuccessorInstall.Plan plan(Context c, boolean graceful) {
        if (graceful) return RepositorySuccessorInstallIT.plan(c);
        var original = input(c); var previous = new DocumentPublicationPreparationRecord(original.key(),original.command(),original.seeds(),
                original.placements(),Duration.ofSeconds(1),0);
        var budget = new PayloadBudget(64_000_000); var incarnation = UUID.randomUUID();
        var claim = new DocumentPublicationPreparationJournal(c.tx(),budget).acquireInitial(CALLER,previous,UUID.randomUUID(),incarnation,NONE);
        new DocumentPublicationModesJournal(c.tx(),budget).bind(CALLER,claim,0,MODES,NONE);
        var admitted = new RepositoryOperationLedger(c.tx()).admit(previous.key(),previous.command(),previous.seeds().ownerNonce(),Duration.ofSeconds(1),claim).owner().orElseThrow();
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
        var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(new RepositoryCoordinatorDrain.Identity(
                claim.key(),claim.commandSha256(),1,claim.token(),incarnation),UUID.randomUUID(),UUID.randomUUID(),LEASE,
                new RepositoryCoordinatorReservation.OwnerIdentity(admitted.generation(),admitted.token()));
        RepositoryCoordinatorExpiration.reserve(c.tx(),CALLER,proposal,NONE);
        return RepositorySuccessorInstall.prepare(proposal,previous,LEASE,MODES);
    }
    private static Object[] state(Context c,RepositorySuccessorInstall.Plan plan) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,CAST(c.write_fence_xid AS text),o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",plan.previous().key().operationId()).getSingleResult());
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void readsBothReservationKindsWithoutStampingOrRenewing(boolean graceful) {
        try (var c = context(POSTGRES)) {
            var plan = plan(c,graceful); var budget = new PayloadBudget(64_000_000); var before = state(c,plan);
            if (graceful) {
                var claim = new RepositoryExecutionClaimLedger.Claim(plan.previous().key(),plan.previous().command().sha256(),2,
                        plan.reservation().successorToken(),(java.time.Instant) before[2]);
                assertThatThrownBy(() -> new DocumentPublicationPreparationJournal(c.tx(),budget).load(CALLER,claim,0,NONE))
                        .hasStackTraceContaining("locally drained");
            }
            var loader = new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS);
            try (var loaded = loader.load(CALLER,CALLER,plan.reservation(),owner(plan),NONE)) {
                assertThat(DocumentPublicationPreparationCodec.encode(loaded.record())).isEqualTo(DocumentPublicationPreparationCodec.encode(plan.previous()));
                assertThat(budget.reservedBytes()).isPositive();
                assertThat(state(c,plan)).containsExactly(before);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery("SELECT require_repository_execution_claim(:a,:p,:o)")
                        .setParameter("a",plan.previous().key().account()).setParameter("p","principal").setParameter("o",plan.previous().key().operationId()).getSingleResult(); }))
                        .hasStackTraceContaining(graceful ? "locally drained" : "successor requires exact activation");
                var fresh = RepositorySuccessorInstall.prepare(plan.reservation(),loaded.record(),LEASE,MODES);
                RepositorySuccessorInstall.install(c.tx(),budget,CALLER,fresh,NONE);
                assertThatThrownBy(() -> loader.load(CALLER,CALLER,plan.reservation(),owner(plan),NONE))
                        .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void rejectsPrivateAuthorityOwnerKindAndInsufficientBudget() {
        try (var c = context(POSTGRES)) {
            var plan = plan(c,true); var budget = new PayloadBudget(64_000_000); var loader = new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS);
            var scoped = new RepositoryCaller("principal",false,Set.of("account"),Set.of());
            assertThatThrownBy(() -> loader.load(scoped,CALLER,plan.reservation(),owner(plan),NONE)).hasMessageContaining("private process authority");
            assertThatThrownBy(() -> loader.load(CALLER,new RepositoryCaller("other",true),plan.reservation(),owner(plan),NONE))
                    .hasMessageContaining("principal differs");
            assertThatThrownBy(() -> loader.load(CALLER,CALLER,plan.reservation(),new RepositoryCoordinatorReservation.OwnerIdentity(1,UUID.randomUUID()),NONE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            var r = plan.reservation();
            var wrong = new RepositoryCoordinatorReservation.ExpiredUnquiesced(r.predecessor(),r.successorToken(),r.successorIncarnation(),r.lease(),owner(plan));
            assertThatThrownBy(() -> loader.load(CALLER,CALLER,wrong,owner(plan),NONE)).hasMessageContaining("Reservation differs");
            var tiny = new PayloadBudget(1);
            assertThatThrownBy(() -> new RepositoryReservedPreparation(c.tx(),tiny,TIMEOUTS).load(CALLER,CALLER,r,owner(plan),NONE))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(tiny.reservedBytes()).isZero(); assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void currentExecutionCallerPolicyIsRecheckedAfterDecode() throws Exception {
        try (var c = context(POSTGRES)) {
            var plan = plan(c,true); var budget = new PayloadBudget(64_000_000);
            var node = ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(plan.previous().command().intent().getMembers(0).getDestination().getAddress());
            var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ)).build();
            policy(c,node,com.google.protobuf.util.JsonFormat.printer().print(security));
            var scoped = new RepositoryCaller("principal",false,Set.of("account"),Set.of());
            try (var loaded = new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS).load(CALLER,scoped,plan.reservation(),owner(plan),NONE)) {
                assertThat(loaded.record().key()).isEqualTo(plan.previous().key());
            }
            var once = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(),() -> { if (once.compareAndSet(false,true)) policy(c,node,"{}"); });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                assertThatThrownBy(() -> new RepositoryReservedPreparation(new Tx(emf),budget,TIMEOUTS).load(CALLER,scoped,plan.reservation(),owner(plan),NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            }
            assertThat(once).isTrue(); assertThat(budget.reservedBytes()).isZero();
        }
    }
    private static void policy(Context c,UUID node,String json) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("policy",json).setParameter("node",node).executeUpdate(); });
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void installationOrCancellationBetweenReadAndDeliveryReleasesBudget(boolean cancel) {
        try (var c = context(POSTGRES)) {
            var plan = plan(c,true); var budget = new PayloadBudget(64_000_000); var once = new AtomicBoolean(); var cancelled = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (once.compareAndSet(false,true)) {
                    if (cancel) cancelled.set(true);
                    else RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,NONE);
                }
            });
            var control = new RepositoryReadControl() { public boolean isCancelled() { return cancelled.get(); } public long remainingNanos() { return Long.MAX_VALUE; } };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                var failure = catchThrowable(() -> new RepositoryReservedPreparation(new Tx(emf),budget,TIMEOUTS)
                        .load(CALLER,CALLER,plan.reservation(),owner(plan),control));
                if (cancel) assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                else assertThat(failure).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            }
            assertThat(once).isTrue(); assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void expiredReservationDoesNotAuthorizePreparationRead() {
        try (var c = context(POSTGRES)) {
            var plan = RepositorySuccessorInstallIT.plan(c,Duration.ofSeconds(1)); var budget = new PayloadBudget(64_000_000);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            assertThatThrownBy(() -> new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS).load(CALLER,CALLER,plan.reservation(),owner(plan),NONE))
                    .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void unjournaledReservationReportsMissingPreparationWithoutReservingBytes() {
        try (var c = context(POSTGRES)) {
            var input = input(c); var incarnation = UUID.randomUUID();
            var claim = c.tx().inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireInitialInTransaction(em,input.key(),input.command(),UUID.randomUUID(),Duration.ofSeconds(1));
                RepositoryCoordinatorBinding.bindInitial(em,acquired,incarnation); return acquired.claim();
            });
            var admitted = new RepositoryOperationLedger(c.tx()).admit(input.key(),input.command(),input.seeds().ownerNonce(),Duration.ofSeconds(1),claim).owner().orElseThrow();
            var identity = new RepositoryCoordinatorDrain.Identity(claim.key(),claim.commandSha256(),1,claim.token(),incarnation);
            RepositoryCoordinatorDrain.begin(c.tx(),CALLER,claim,incarnation,NONE);
            RepositoryCoordinatorLocalDrain.record(c.tx(),CALLER,identity,NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var handoff = new RepositoryCoordinatorHandoff.Proposal(identity,UUID.randomUUID(),UUID.randomUUID(),LEASE);
            RepositoryCoordinatorHandoff.reserve(c.tx(),CALLER,handoff,NONE);
            var budget = new PayloadBudget(64_000_000);
            assertThatThrownBy(() -> new RepositoryReservedPreparation(c.tx(),budget,TIMEOUTS).load(CALLER,CALLER,
                    new RepositoryCoordinatorReservation.Graceful(handoff),
                    new RepositoryCoordinatorReservation.OwnerIdentity(admitted.generation(),admitted.token()),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION))
                    .hasMessageContaining("preparation is absent");
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}

package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL ancestry; source publications use the existing synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalRetentionLoaderIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10));

    @Test void loadsOriginalAcrossTwoSuccessorsWithoutCreatingExecution() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            activateAndRelease(c, rig, second);
            var reservation = reserve(c, rig);
            var third = RepositorySuccessorInstall.prepare(reservation, second.next(), Duration.ofSeconds(1), second.modes());
            RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, third, NONE);
            activateAndRelease(c, rig, third);
            var fourth = reserve(c, rig);
            var budget = new PayloadBudget(64_000_000);
            var before = effects(c);
            RepositoryHistoricalRetentionLoader.Loaded held;
            try (var loaded = loader(c, budget).load(CALLER, CALLER, fourth, owner(third.next()), third.next(), NONE)) {
                held = loaded;
                assertThat(DocumentPublicationPreparationCodec.encode(loaded.record()))
                        .isEqualTo(DocumentPublicationPreparationCodec.encode(rig.record()));
                assertThat(loaded.record().predecessorGeneration()).isNotEqualTo(third.next().predecessorGeneration());
                assertThat(budget.reservedBytes()).isPositive();
                assertThat(effects(c)).containsExactly(before);
            }
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(held::record).hasMessageContaining("closed");
        }
    }

    @ParameterizedTest @ValueSource(strings = {"edge", "digest", "bytes", "capture", "capture-owner", "capture-pin", "capture-epoch"})
    void rejectsDamagedAncestryAndReleasesMemory(String damage) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            activateAndRelease(c, rig, second);
            var reservation = reserve(c, rig);
            c.tx().inTransaction(em -> {
                // Deliberate on-disk corruption; ordinary SQL guards prohibit these writes.
                em.createNativeQuery("SET LOCAL session_replication_role = replica").executeUpdate();
                if (damage.equals("bytes")) em.createNativeQuery(
                        "ALTER TABLE repository_publication_preparations DROP CONSTRAINT repository_preparation_digest").executeUpdate();
                String sql = switch (damage) {
                    case "edge" -> "DELETE FROM repository_successor_installs";
                    case "digest" -> "UPDATE repository_successor_installs SET predecessor_preparation_sha256=decode(repeat('00',32),'hex')";
                    case "bytes" -> "UPDATE repository_publication_preparations SET preparation_bytes=decode('00','hex') WHERE predecessor_generation=0";
                    case "capture-owner" -> "UPDATE repository_preparation_pin_owners SET incarnation=gen_random_uuid() WHERE predecessor_generation=0";
                    case "capture-pin" -> "UPDATE repository_preparation_source_pins SET publication_revision=publication_revision+1 WHERE predecessor_generation=0";
                    case "capture-epoch" -> "UPDATE repository_preparation_pin_owners o SET claim_epoch=i.claim_epoch,claim_token=i.claim_token,incarnation=i.incarnation FROM repository_coordinator_bindings i WHERE o.account_id=i.account_id AND o.principal=i.principal AND o.operation_id=i.operation_id AND o.predecessor_generation=0 AND i.claim_epoch=2";
                    default -> "DELETE FROM repository_preparation_pin_owners WHERE predecessor_generation=0";
                };
                em.createNativeQuery(sql).executeUpdate();
                return null;
            });
            var budget = new PayloadBudget(64_000_000);
            assertThatThrownBy(() -> loader(c, budget).load(CALLER, CALLER, reservation, owner(second.next()), second.next(), NONE))
                    .isInstanceOf(RepositoryException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void rejectsWrongCallerAndInsufficientMemory() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            activateAndRelease(c, rig, second);
            var reservation = reserve(c, rig);
            var budget = new PayloadBudget(64_000_000);
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> loader(c, budget).load(scoped, CALLER, reservation, owner(second.next()), second.next(), NONE))
                    .hasMessageContaining("private process authority");
            assertThatThrownBy(() -> loader(c, budget).load(CALLER, new RepositoryCaller("other", true), reservation,
                    owner(second.next()), second.next(), NONE)).hasMessageContaining("principal differs");
            var tiny = new PayloadBudget(1);
            assertThatThrownBy(() -> loader(c, tiny).load(CALLER, CALLER, reservation, owner(second.next()), second.next(), NONE))
                    .isInstanceOf(RuntimeException.class);
            assertThat(tiny.reservedBytes()).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static RepositoryHistoricalRetentionLoader loader(Context c, PayloadBudget budget) {
        return new RepositoryHistoricalRetentionLoader(c.tx(), budget, TIMEOUTS);
    }

    @ParameterizedTest @ValueSource(strings = {"acl", "credential", "cancel", "install"})
    void rechecksChangesBetweenReadAndDelivery(String change) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            activateAndRelease(c, rig, second);
            var reservation = reserve(c, rig);
            var budget = new PayloadBudget(64_000_000);
            var binding = new RepositoryCredentialBinding("retention-test", UUID.randomUUID(), 1);
            var credentials = new RepositoryCredentialAuthorities(c.tx());
            credentials.register(CALLER, binding, CALLER.principalName());
            var scoped = new RepositoryCaller(CALLER.principalName(), false, Set.of("account"), Set.of(), Optional.of(binding));
            var security = ai.protomolt.proto.repo.v1.DocumentSecurity.newBuilder().addPermissions(
                    ai.protomolt.proto.repo.v1.AccessRule.newBuilder().setIdentityType("public").setIdentity("public")
                            .setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ)).build();
            policy(c, rig, com.google.protobuf.util.JsonFormat.printer().print(security));
            try (var loaded = loader(c, budget).load(CALLER, scoped, reservation, owner(second.next()), second.next(), NONE)) {
                assertThat(loaded.record().predecessorGeneration()).isZero();
            }
            var fired = new java.util.concurrent.atomic.AtomicBoolean();
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (!armed.get()) return;
                if (!fired.compareAndSet(false, true)) return;
                switch (change) {
                    case "acl" -> policy(c, rig, "{}");
                    case "credential" -> credentials.revoke(CALLER, binding, CALLER.principalName());
                    case "cancel" -> cancelled.set(true);
                    case "install" -> RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER,
                            RepositorySuccessorInstall.prepare(reservation, second.next(), Duration.ofMinutes(1), second.modes()), NONE);
                    default -> throw new AssertionError(change);
                }
            });
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"))) {
                armed.set(true);
                var failure = catchThrowable(() -> new RepositoryHistoricalRetentionLoader(new Tx(emf), budget, TIMEOUTS)
                        .load(CALLER, scoped, reservation, owner(second.next()), second.next(), control));
                if (change.equals("install")) assertThat(failure).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                else assertThat(failure).isInstanceOfSatisfying(RepositoryException.class, error ->
                        assertThat(error.code()).isEqualTo(switch (change) {
                            case "acl" -> RepositoryException.Code.NOT_FOUND;
                            case "credential" -> RepositoryException.Code.UNAUTHENTICATED;
                            default -> RepositoryException.Code.CANCELLED;
                        }));
            }
            assertThat(fired).isTrue();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static void policy(Context c, Rig rig, String json) {
        c.tx().inTransaction(em -> {
            em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", json).setParameter("node", ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(rig.fixture().address()))
                    .executeUpdate();
            return null;
        });
    }
    private static RepositoryCoordinatorReservation.OwnerIdentity owner(DocumentPublicationPreparationRecord record) {
        return new RepositoryCoordinatorReservation.OwnerIdentity(record.predecessorGeneration() + 1, record.seeds().ownerNonce());
    }
    private static void activateAndRelease(Context c, Rig rig, RepositorySuccessorInstall.Plan plan) throws Exception {
        try (var later = capture(c, rig)) {
            var retained = activation(c.tx(), c, rig, plan, later).activate(CALLER, CALLER, NONE);
            later.sources().close();
            assertThat(retained.complete(CALLER, Duration.ofSeconds(1), NONE)).isPresent();
        }
    }
    private static RepositoryCoordinatorReservation.ExpiredUnquiesced reserve(Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
        var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE).candidate().orElseThrow();
        var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(observed.predecessor(), UUID.randomUUID(),
                UUID.randomUUID(), Duration.ofMinutes(2), observed.owner());
        RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
        return reservation;
    }
    private static long[] effects(Context c) {
        return java.util.stream.Stream.of("repository_execution_claims", "repository_publication_assessment_starts",
                "repository_preparation_pin_batches", "repository_historical_activations", "document_assessment_owners",
                "document_revision_commits", "repository_preparation_capture_drains").mapToLong(table -> RepositoryHistoricalSuccessorActivationIT.count(c, table)).toArray();
    }
}

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

    @Test void loadsInitialAnchorBeforeAnySuccessorInstall() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(5))) {
            var record = rig.record();
            var modes = Map.of(record.command().intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
            try (var work = rig.sources().work()) {
                var admission = RepositoryOperationLedger.prepareHistoricalAdmission(record.key(), record.command(),
                        record.seeds().ownerNonce(), Duration.ofSeconds(1), work);
                c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                    DocumentPublicationModesJournal.insert(em, rig.claim(), record,
                            DocumentPublicationModesJournal.encode(record.command(), modes));
                    admission.apply(em, rig.claim());
                    return null;
                });
            }
            var reservation = reserve(c, rig);
            var budget = new PayloadBudget(64_000_000);
            var before = effects(c);
            try (var loaded = loader(c, budget).load(CALLER, CALLER, reservation, owner(record), record, NONE)) {
                assertThat(DocumentPublicationPreparationCodec.encode(loaded.record()))
                        .isEqualTo(DocumentPublicationPreparationCodec.encode(record));
                assertThat(budget.reservedBytes()).isEqualTo(DocumentPublicationPreparationCodec.encode(record).size());
                try (var available = budget.reserve(budget.capacity() - budget.reservedBytes())) {
                    assertThat(budget.reservedBytes()).isEqualTo(budget.capacity());
                    assertThat(loaded.record()).isNotNull();
                }
            }
            assertThat(effects(c)).containsExactly(before);
            assertThat(RepositoryHistoricalSuccessorActivationIT.count(c, "repository_successor_installs")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void loadsThroughAnInstalledPredecessorThatNeverActivated() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            waitExpired(c, rig);
            var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                    .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
            assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED);
            var source = observed.unactivated().orElseThrow();
            var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),
                    UUID.randomUUID(), UUID.randomUUID(), Duration.ofMinutes(2), source.owner(),
                    source.preparationSha256(), source.installation());
            RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, reservation, NONE);
            var budget = new PayloadBudget(64_000_000);
            var before = effects(c);
            try (var loaded = loader(c, budget).load(CALLER, CALLER, reservation, source.owner(), second.next(), NONE)) {
                assertThat(DocumentPublicationPreparationCodec.encode(loaded.record()))
                        .isEqualTo(DocumentPublicationPreparationCodec.encode(rig.record()));
            }
            assertThat(effects(c)).containsExactly(before);
            assertThat(RepositoryHistoricalSuccessorActivationIT.count(c, "repository_historical_activations")).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void countsPlannedSuccessorInThe64EdgeLimit() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var current = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            var budget = new PayloadBudget(64_000_000);
            var before = effects(c);
            for (int edges = 1; edges <= 64; edges++) {
                waitExpired(c, rig);
                var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                        .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
                assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED);
                var source = observed.unactivated().orElseThrow();
                var lease = edges >= 63 ? Duration.ofSeconds(30) : Duration.ofSeconds(1);
                var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),
                        UUID.randomUUID(), UUID.randomUUID(), lease, source.owner(), source.preparationSha256(), source.installation());
                RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, reservation, NONE);
                if (edges == 63) {
                    try (var loaded = loader(c, budget).load(CALLER, CALLER, reservation, source.owner(), current.next(), NONE)) {
                        assertThat(DocumentPublicationPreparationCodec.encode(loaded.record()))
                                .isEqualTo(DocumentPublicationPreparationCodec.encode(rig.record()));
                    }
                } else if (edges == 64) {
                    var previous = current.next();
                    assertThatThrownBy(() -> loader(c, budget).load(CALLER, CALLER, reservation, source.owner(), previous, NONE))
                            .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                                assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                                assertThat(failure.getMessage()).contains("64-link activation limit");
                            });
                    break;
                }
                current = RepositorySuccessorInstall.prepare(reservation, current.next(), Duration.ofSeconds(1), current.modes());
                RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, current, NONE);
            }
            assertThat(RepositoryHistoricalSuccessorActivationIT.count(c, "repository_successor_installs")).isEqualTo(64);
            assertThat(effects(c)).containsExactly(before);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void loadedMetadataDoesNotKeepReleasedRootsAlive() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c)) {
            var second = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(1));
            activateAndRelease(c, rig, second);
            var reservation = reserve(c, rig);
            var budget = new PayloadBudget(64_000_000);
            try (var loaded = loader(c, budget).load(CALLER, CALLER, reservation, owner(second.next()), second.next(), NONE)) {
                var third = RepositorySuccessorInstall.prepare(reservation, second.next(), Duration.ofMinutes(1), second.modes());
                RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, third, NONE);
                try (var later = capture(c, rig)) {
                    var capture = activation(c.tx(), c, rig, third, later).activate(CALLER, CALLER, NONE);
                    var owner = c.tx().inTransaction(em -> {
                        var claim = RepositoryExecutionClaimLedger.lockLive(em, rig.record().key(), rig.record().command().sha256(),
                                reservation.predecessor().epoch() + 1, reservation.successorToken());
                        return RepositoryOperationLedger.lockLiveOwner(em, rig.record().key(), third.next().predecessorGeneration() + 1,
                                third.next().seeds().ownerNonce(), Optional.of(claim));
                    });
                    assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, rig.record().command(), NONE).rejection()).isPresent();
                    later.sources().close();
                    assertThat(capture.complete(CALLER, Duration.ofSeconds(1), NONE)).isPresent();
                }
                rig.sources().close(); rig.history().close();
                assertThat(rig.history().awaitDrained(Duration.ofSeconds(1))).isTrue();
                rig.history().release();
                rig.reads().fence(); rig.reads().attestLocalQuiescence();
                var pins = c.tx().readOnly(em -> (byte[]) em.createNativeQuery(
                        "SELECT pins_sha256 FROM repository_preparation_pin_batches WHERE operation_id=:o AND initial_capture")
                        .setParameter("o", rig.record().key().operationId()).getSingleResult());
                var initial = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(rig.record().key(),
                        rig.record().command().sha256(), rig.claim().epoch(), rig.claim().token(), rig.coordinator()),
                        0, HexFormat.of().formatHex(pins));
                assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, initial, NONE).kind()).isEqualTo("QUIESCED");
                assertThat(RepositoryHistoricalSuccessorActivationIT.count(c, "repository_preparation_capture_drains")).isEqualTo(3);
                var released = DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, loaded.record(), NONE);
                assertThat(released.captureCount()).isEqualTo(3);
                assertThat(RepositoryHistoricalSuccessorActivationIT.count(c, "repository_preparation_history_roots")).isZero();
                assertThat(budget.reservedBytes()).isPositive();
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    DocumentHistoricalRetentionBinding.require(em, loaded.record(),
                            DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(loaded.record())), true);
                    return null;
                })).isInstanceOfSatisfying(RepositoryException.class, failure ->
                        assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                var retryBudget = new PayloadBudget(64_000_000);
                assertThatThrownBy(() -> loader(c, retryBudget).load(CALLER, CALLER, reservation, owner(second.next()), second.next(), NONE))
                        .isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                assertThat(retryBudget.reservedBytes()).isZero();
            }
            assertThat(budget.reservedBytes()).isZero();
        }
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
        waitExpired(c, rig);
        var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE).candidate().orElseThrow();
        var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(observed.predecessor(), UUID.randomUUID(),
                UUID.randomUUID(), Duration.ofMinutes(2), observed.owner());
        RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
        return reservation;
    }
    private static void waitExpired(Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }
    private static long[] effects(Context c) {
        return java.util.stream.Stream.of("repository_execution_claims", "repository_publication_assessment_starts",
                "repository_preparation_pin_batches", "repository_historical_activations", "document_assessment_owners",
                "document_revision_commits", "repository_preparation_capture_drains").mapToLong(table -> RepositoryHistoricalSuccessorActivationIT.count(c, table)).toArray();
    }
}

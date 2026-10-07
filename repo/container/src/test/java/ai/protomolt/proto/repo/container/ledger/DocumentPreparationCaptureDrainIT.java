package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL, lifecycle and fault injection. Publication provider observations are fixture supplied. */
@Testcontainers
class DocumentPreparationCaptureDrainIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void captureCoverageRequiresCompletionAndDoesNotReleaseRootsOrRenewClaim() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            var check = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
            Object lease = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", rig.command().operationId()).getSingleResult());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return check.lockAndRequireDrained(em, NONE); }))
                    .hasMessageContaining("has not drained");
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
            int batches = c.tx().inTransaction(em -> { return check.lockAndRequireDrained(em, NONE); });
            assertThat(batches).isEqualTo(1);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").executeUpdate();
                return check.lockAndRequireDrained(em, NONE);
            })).hasStackTraceContaining("requires READ COMMITTED isolation");
            Object after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", rig.command().operationId()).getSingleResult());
            assertThat(after).isEqualTo(lease);
            assertThat(count(c, "repository_preparation_history_roots", rig)).isEqualTo(1);
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({
            "pin,Preparation capture coverage differs", "publication,Preparation capture coverage differs",
            "owner,Preparation capture coverage differs", "drain,Preparation capture has not drained",
            "initial,Preparation capture coverage differs", "missing_owner,Preparation capture ownership is unknown",
            "missing_batch,Preparation capture coverage is unknown"})
    void captureCoverageRejectsCorruptOrIncompleteEvidence(String kind, String expected) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
            var check = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
            var mutation = switch (kind) {
                case "pin" -> "UPDATE repository_preparation_source_pins SET pin_id=gen_random_uuid() WHERE operation_id=:o";
                case "publication" -> "UPDATE repository_preparation_source_pins SET publication_revision=publication_revision+1 WHERE operation_id=:o";
                case "owner" -> "UPDATE repository_preparation_capture_drains SET claim_token=gen_random_uuid() WHERE operation_id=:o";
                case "drain" -> "DELETE FROM repository_preparation_capture_drains WHERE operation_id=:o";
                case "initial" -> "UPDATE repository_preparation_pin_batches SET initial_capture=false WHERE operation_id=:o";
                case "missing_owner" -> "DELETE FROM repository_preparation_pin_owners WHERE operation_id=:o";
                case "missing_batch" -> "DELETE FROM repository_preparation_pin_batches WHERE operation_id=:o";
                default -> throw new IllegalArgumentException(kind);
            };
            assertThatThrownBy(() -> c.tx().inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
                // Administrative corruption confined to this rolled-back test transaction.
                for (var entry : Map.of("repository_preparation_source_pins", "repository_preparation_source_pin_guard",
                        "repository_preparation_capture_drains", "repository_preparation_capture_drain_guard",
                        "repository_preparation_pin_owners", "repository_preparation_pin_owner_guard",
                        "repository_preparation_pin_batches", "repository_preparation_pin_batch_guard").entrySet())
                    em.createNativeQuery("ALTER TABLE " + entry.getKey() + " DISABLE TRIGGER " + entry.getValue()).executeUpdate();
                if (kind.startsWith("missing_"))
                    em.createNativeQuery("DELETE FROM repository_preparation_capture_drains WHERE operation_id=:o")
                            .setParameter("o", rig.command().operationId()).executeUpdate();
                if (kind.equals("missing_batch")) {
                    em.createNativeQuery("DELETE FROM repository_preparation_pin_owners WHERE operation_id=:o")
                            .setParameter("o", rig.command().operationId()).executeUpdate();
                    em.createNativeQuery("DELETE FROM repository_preparation_source_pins WHERE operation_id=:o")
                            .setParameter("o", rig.command().operationId()).executeUpdate();
                }
                assertThat(em.createNativeQuery(mutation).setParameter("o", rig.command().operationId()).executeUpdate()).isEqualTo(1);
                assertThatThrownBy(() -> check.lockAndRequireDrained(em, NONE)).hasMessageContaining(expected);
                throw new CoverageRollback();
            })).isInstanceOf(CoverageRollback.class);
            int batches = c.tx().inTransaction(em -> { return check.lockAndRequireDrained(em, NONE); });
            assertThat(batches).isEqualTo(1);
        }
    }

    private static final class CoverageRollback extends RuntimeException {}

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void localCompletionWaitsForActualWorkOrAliasedUseWithoutFencingOtherHandles(boolean alias) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            var unrelated = rig.reads().captureHistorical(CALLER, rig.fixture().address(), rig.fixture().revision());
            try {
                AutoCloseable held = alias
                        ? DocumentHistoricalAssessmentSources.open(rig.command(), CALLER, List.of(rig.history()), NONE)
                        : rig.sources().work();
                try {
                    assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isEmpty();
                    assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).isEmpty();
                    assertThat(rig.history().isDrained()).isFalse();
                } finally { held.close(); }
                var receipt = rig.capture().complete(CALLER, Duration.ofSeconds(1), NONE).orElseThrow();
                assertThat(receipt.kind()).isEqualTo("LOCAL");
                assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).contains(receipt);
                try (var use = unrelated.use()) { assertThat(use.plan().revision()).isEqualTo(rig.fixture().revision()); }
                assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
                assertThat(count(c, "repository_preparation_history_roots", rig)).isEqualTo(1);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(
                        "DELETE FROM repository_preparation_capture_drains WHERE operation_id=:o")
                        .setParameter("o", rig.command().operationId()).executeUpdate(); }))
                        .hasStackTraceContaining("capture drain is immutable");
                var id = rig.capture().identity(); var owner = id.owner();
                var wrong = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(owner.key(),
                        owner.commandSha256(), owner.epoch(), UUID.randomUUID(), owner.incarnation()), id.generation(), id.pinsSha256());
                assertThatThrownBy(() -> DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, wrong, NONE))
                        .hasMessageContaining("differs from original binding");
            } finally { unrelated.close(); unrelated.release(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void actualOriginalCaptureCanDrainAfterExpiryOrTransferWithoutChangingCurrentLease(boolean transfer) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofSeconds(1))) {
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var id = rig.capture().identity();
            // Exercise the guarded SQL epoch transition, not the still-gated public historical successor API.
            if (transfer) c.tx().inTransaction(em -> {
                var token = UUID.randomUUID();
                em.createNativeQuery("""
                        UPDATE repository_execution_claims SET claim_epoch=claim_epoch+1,claim_token=:token,
                        lease_until=clock_timestamp()+interval '5 minutes',fence_epoch=:epoch,fence_token=:token
                        WHERE operation_id=:o
                        """).setParameter("token", token).setParameter("epoch", id.owner().epoch()+1)
                        .setParameter("o", rig.command().operationId()).executeUpdate();
            });
            Object before = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", rig.command().operationId()).getSingleResult());
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
            var coverage = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
            int qualified = c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); });
            assertThat(qualified).isEqualTo(1);
            Object after = c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:o")
                    .setParameter("o", rig.command().operationId()).getSingleResult());
            assertThat(after).isEqualTo(before);
            long epoch = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT claim_epoch FROM repository_preparation_capture_drains WHERE operation_id=:o")
                    .setParameter("o", rig.command().operationId()).getSingleResult()).longValue());
            assertThat(epoch).isEqualTo(id.owner().epoch());
        }
    }

    @Test void recoveryRequiresAbsentPinsAndPermanentQuiescenceNotActiveOrFencedReaders() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            var id = rig.capture().identity();
            assertThatThrownBy(() -> DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, id, NONE))
                    .hasStackTraceContaining("absent native pins and mirrors");
            rig.sources().close(); rig.history().close(); rig.history().release();
            assertThatThrownBy(() -> DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, id, NONE))
                    .hasStackTraceContaining("every original reader quiesced");
            rig.reads().fence();
            assertThatThrownBy(() -> DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, id, NONE))
                    .hasStackTraceContaining("every original reader quiesced");
            rig.reads().attestLocalQuiescence();
            var receipt = DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, id, NONE);
            assertThat(receipt.kind()).isEqualTo("QUIESCED");
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).contains(receipt);
            assertThat(count(c, "repository_preparation_history_roots", rig)).isEqualTo(1);
        }
    }

    @Test void failedActualPinReleaseLeavesNoMarkerAndCanBeRetried() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION test_refuse_pin_release() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'Injected native pin cleanup failure'; END; $$
                        """).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER test_pin_release BEFORE DELETE ON document_read_pins FOR EACH ROW EXECUTE FUNCTION test_refuse_pin_release()")
                        .executeUpdate();
            });
            try {
                assertThatThrownBy(() -> rig.capture().complete(CALLER, Duration.ZERO, NONE))
                        .hasStackTraceContaining("Injected native pin cleanup failure");
                assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).isEmpty();
            } finally {
                c.tx().inTransaction(em -> { em.createNativeQuery("DROP TRIGGER test_pin_release ON document_read_pins").executeUpdate(); });
            }
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void lostMarkerAcknowledgmentConfirmsExactCommittedReceipt(boolean bounded) throws Exception {
        try (var c = context(POSTGRES)) {
            var fault = new AtomicBoolean(true);
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                long markers = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_preparation_capture_drains").getSingleResult()).longValue());
                if (markers > 0 && fault.compareAndSet(true, false)) throw new java.sql.SQLException("Lost capture drain acknowledgement", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var rig = prepare(c, bounded ? new Tx(emf).withTimeouts(new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5)))
                         : new Tx(emf), Duration.ofMinutes(5))) {
                var receipt = rig.capture().complete(CALLER, Duration.ZERO, NONE).orElseThrow();
                assertThat(fault).isFalse();
                assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).contains(receipt);
                assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).contains(receipt);
                assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
            }
        }
    }

    @Test void publicCallerCannotClosePrivateCapture() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertThatThrownBy(() -> rig.capture().complete(caller, Duration.ZERO, NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            try (var work = rig.sources().work()) { assertThat(work.references(rig.command(), () -> {})).isNotEmpty(); }
        }
    }

    @Test void cancellationAfterActualMarkerCommitPreservesCancellationAndLaterExactConfirmation() throws Exception {
        try (var c = context(POSTGRES)) {
            var cancelled = new AtomicBoolean();
            RepositoryReadControl control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return cancelled.get(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                long markers = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_preparation_capture_drains").getSingleResult()).longValue());
                if (markers > 0) cancelled.set(true);
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var rig = prepare(c, new Tx(emf).withTimeouts(new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5))), Duration.ofMinutes(5))) {
                assertThatThrownBy(() -> rig.capture().complete(CALLER, Duration.ZERO, control))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).isPresent();
                assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
                assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
            }
        }
    }

    @Test void concurrentCompletionConfirmsOneImmutableReceipt() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5));
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.Callable<DocumentPreparationCaptureDrain.Receipt> call = () -> {
                start.await(); return rig.capture().complete(CALLER, Duration.ofSeconds(5), NONE).orElseThrow();
            };
            var first = executor.submit(call); var second = executor.submit(call);
            start.countDown();
            assertThat(first.get(15, java.util.concurrent.TimeUnit.SECONDS))
                    .isEqualTo(second.get(15, java.util.concurrent.TimeUnit.SECONDS));
            assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void heldWorkWaitObservesCancellationAndDeadlineWithoutMintingDrain(boolean deadline) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5));
             var held = rig.sources().work(); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var entered = new java.util.concurrent.CountDownLatch(1);
            var cancelled = new AtomicBoolean();
            long end = System.nanoTime() + Duration.ofMillis(250).toNanos();
            RepositoryReadControl control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { entered.countDown(); return cancelled.get(); }
                @Override public long remainingNanos() { return deadline ? end - System.nanoTime() : Long.MAX_VALUE; }
            };
            var future = executor.submit(() -> rig.capture().complete(CALLER, Duration.ofSeconds(10), control));
            assertThat(entered.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            if (!deadline) cancelled.set(true);
            assertThatThrownBy(() -> future.get(3, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RepositoryException.class)
                    .satisfies(failure -> assertThat(((RepositoryException) failure.getCause()).code()).isEqualTo(
                            deadline ? RepositoryException.Code.DEADLINE_EXCEEDED : RepositoryException.Code.CANCELLED));
            assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).isEmpty();
            assertThat(held.references(rig.command(), () -> {})).isNotEmpty();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void tentativeCapabilityDistinguishesRolledBackRegistrationFromLostAcknowledgment(boolean beforeCommit) throws Exception {
        try (var c = context(POSTGRES)) {
            var armed = new AtomicBoolean();
            var injected = new AtomicBoolean();
            var acknowledged = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (!beforeCommit && armed.get() && injected.compareAndSet(false, true))
                    throw new java.sql.SQLException("Injected registration acknowledgement loss", "08006");
            });
            var datasource = DocumentJdbcFaults.beforeCommit(acknowledged, connection -> {
                try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                        "SELECT EXISTS(SELECT 1 FROM repository_preparation_pin_batches WHERE creation_xid=pg_current_xact_id())")) {
                    rows.next();
                    if (!rows.getBoolean(1)) return;
                    armed.set(true);
                    if (beforeCommit && injected.compareAndSet(false, true))
                        throw new java.sql.SQLException("Injected registration commit refusal", "08006");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var rig = prepare(c, new Tx(emf), Duration.ofMinutes(5), true)) {
                assertThat(injected).isTrue();
                if (beforeCommit) {
                    assertThat(rig.capture().identity()).isNotNull();
                    assertThatThrownBy(() -> rig.capture().complete(CALLER, Duration.ZERO, NONE))
                            .hasStackTraceContaining("original command scope");
                    assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
                } else {
                    assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
                }
            }
        }
    }

    @Test void populatedMigrationCreatesNoInventedDrainAndOriginalCaptureCanComplete() throws Exception {
        try (var c = context(POSTGRES, "106"); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").target("107").load().migrate();
            assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
            assertThat(count(c, "repository_preparation_pin_owners", rig)).isEqualTo(1);
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
        }
    }

    @Test void mismatchedOriginalOwnerCannotRecordNewDrain() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            var id = rig.capture().identity(); var owner = id.owner();
            var wrongOwners = List.of(
                    new RepositoryCoordinatorDrain.Identity(owner.key(), owner.commandSha256(), owner.epoch()+1, owner.token(), owner.incarnation()),
                    new RepositoryCoordinatorDrain.Identity(owner.key(), owner.commandSha256(), owner.epoch(), UUID.randomUUID(), owner.incarnation()),
                    new RepositoryCoordinatorDrain.Identity(owner.key(), owner.commandSha256(), owner.epoch(), owner.token(), UUID.randomUUID()));
            for (var wrong : wrongOwners) {
                var changed = new DocumentPreparationCaptureDrain.Identity(wrong, id.generation(), id.pinsSha256());
                assertThatThrownBy(() -> DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, changed, NONE))
                        .hasStackTraceContaining("exact immutable owner");
            }
            assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
        }
    }

    @Test void orphanMirrorFromCorruptCleanupCannotProduceDrainProof() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            // Deliberately corrupt this isolated database: normal release removes both atomically.
            c.tx().inTransaction(em -> { em.createNativeQuery("ALTER TABLE document_read_pins DISABLE TRIGGER document_read_pin_mirror").executeUpdate(); });
            try {
                assertThatThrownBy(() -> rig.capture().complete(CALLER, Duration.ZERO, NONE))
                        .hasStackTraceContaining("absent native pins and mirrors");
                assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).isEmpty();
            } finally {
                c.tx().inTransaction(em -> {
                    em.createNativeQuery("ALTER TABLE document_read_pins ENABLE TRIGGER document_read_pin_mirror").executeUpdate();
                    em.createNativeQuery("""
                            DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_READER'
                            AND owner_id IN (SELECT pin_id FROM repository_preparation_source_pins WHERE operation_id=:o)
                            """).setParameter("o", rig.command().operationId()).executeUpdate();
                });
            }
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
        }
    }

    private record Rig(Fixture fixture, DocumentPublicationCommand command, DocumentPublicationPreparationRecord record, DocumentReadLedger reads,
            DocumentReadLedger.PinnedHistory history, DocumentHistoricalAssessmentSources sources,
            DocumentPreparationCaptureDrain.Capture capture, PayloadBudget budget) implements AutoCloseable {
        @Override public void close() throws Exception {
            sources.close(); release(reads, history); assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static Rig prepare(Context c, Tx registrationTx, Duration lease) throws Exception {
        return prepare(c, registrationTx, lease, false);
    }

    private static Rig prepare(Context c, Tx registrationTx, Duration lease, boolean failedRegistration) throws Exception {
        var original = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
        var fixture = new Fixture(original, DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {})));
        var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
        var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
        var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var placement = original.prepared().members().getFirst().placement();
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), lease, 0);
        var budget = new PayloadBudget(64L * 1024 * 1024);
        var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), NONE);
        boolean delivered = false;
        try {
            var registration = DocumentPublicationRegistration.historical(registrationTx, budget, record, sources,
                    UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), NONE);
            if (failedRegistration) {
                assertThatThrownBy(() -> registration.admitInitial(CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED), NONE))
                        .hasStackTraceContaining("Injected registration");
            } else assertThat(registration.admitInitial(CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED), NONE)).isPresent();
            var result = new Rig(fixture, command, record, reads, history, sources, registration.historicalCapture().orElseThrow(), budget);
            delivered = true; return result;
        } finally { if (!delivered) { sources.close(); release(reads, history); } }
    }

    private static long count(Context c, String table, Rig rig) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:o")
                .setParameter("o", rig.command().operationId()).getSingleResult()).longValue());
    }
}

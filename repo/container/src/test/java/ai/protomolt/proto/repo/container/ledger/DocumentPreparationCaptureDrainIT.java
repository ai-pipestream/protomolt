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
                assertThat(initialState(c, rig, CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED)))
                        .isEqualTo(beforeCommit ? RepositoryInitialHistoricalCaptureState.State.ABSENT
                                : RepositoryInitialHistoricalCaptureState.State.REGISTERED);
                // prepare has joined the failed registration call. No commit can still be
                // in flight when these assertions distinguish rollback from a lost reply.
                var unrelated = rig.reads().captureHistorical(CALLER, rig.fixture().address(), rig.fixture().revision());
                try {
                    try (var held = rig.sources().work()) {
                        if (beforeCommit) {
                            assertThat(rig.capture().releaseLocal(Duration.ZERO, NONE)).isFalse();
                        } else {
                            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isEmpty();
                        }
                        assertThat(rig.history().isReleased()).isFalse();
                        assertThat(held.references(rig.command(), () -> {})).isNotEmpty();
                        assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
                    }
                    if (beforeCommit) {
                        assertThat(rig.capture().identity()).isNotNull();
                        assertThat(rig.capture().releaseLocal(Duration.ofSeconds(1), NONE)).isTrue();
                        assertThat(rig.capture().releaseLocal(Duration.ZERO, NONE)).isTrue();
                        for (var table : List.of("repository_execution_claims", "repository_coordinator_bindings",
                                "repository_publication_preparations", "repository_preparation_history_sets",
                                "repository_preparation_pin_batches", "repository_preparation_pin_owners",
                                "repository_preparation_source_pins")) {
                            assertThat(count(c, table, rig)).as(table).isZero();
                        }
                        assertThatThrownBy(() -> rig.capture().complete(CALLER, Duration.ZERO, NONE))
                                .hasStackTraceContaining("original command scope");
                        assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
                        assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).isEmpty();
                        verifyDifferentInitialWinner(c, rig);
                    } else {
                        var receipt = rig.capture().complete(CALLER, Duration.ZERO, NONE).orElseThrow();
                        assertThat(receipt.kind()).isEqualTo("LOCAL");
                        assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rig.capture().identity(), NONE)).contains(receipt);
                        assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
                    }
                    assertThat(rig.history().isReleased()).isTrue();
                    try (var use = unrelated.use()) {
                        assertThat(use.plan().revision()).isEqualTo(rig.fixture().revision());
                    }
                } finally {
                    unrelated.close();
                    assertThat(unrelated.awaitDrained(Duration.ofSeconds(1))).isTrue();
                    unrelated.release();
                }
            }
        }
    }

    @Test void populatedMigrationCreatesNoInventedDrainAndOriginalCaptureCanComplete() throws Exception {
        try (var c = DocumentPreparationRootReleaseIT.legacyReaderContext(POSTGRES, "106").context();
             var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, Duration.ofMinutes(5))) {
            DocumentPreparationCaptureDrain.Capture capture;
            try (var work = rig.sources().work()) {
                var pins = DocumentPreparationSourcePins.prepare(rig.record().command(),
                        work.references(rig.record().command(), () -> {}), () -> {});
                var record = rig.record();
                var admission = RepositoryOperationLedger.prepareHistoricalAdmission(record.key(), record.command(),
                        record.seeds().ownerNonce(), record.lease(), work);
                // Recover the local handle for the exact capture written by the
                // V106 fixture. This verifies its existing rows; it creates no drain.
                capture = c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                    DocumentPublicationModesJournal.insert(em, rig.claim(), record,
                            DocumentPublicationModesJournal.encode(record.command(), Map.of(
                                    record.command().intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED)));
                    assertThat(admission.apply(em, rig.claim()).owner()).isPresent();
                    return DocumentPreparationCaptureDrain.register(c.tx(), em, rig.record(), pins,
                            rig.claim(), rig.coordinator(), rig.sources(), work, NONE);
                });
            }
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").target("107").load().migrate();
            assertThat(DocumentPreparationCoverageCertificatesIT.count(c, rig.record(), "repository_preparation_capture_drains")).isZero();
            assertThat(DocumentPreparationCoverageCertificatesIT.count(c, rig.record(), "repository_preparation_pin_owners")).isEqualTo(1);
            assertThat(capture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
        }
    }

    @Test void initialCaptureClassificationRequiresProcessAuthorityAndExactModes() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            assertThatThrownBy(() -> initialState(c, rig, new RepositoryCaller("principal", false),
                    Map.of("member", DocumentPublicationCandidate.Mode.TYPED)))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThatThrownBy(() -> initialState(c, rig, CALLER, Map.of("member", DocumentPublicationCandidate.Mode.OPAQUE)))
                    .hasMessageContaining("incomplete or inconsistent");
            assertThat(initialState(c, rig, CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED)))
                    .isEqualTo(RepositoryInitialHistoricalCaptureState.State.REGISTERED);
            assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
        }
    }

    private static RepositoryInitialHistoricalCaptureState.State initialState(Context c, Rig rig,
            RepositoryCaller caller, Map<String, DocumentPublicationCandidate.Mode> modes) {
        return RepositoryInitialHistoricalCaptureState.classify(c.tx(), rig.budget(), caller, rig.record(), modes,
                rig.capture().identity().owner(), rig.capture().identity(), NONE);
    }

    private static void verifyDifferentInitialWinner(Context c, Rig rolledBack) throws Exception {
        var record = new DocumentPublicationPreparationRecord(rolledBack.record().key(), rolledBack.command(),
                DocumentPublicationSeeds.mint(rolledBack.record().key(), rolledBack.command()),
                rolledBack.record().placements(), Duration.ofMinutes(5), 0);
        var history = rolledBack.reads().captureHistorical(CALLER, rolledBack.fixture().address(), rolledBack.fixture().revision());
        try (var sources = DocumentHistoricalAssessmentSources.open(record.command(), CALLER, List.of(history), NONE)) {
            var registration = DocumentPublicationRegistration.historical(c.tx(), rolledBack.budget(), record, sources,
                    UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), NONE);
            var modes = Map.of("member", DocumentPublicationCandidate.Mode.TYPED);
            assertThat(registration.admitInitial(CALLER, modes, NONE)).isPresent();
            var winner = registration.historicalCapture().orElseThrow();
            assertThat(winner.identity().owner().token()).isNotEqualTo(rolledBack.capture().identity().owner().token());
            assertThat(winner.identity().pinsSha256()).isNotEqualTo(rolledBack.capture().identity().pinsSha256());
            assertThat(initialState(c, rolledBack, CALLER, modes)).isEqualTo(RepositoryInitialHistoricalCaptureState.State.ABSENT);
            assertThat(RepositoryInitialHistoricalCaptureState.classify(c.tx(), rolledBack.budget(), CALLER,
                    record, modes, registration.drainIdentity(), winner.identity(), NONE))
                    .isEqualTo(RepositoryInitialHistoricalCaptureState.State.REGISTERED);
            var receipt = winner.complete(CALLER, Duration.ZERO, NONE).orElseThrow();
            assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, winner.identity(), NONE)).contains(receipt);
            assertThat(DocumentPreparationCaptureDrain.confirm(c.tx(), CALLER, rolledBack.capture().identity(), NONE)).isEmpty();
            assertThat(count(c, "repository_preparation_capture_drains", rolledBack)).isEqualTo(1);
        } finally {
            history.close();
            assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue();
            history.release();
        }
    }

    @ParameterizedTest @ValueSource(strings = {"batch", "pins", "owner"})
    void partialInitialCaptureEvidenceIsNeverClassifiedAsAbsent(String corruption) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            String table = switch (corruption) {
                case "batch" -> "repository_preparation_pin_batches";
                case "pins" -> "repository_preparation_source_pins";
                default -> "repository_preparation_pin_owners";
            };
            // Deliberate corruption of this test's isolated schema, not a supported mutation.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE " + table + " DISABLE TRIGGER USER").executeUpdate();
                String mutation = switch (corruption) {
                    case "batch" -> "UPDATE " + table + " SET sealed=false WHERE operation_id=:o";
                    case "pins" -> "DELETE FROM " + table + " WHERE operation_id=:o";
                    default -> "UPDATE " + table + " SET incarnation='00000000-0000-0000-0000-000000000001' WHERE operation_id=:o";
                };
                assertThat(em.createNativeQuery(mutation).setParameter("o", rig.command().operationId()).executeUpdate()).isEqualTo(1);
                em.createNativeQuery("ALTER TABLE " + table + " ENABLE TRIGGER USER").executeUpdate();
            });
            assertThatThrownBy(() -> initialState(c, rig, CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED)))
                    .hasMessageContaining("incomplete or inconsistent");
            assertThat(count(c, "repository_preparation_capture_drains", rig)).isZero();
        }
    }

    @Test void initialCaptureRemainsRegisteredAfterSuccessorTakeover() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofSeconds(1))) {
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", rig.command().operationId()).getSingleResult());
            var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(rig.capture().identity().owner(),
                    UUID.randomUUID(), UUID.randomUUID(), Duration.ofMinutes(5),
                    new RepositoryCoordinatorReservation.OwnerIdentity(1, rig.record().seeds().ownerNonce()));
            RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
            assertThat(RepositoryCoordinatorReservation.confirm(c.tx(), CALLER, reservation, NONE)).isPresent();
            assertThat(initialState(c, rig, CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED)))
                    .isEqualTo(RepositoryInitialHistoricalCaptureState.State.REGISTERED);
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
        }
    }

    @Test void initialCaptureRemainsRegisteredAfterTerminalRootRelease() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, c.tx(), Duration.ofMinutes(5))) {
            new DocumentPublicationRejections(c.tx()).cancel(CALLER, rig.owner().orElseThrow(), rig.command(), NONE);
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
            DocumentPreparationRootReleases.release(c.tx(), rig.budget(), CALLER, rig.record(), NONE);
            assertThat(count(c, "repository_preparation_history_roots", rig)).isZero();
            assertThat(initialState(c, rig, CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED)))
                    .isEqualTo(RepositoryInitialHistoricalCaptureState.State.REGISTERED);
            assertThat(rig.capture().complete(CALLER, Duration.ZERO, NONE)).isPresent();
            assertThat(count(c, "repository_preparation_capture_drains", rig)).isEqualTo(1);
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
            DocumentPreparationCaptureDrain.Capture capture, PayloadBudget budget,
            Optional<RepositoryOperationLedger.Owner> owner) implements AutoCloseable {
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
            Optional<RepositoryOperationLedger.Owner> owner = Optional.empty();
            if (failedRegistration) {
                assertThatThrownBy(() -> registration.admitInitial(CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED), NONE))
                        .hasStackTraceContaining("Injected registration");
            } else {
                owner = registration.admitInitial(CALLER, Map.of("member", DocumentPublicationCandidate.Mode.TYPED), NONE);
                assertThat(owner).isPresent();
            }
            var result = new Rig(fixture, command, record, reads, history, sources, registration.historicalCapture().orElseThrow(), budget, owner);
            delivered = true; return result;
        } finally { if (!delivered) { sources.close(); release(reads, history); } }
    }

    private static long count(Context c, String table, Rig rig) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:o")
                .setParameter("o", rig.command().operationId()).getSingleResult()).longValue());
    }
}

package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Actual pre-owner preparation, capture and abandonment transactions; source provider observations are synthetic. */
@Testcontainers
class DocumentCaptureAdmissionClosureIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void historicalSuccessorInstallDoesNotGrantActivationOrInventCaptureCoverage() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, false, false, true, Duration.ofSeconds(1))) {
            var command = rig.record().command();
            var plan = installedHistoricalSuccessor(c, rig);
            assertThatThrownBy(() -> RepositorySuccessorExecution.activate(c.tx(), rig.budget(), CALLER, CALLER, plan, NONE))
                    .isInstanceOf(UnsupportedOperationException.class).hasMessage("Historical reuse execution is not implemented");
            long grants = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_successor_executions WHERE operation_id=:o")
                    .setParameter("o", command.operationId()).getSingleResult()).longValue());
            long bindings = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_coordinator_bindings WHERE operation_id=:o AND claim_epoch=2")
                    .setParameter("o", command.operationId()).getSingleResult()).longValue());
            assertThat(grants).isZero(); assertThat(bindings).isZero();
            assertThat(batches(c, rig)).isEqualTo(1);
            var oldCoverage = c.tx().readOnly(em -> DocumentPreparationHistoryRoots.coverage(em, rig.record(),
                    DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(rig.record()))));
            var nextCoverage = c.tx().readOnly(em -> DocumentPreparationHistoryRoots.coverage(em, plan.next(),
                    DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.next()))));
            assertThat(oldCoverage).isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
            assertThat(nextCoverage).isEqualTo(DocumentPreparationHistoryRoots.Coverage.UNKNOWN);
        }
    }

    static Rig historicalInitial(Context c) throws Exception { return prepare(c, false, false, true, Duration.ofSeconds(1)); }
    static Rig historicalInitial(Context c, Duration lease) throws Exception { return prepare(c, false, false, true, lease); }

    static Rig historicalInitial(Context c, java.util.function.Consumer<DocumentPublicationPreparationRecord> beforeClaim,
            Runnable afterPreparation) throws Exception {
        return prepare(c, false, false, true, Duration.ofMinutes(5), beforeClaim, false, afterPreparation);
    }

    static Rig historicalCreationInitial(Context c, java.util.function.Consumer<DocumentPublicationPreparationRecord> beforeClaim) throws Exception {
        return prepare(c, false, false, true, Duration.ofSeconds(1), beforeClaim, true);
    }

    static RepositorySuccessorInstall.Plan installedHistoricalSuccessor(Context c, Rig rig) {
        return installedHistoricalSuccessor(c, rig, LEASE);
    }

    static RepositorySuccessorInstall.Plan installedHistoricalSuccessor(Context c, Rig rig, Duration successorLease) {
            var command = rig.record().command();
            var modes = Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
            try (var work = rig.sources().work()) {
                var admission = RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(), command,
                        rig.record().seeds().ownerNonce(), Duration.ofSeconds(1), work);
                c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                    DocumentPublicationModesJournal.insert(em, rig.claim(), rig.record(),
                            DocumentPublicationModesJournal.encode(command, modes));
                    admission.apply(em, rig.claim());
                });
            }
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id", rig.record().key().operationId()).getSingleResult());
            var identity = new RepositoryCoordinatorDrain.Identity(rig.record().key(), command.sha256(),
                    rig.claim().epoch(), rig.claim().token(), rig.coordinator());
            var reservation = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity,
                    UUID.randomUUID(), UUID.randomUUID(), successorLease,
                    new RepositoryCoordinatorReservation.OwnerIdentity(1, rig.record().seeds().ownerNonce()));
            RepositoryCoordinatorExpiration.reserve(c.tx(), CALLER, reservation, NONE);
            var plan = RepositorySuccessorInstall.prepare(reservation, rig.record(), successorLease, modes);
            RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, plan, NONE);
            return plan;
    }

    @Test void migrationCannotInventInitialCaptureForRetainedLegacyHistory() throws Exception {
        var legacy = DocumentPreparationRootReleaseIT.legacyReaderContext(POSTGRES, "103");
        try (var c = legacy.context(); var rig = prepare(c, false, false, false)) {
            legacy.disable();
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            var bytes = DocumentPublicationPreparationCodec.encode(rig.record());
            var before = c.tx().readOnly(em -> DocumentPreparationHistoryRoots.coverage(em, rig.record(),
                    DocumentPublicationPreparationJournal.digest(bytes)));
            assertThat(before).isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
            assertThat(batches(c, rig)).isZero();
            var coverage = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); }))
                    .hasMessageContaining("capture coverage is unknown");
            // A later capture is valid evidence of that reader, not of the creation transaction's readers.
            append(c, rig);
            assertThat(batches(c, rig)).isEqualTo(1);
            rig.sources().close(); rig.history().close(); rig.history().release();
            rig.reads().fence(); rig.reads().attestLocalQuiescence();
            var digest = c.tx().readOnly(em -> (byte[]) em.createNativeQuery(
                    "SELECT pins_sha256 FROM repository_preparation_pin_batches WHERE operation_id=:o")
                    .setParameter("o", rig.record().key().operationId()).getSingleResult());
            var identity = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(
                    rig.record().key(), rig.record().command().sha256(), rig.claim().epoch(), rig.claim().token(),
                    rig.coordinator()), 0, HexFormat.of().formatHex(digest));
            assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, identity, NONE).kind()).isEqualTo("QUIESCED");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); }))
                    .hasMessageContaining("initial capture coverage is unknown");
            var after = c.tx().readOnly(em -> DocumentPreparationHistoryRoots.coverage(em, rig.record(),
                    DocumentPublicationPreparationJournal.digest(bytes)));
            assertThat(after).isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
        }
    }

    @Test void onePhysicalObjectInTwoRevisionsRetainsTwoCaptureIdentities() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c)) {
            var original = rig.fixture().original();
            var reuse = new DocumentPublicationCommand(rig.record().command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).build());
            UUID secondRevision;
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(reuse, original.batch().policy(),
                    Map.of("member", DocumentPublicationCandidate.Mode.TYPED), Map.of("member", rig.fixture().fragments()),
                    Optional.empty(), (m, occurrence) -> { throw new AssertionError("Retained reuse must not resolve a live registry"); },
                    rig.budget(), new ai.protomolt.proto.repo.codec.DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 1_000_000),
                    java.time.Instant.now(), CALLER, List.of(rig.history()), NONE)) {
                var prepared = assessment.preparePhysical(rig.record().placements(), Map.of(), LEASE, Map.of(), NONE);
                var owner = new RepositoryOperationLedger(c.tx()).admitHistorical(CALLER,
                        new RepositoryOperationLedger.Key("account", "principal", reuse.operationId()), prepared, UUID.randomUUID(), LEASE)
                        .owner().orElseThrow();
                new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared);
                var result = assessment.publish(CALLER, owner, prepared, Map.of(), new RepositorySchemaArtifacts(c.tx()),
                        new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), true, false), NONE);
                assertThat(result.getMembers(0).getMutationRevision()).isEqualTo(2);
                secondRevision = UUID.fromString(result.getMembers(0).getRevisionId());
            }
            var history = rig.reads().captureHistorical(CALLER, rig.fixture().address(), secondRevision);
            try {
                var firstMember = aliasDestination(member(rig.fixture(), rig.history()), "old");
                var secondMember = aliasDestination(member(new Fixture(original, secondRevision), history), "new");
                assertThat(firstMember.getParts(0).getHistoricalReuse().getObject().getObjectId())
                        .isEqualTo(secondMember.getParts(0).getHistoricalReuse().getObject().getObjectId());
                var command = new DocumentPublicationCommand(reuse.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                        .clearMembers().addMembers(firstMember).addMembers(secondMember).build());
                var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        rig.record().placements(), LEASE, 0);
                try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(rig.history(), history), NONE)) {
                    DocumentPreparationCaptureDrain.Capture capture;
                    try (var work = sources.work()) {
                        var pins = DocumentPreparationSourcePins.prepare(command, work.references(command, () -> {}), () -> {});
                        assertThat(pins.pins()).hasSize(2);
                        assertThat(pins.pins().stream().map(DocumentHistoricalSourcePin::object).distinct()).hasSize(1);
                        assertThat(pins.pins().stream().map(DocumentHistoricalSourcePin::revision).distinct()).hasSize(2);
                        capture = c.tx().inTransaction(em -> {
                            var acquired = RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(em, key, command, UUID.randomUUID(), LEASE, work);
                            var coordinator = UUID.randomUUID(); RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
                            var bytes = DocumentPublicationPreparationCodec.encode(record);
                            DocumentPublicationPreparationJournal.insert(em, acquired.claim(), record, bytes,
                                    DocumentPublicationPreparationJournal.digest(bytes), work.references(command, () -> {}));
                            lockSources(em, rig.fixture(), pins);
                            return DocumentPreparationCaptureDrain.register(c.tx(), em, record, pins, acquired.claim(), coordinator, sources, work, NONE);
                        });
                    }
                    rig.sources().close();
                    assertThat(capture.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                    var coverage = DocumentPreparationCaptureCoverage.prepare(record, NONE);
                    int qualified = c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); });
                    assertThat(qualified).isEqualTo(1);
                }
            } finally { history.close(); history.release(); }
        }
    }

    private static ai.protomolt.proto.repo.v1.DocumentPublicationMember aliasDestination(
            ai.protomolt.proto.repo.v1.DocumentPublicationMember member, String name) {
        return member.toBuilder().setMemberId(name).setDestination(ai.protomolt.proto.repo.v1.DocumentRevisionCondition.newBuilder()
                .setIfAbsent(true).setAddress(member.getDestination().getAddress().toBuilder().setGraphAddressId("alias-" + name))).build();
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void internallyConsistentBatchCannotOmitOrDuplicateSelectedObjects(boolean duplicate) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, false, !duplicate)) {
            var history = rig.reads().captureHistorical(CALLER, rig.fixture().address(), rig.fixture().revision());
            try (var sources = DocumentHistoricalAssessmentSources.open(rig.record().command(), CALLER, List.of(history), NONE)) {
                var refs = new ArrayList<>(sources.references(rig.record().command(), () -> {}));
                if (duplicate) refs.addAll(rig.sources().references(rig.record().command(), () -> {}));
                var full = DocumentPreparationSourcePins.prepare(rig.record().command(), refs, () -> {});
                assertThat(full.pins()).hasSize(2);
                DocumentPreparationSourcePins.Prepared candidate = full;
                if (!duplicate) {
                    var one = List.of(full.pins().getFirst());
                    var json = com.google.gson.JsonParser.parseString(full.json()).getAsJsonArray();
                    var subset = new com.google.gson.JsonArray(); subset.add(json.get(0));
                    candidate = new DocumentPreparationSourcePins.Prepared(one, DocumentPreparationSourcePins.digest(one, () -> {}), subset.toString());
                }
                var pins = candidate;
                // Exercise low-level persisted evidence with real live pins and valid SQL guards.
                // Its count and digest agree internally, but its object selection is not canonical.
                c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                    lockSources(em, rig.fixture(), pins);
                    DocumentPreparationSourcePins.insert(em, rig.record(), pins, rig.claim(), rig.coordinator(), () -> {});
                });
                assertThat(batches(c, rig)).isEqualTo(2);
                sources.close(); history.close(); history.release();
                rig.sources().close(); rig.history().close(); rig.history().release();
                rig.reads().fence(); rig.reads().attestLocalQuiescence();
                var digests = c.tx().readOnly(em -> em.createNativeQuery(
                        "SELECT encode(pins_sha256,'hex') FROM repository_preparation_pin_batches WHERE operation_id=:o")
                        .setParameter("o", rig.record().key().operationId()).getResultList());
                for (var digest : digests) {
                    var id = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(rig.record().key(),
                            rig.record().command().sha256(), rig.claim().epoch(), rig.claim().token(), rig.coordinator()), 0, (String) digest);
                    assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, id, NONE).kind()).isEqualTo("QUIESCED");
                }
                var check = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return check.lockAndRequireDrained(em, NONE); }))
                        .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS))
                        .hasMessageContaining("capture coverage differs");
            } finally { history.close(); history.release(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void everyCaptureMustDrainIncludingOriginalWithRepeatedSelectors(boolean repeatedSelector) throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c, repeatedSelector)) {
            var originalPins = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            assertThat(originalPins.pins()).hasSize(1);
            var reader = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reader.captureHistorical(CALLER, rig.fixture().address(), rig.fixture().revision());
            try (var sources = DocumentHistoricalAssessmentSources.open(rig.record().command(), CALLER, List.of(history), NONE)) {
                DocumentPreparationCaptureDrain.Capture later;
                try (var work = sources.work()) {
                    var pins = DocumentPreparationSourcePins.prepare(rig.record().command(), work.references(rig.record().command(), () -> {}), () -> {});
                    assertThat(pins.pins()).hasSize(1);
                    later = c.tx().inTransaction(em -> {
                        RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                        lockSources(em, rig.fixture(), pins);
                        return DocumentPreparationCaptureDrain.register(c.tx(), em, rig.record(), pins, rig.claim(), rig.coordinator(), sources, work, NONE);
                    });
                }
                assertThat(later.complete(CALLER, Duration.ZERO, NONE)).isPresent();
                var coverage = DocumentPreparationCaptureCoverage.prepare(rig.record(), NONE);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); }))
                        .hasMessageContaining("has not drained");
                rig.sources().close(); rig.history().close(); rig.history().release();
                rig.reads().fence(); rig.reads().attestLocalQuiescence();
                var identity = new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(rig.record().key(),
                        rig.record().command().sha256(), rig.claim().epoch(), rig.claim().token(), rig.coordinator()), 0,
                        HexFormat.of().formatHex(originalPins.digest()));
                assertThat(DocumentPreparationCaptureDrain.recover(c.tx(), CALLER, identity, NONE).kind()).isEqualTo("QUIESCED");
                int count = c.tx().inTransaction(em -> { return coverage.lockAndRequireDrained(em, NONE); });
                assertThat(count).isEqualTo(2);
            } finally { history.close(); history.release(); reader.fence(); reader.attestLocalQuiescence(); }
        }
    }

    @Test void committedCancellationRejectsAnotherCapture() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c); var work = rig.sources().work()) {
            var command = rig.record().command();
            var modes = DocumentPublicationModesJournal.encode(command,
                    Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
            var admission = RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(), command,
                    rig.record().seeds().ownerNonce(), LEASE, work);
            var owner = c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPublicationModesJournal.insert(em, rig.claim(), rig.record(), modes);
                return admission.apply(em, rig.claim()).owner().orElseThrow();
            });
            var cancelled = new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, command, NONE);
            assertThat(cancelled.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
            var receipt = cancelled.rejection().orElseThrow();
            assertThat(receipt.getCommandSha256()).isEqualTo(command.sha256());
            assertThat(receipt.getOwnerGeneration()).isEqualTo(owner.generation());
            assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, command).rejection()).contains(receipt);
            assertThatThrownBy(() -> append(c, rig)).hasStackTraceContaining("Publication capture admission is closed");
            assertThat(batches(c, rig)).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void abandonedPreparationCannotAddAnotherCaptureButOpenPreparationCan(boolean migrateExisting) throws Exception {
        var legacy = migrateExisting ? DocumentPreparationRootReleaseIT.legacyReaderContext(POSTGRES, "107") : null;
        try (var c = migrateExisting ? legacy.context() : context(POSTGRES); var rig = prepare(c)) {
            // A positive control: an open preparation can add a fresh capture.
            append(c, rig);
            assertThat(batches(c, rig)).isEqualTo(2);
            DocumentPublicationAbandonment.abandon(c.tx(), rig.budget(), CALLER, rig.claim(), rig.record(), NONE);
            if (migrateExisting) org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").target("108").load().migrate();
            assertThatThrownBy(() -> append(c, rig)).hasStackTraceContaining("Publication capture admission is closed");
            assertThat(batches(c, rig)).isEqualTo(2);
            var existing = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPreparationSourcePins.insert(em, rig.record(), existing, rig.claim(), rig.coordinator(), () -> {});
                int inserted = em.createNativeQuery("""
                        INSERT INTO repository_preparation_pin_batches(account_id,principal,operation_id,predecessor_generation,
                         pins_sha256,expected_count,initial_capture) VALUES('account','principal',:o,0,:d,:count,false)
                        ON CONFLICT DO NOTHING
                        """).setParameter("o", rig.record().key().operationId()).setParameter("d", existing.digest())
                        .setParameter("count", existing.pins().size()).executeUpdate();
                assertThat(inserted).isZero();
            });
            assertThat(batches(c, rig)).isEqualTo(2);
        }
    }

    @Test void ownerInsertionRechecksAbandonmentEvenWhenBatchBeganOpen() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c)) {
            // Insert the actual V85 marker between batch insertion and owner insertion.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION test_abandon_before_capture_owner() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                         INSERT INTO repository_publication_abandonments(account_id,principal,operation_id,predecessor_generation,
                          owner_nonce,preparation_sha256,claim_epoch,claim_token)
                         SELECT p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.owner_nonce,p.preparation_sha256,
                          NEW.claim_epoch,NEW.claim_token FROM repository_publication_preparations p
                          WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id;
                         RETURN NEW;
                        END; $$
                        """).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER a_test_abandon BEFORE INSERT ON repository_preparation_pin_owners "
                        + "FOR EACH ROW EXECUTE FUNCTION test_abandon_before_capture_owner()").executeUpdate();
            });
            assertThatThrownBy(() -> append(c, rig)).hasStackTraceContaining("Publication capture admission is closed");
            assertThat(batches(c, rig)).isEqualTo(1);
            long abandonments = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_publication_abandonments WHERE operation_id=:o")
                    .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
            assertThat(abandonments).isZero(); // The failed capture transaction rolls back the interposed marker too.
        }
    }

    @Test void captureWaitingOnAbandonmentClaimLockSeesCommittedClosure() throws Exception {
        try (var c = context(POSTGRES); var rig = prepare(c)) {
            var entered = new java.util.concurrent.CountDownLatch(1);
            var finish = new java.util.concurrent.CountDownLatch(1);
            var captureEntered = new java.util.concurrent.CountDownLatch(1);
            var blocker = new java.util.concurrent.atomic.AtomicInteger();
            var waiter = new java.util.concurrent.atomic.AtomicInteger();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                    rows.next(); blocker.set(rows.getInt(1));
                }
                entered.countDown();
                try {
                    if (!finish.await(15, java.util.concurrent.TimeUnit.SECONDS)) throw new java.sql.SQLException("Abandonment gate timeout");
                } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(failure); }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                var abandoning = executor.submit(() -> DocumentPublicationAbandonment.abandon(new Tx(emf), rig.budget(), CALLER,
                        rig.claim(), rig.record(), NONE));
                try {
                    assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    var capturing = executor.submit(() -> {
                        append(c, rig, em -> {
                            waiter.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                            captureEntered.countDown();
                        });
                        return true;
                    });
                    assertThat(captureEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    boolean blocked = false;
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery(
                                "SELECT :blocker=ANY(pg_blocking_pids(:waiter))")
                                .setParameter("blocker", blocker.get()).setParameter("waiter", waiter.get()).getSingleResult());
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).as("capture waits for exact abandonment transaction").isTrue();
                    finish.countDown();
                    abandoning.get(10, java.util.concurrent.TimeUnit.SECONDS);
                    assertThatThrownBy(() -> capturing.get(10, java.util.concurrent.TimeUnit.SECONDS))
                            .hasStackTraceContaining("Publication capture admission is closed");
                    assertThat(batches(c, rig)).isEqualTo(1);
                } finally { finish.countDown(); }
            }
        }
    }

    record Rig(Fixture fixture, DocumentReadLedger reads, DocumentReadLedger.PinnedHistory history,
            DocumentHistoricalAssessmentSources sources, DocumentPublicationPreparationRecord record,
            RepositoryExecutionClaimLedger.Claim claim, UUID coordinator, PayloadBudget budget) implements AutoCloseable {
        @Override public void close() throws Exception {
            sources.close(); release(reads, history); assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static Rig prepare(Context c) throws Exception {
        return prepare(c, false);
    }

    private static Rig prepare(Context c, boolean repeatedSelector) throws Exception {
        return prepare(c, repeatedSelector, false);
    }

    private static Rig prepare(Context c, boolean repeatedSelector, boolean twoParts) throws Exception {
        return prepare(c, repeatedSelector, twoParts, true);
    }

    private static Rig prepare(Context c, boolean repeatedSelector, boolean twoParts, boolean capture) throws Exception {
        return prepare(c, repeatedSelector, twoParts, capture, LEASE);
    }

    private static Rig prepare(Context c, boolean repeatedSelector, boolean twoParts, boolean capture, Duration lease) throws Exception {
        return prepare(c, repeatedSelector, twoParts, capture, lease, ignored -> {}, false);
    }

    private static Rig prepare(Context c, boolean repeatedSelector, boolean twoParts, boolean capture, Duration lease,
            java.util.function.Consumer<DocumentPublicationPreparationRecord> beforeClaim, boolean creation) throws Exception {
        return prepare(c, repeatedSelector, twoParts, capture, lease, beforeClaim, creation, () -> {});
    }

    private static Rig prepare(Context c, boolean repeatedSelector, boolean twoParts, boolean capture, Duration lease,
            java.util.function.Consumer<DocumentPublicationPreparationRecord> beforeClaim, boolean creation,
            Runnable afterPreparation) throws Exception {
        var document = ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("capture-coverage")
                .setOwnership(ai.protomolt.proto.repo.v1.OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(ai.protomolt.proto.repo.v1.DocumentSecurity.getDefaultInstance()))
                .setStructuredData(com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of("core"), "type.test"));
        if (twoParts) document.putParserResults("parsed", ai.protomolt.proto.repo.v1.ParserResult.newBuilder()
                .setDocument(ai.protomolt.proto.repo.v1.ParserDocument.newBuilder()
                        .setShape(com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of("parsed"), "type.test"))).build());
        var original = DocumentSchemaRetentionFixture.prepare(c, true, false, document.build(), "node", "capture-coverage");
        new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
        var fixture = new Fixture(original, DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {})));
        var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
        var history = reads.captureHistorical(CALLER, fixture.address(), fixture.revision());
        var intent = original.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history));
        if (creation) {
            var target = intent.getMembers(0).toBuilder();
            target.setDestination(target.getDestination().toBuilder().clearExpectedMutationRevision().setIfAbsent(true)
                    .setAddress(target.getDestination().getAddress().toBuilder().setGraphAddressId("historical-creation")));
            intent.setMembers(0, target);
        }
        if (repeatedSelector) {
            var repeated = intent.getMembers(0).toBuilder().setMemberId("repeat");
            repeated.setDestination(repeated.getDestination().toBuilder().clearExpectedMutationRevision().setIfAbsent(true)
                    .setAddress(repeated.getDestination().getAddress().toBuilder().setGraphAddressId("repeat-destination")));
            intent.addMembers(repeated);
        }
        var command = new DocumentPublicationCommand(intent.build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var placement = original.prepared().members().getFirst().placement();
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                Map.of(placement.drive().id(), placement), lease, 0);
        var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), NONE);
        var coordinator = UUID.randomUUID();
        var budget = new PayloadBudget(64L * 1024 * 1024);
        boolean delivered = false;
        try {
            beforeClaim.accept(record);
            var pins = DocumentPreparationSourcePins.prepare(command, sources.references(command, () -> {}), () -> {});
            var claim = c.tx().inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(em, key, command, UUID.randomUUID(), lease, sources);
                RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
                var bytes = DocumentPublicationPreparationCodec.encode(record);
                if (LegacyPublicationPreparationFixture.beforeCoverageCertificates(em))
                    LegacyPublicationPreparationFixture.insertProjected(em, record, sources.references(command, () -> {}));
                else DocumentPublicationPreparationJournal.insert(em, acquired.claim(), record, bytes,
                        DocumentPublicationPreparationJournal.digest(bytes), sources.references(command, () -> {}));
                afterPreparation.run();
                lockSources(em, fixture, pins);
                if (capture) DocumentPreparationSourcePins.insert(em, record, pins, acquired.claim(), coordinator, () -> {});
                return acquired.claim();
            });
            delivered = true; return new Rig(fixture, reads, history, sources, record, claim, coordinator, budget);
        } finally { if (!delivered) { sources.close(); release(reads, history); } }
    }

    static void append(Context c, Rig rig) throws Exception {
        append(c, rig, em -> {});
    }

    private static void append(Context c, Rig rig, java.util.function.Consumer<jakarta.persistence.EntityManager> beforeLock) throws Exception {
        var history = rig.reads().captureHistorical(CALLER, rig.fixture().address(), rig.fixture().revision());
        try (var sources = DocumentHistoricalAssessmentSources.open(rig.record().command(), CALLER, List.of(history), NONE)) {
            var pins = DocumentPreparationSourcePins.prepare(rig.record().command(), sources.references(rig.record().command(), () -> {}), () -> {});
            c.tx().inTransaction(em -> {
                beforeLock.accept(em);
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                lockSources(em, rig.fixture(), pins);
                DocumentPreparationSourcePins.insert(em, rig.record(), pins, rig.claim(), rig.coordinator(), () -> {});
            });
        } finally { history.close(); history.release(); }
    }

    private static void lockSources(jakarta.persistence.EntityManager em, Fixture fixture, DocumentPreparationSourcePins.Prepared pins) {
        var objects = pins.pins().stream().map(DocumentHistoricalSourcePin::object).collect(java.util.stream.Collectors.toSet());
        var origins = DocumentPublicationLocks.lockIndependentOrigins(em, Set.of(DocumentIds.nodeId(fixture.address())), objects, Set.of());
        DocumentPublicationLocks.lockIndependentRetention(em, origins);
    }

    private static long batches(Context c, Rig rig) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_preparation_pin_batches WHERE operation_id=:o")
                .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
    }
}

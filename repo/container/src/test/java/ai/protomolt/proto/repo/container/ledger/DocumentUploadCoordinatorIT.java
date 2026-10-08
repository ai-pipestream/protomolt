package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real SQL and versioned adapter staging. Typed validation and publication are separate boundaries. */
@Testcontainers
class DocumentUploadCoordinatorIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3=new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @Container static final org.testcontainers.containers.GenericContainer<?> REDIS =
            new org.testcontainers.containers.GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    private static LedgerDatabase database;
    private static Tx tx;
    private static OpenedBlobStore opened;
    private static ManagedBackendLedger.Profile profile;
    private static DocumentOperationUploadAdmission admission;
    private static final String GENERATION="selected-transfer";
    private static final String NAMESPACE="selected-transfer";
    private static final Duration LEASE=Duration.ofMinutes(5);
    private static final SqlTimeouts SQL_LIMITS = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5));
    private static final RepositoryCaller ADMIN=new RepositoryCaller("principal",true);

    @BeforeAll static void open() {
        database=new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword()));
        tx=new Tx(database.entityManagerFactory()); admission=new DocumentOperationUploadAdmission(tx,new DriveLedger(tx));
        opened=BlobStores.discover().open("s3",Map.of("endpoint",S3.getEndpoint().toString(),"region",S3.getRegion(),
                "access-key",S3.getAccessKey(),"secret-key",S3.getSecretKey(),"path-style","true","conditional-writes","false"));
        opened.ensureNamespace(NAMESPACE);
        try (var admin=software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(software.amazon.awssdk.regions.Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(S3.getAccessKey(),S3.getSecretKey()))).build()) {
            admin.putBucketVersioning(b -> b.bucket(NAMESPACE).versioningConfiguration(v ->
                    v.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        }
        profile=new ManagedBackendLedger.Profile(S3BackendIdentity.of(S3.getEndpoint().toString(),S3.getRegion(),true),"selected-transfer-realm");
        new ManagedBackendLedger(tx).bind(GENERATION,profile);
    }
    @AfterAll static void close() throws Exception { if(opened!=null) opened.close(); if(database!=null) database.close(); }

    private record Fixture(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<UUID, DocumentUploadPlan.Placement> placements, UUID attempt) {}

    @Test void deliveryAuthorityFailurePreservesVerifiedWritesForExactReplay() throws Exception {
        var f=fixture(2,LEASE);
        var puts=new java.util.concurrent.atomic.AtomicInteger();
        var verified=new java.util.concurrent.atomic.AtomicInteger();
        var delivered=new java.util.concurrent.atomic.AtomicInteger();
        var store=intercept((method,args,call)->{
            if(method.equals("put")) puts.incrementAndGet();
            return call.call();
        });
        var budget=new PayloadBudget(1_000_000);
        try(var coordinator=coordinator(store,budget,Duration.ofMillis(25))) {
            var authority=new DocumentUploadAuthority() {
                public DocumentOperationUploadAdmission.Admission admit() {
                    return admission.admitOrReuseVerified(ADMIN,f.owner,f.prepared);
                }
                public void renewOwnerAndSelections(List<DocumentSelectedAttemptLedger.Selected> selections) {
                    var renewal=selections.isEmpty()?null:DocumentSelectedAttemptLedger.prepareRenewal(selections,LEASE);
                    tx.inTransaction(em->{DocumentSelectedAttemptLedger.renewOwnerAndSelections(em,f.owner,renewal,LEASE);});
                }
                public List<DocumentPartAttemptLedger.Attempt> renewSelections(List<DocumentSelectedAttemptLedger.Selected> selections) {
                    var renewal=DocumentSelectedAttemptLedger.prepareRenewal(selections,LEASE);
                    return tx.inTransaction(em->{return DocumentSelectedAttemptLedger.renew(em,f.owner,renewal);});
                }
                public void verify(DocumentSelectedAttemptLedger.Selected selection,List<DocumentSelectedAttemptLedger.Observation> rows) {
                    var encoded=DocumentSelectedAttemptLedger.prepareVerification(rows);
                    tx.inTransaction(em->{DocumentSelectedAttemptLedger.verifyBatch(em,f.owner,selection,encoded);});
                    verified.addAndGet(rows.size());
                }
                public void recheckPreparation(List<DocumentSelectedAttemptLedger.Selected> selections,Runnable active) {
                    throw new AssertionError("Staging does not perform assessment preparation");
                }
                public void afterDrain() {
                    assertThat(coordinator.providerActivity().active()).isZero();
                    assertThat(verified.get()).isEqualTo(2);
                    delivered.incrementAndGet();
                    throw new IllegalStateException("injected delivery authority failure");
                }
            };
            assertThatThrownBy(()->coordinator.stageAuthorized(f.prepared,f.bodies,Map.of(),()->{},authority))
                    .hasMessageContaining("injected delivery authority failure");
            assertThat(delivered.get()).isEqualTo(1);
            assertThat(puts.get()).isEqualTo(2);
            assertThat(budget.reservedBytes()).isZero();
            var replay=coordinator.stage(ADMIN,f.owner,f.prepared,f.bodies,Map.of(),()->{});
            assertThat(replay.members()).allSatisfy(member->assertThat(member.attempt().state()).isEqualTo("VERIFIED"));
            assertThat(puts.get()).isEqualTo(2);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void exactVerifiedInitialRetryDoesNotWriteProviderAgain() throws Exception {
        var f = fixture(2, LEASE);
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, call) -> {
            if (method.equals("put")) puts.incrementAndGet();
            return call.call();
        });
        var budget = new PayloadBudget(1_000_000);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25))) {
            var original = coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {});
            int before = puts.get(); assertThat(before).isEqualTo(2);
            var repeated = coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {});
            assertThat(puts.get()).isEqualTo(before);
            assertThat(repeated.members()).extracting(m -> m.attempt().id())
                    .containsExactlyElementsOf(original.members().stream().map(m -> m.attempt().id()).toList());
            assertThat(repeated.members()).allSatisfy(m -> assertThat(m.attempt().state()).isEqualTo("VERIFIED"));
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"staging", "token", "expired", "displaced", "command"})
    void verifiedReuseRefusesUnfinishedOrChangedAuthority(String kind) throws Exception {
        var f = fixture(1, kind.equals("expired") ? Duration.ofSeconds(3) : LEASE);
        if (kind.equals("staging")) admission.admit(ADMIN, f.owner, f.prepared);
        else try (var coordinator = coordinator(opened.store(), new PayloadBudget(1_000_000), Duration.ofMillis(25))) {
            coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {});
        }
        var selected = f.prepared;
        switch (kind) {
            case "token" -> selected = DocumentOperationUploadAdmission.prepare(f.command, f.placements,
                    Map.of("member", f.attempt), LEASE, Map.of("member", UUID.randomUUID()));
            case "expired" -> {
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                while (!tx.readOnly(em -> (Boolean) em.createNativeQuery(
                        "SELECT lease_until<=clock_timestamp() FROM document_part_attempts WHERE attempt_id=:id")
                        .setParameter("id", f.attempt).getSingleResult())) {
                    if (System.nanoTime() >= deadline) throw new AssertionError("Upload lease did not expire");
                    Thread.sleep(25);
                }
            }
            case "displaced" -> admission.retry(ADMIN, f.owner,
                    DocumentOperationUploadAdmission.prepare(f.command, f.placements, Map.of("member", UUID.randomUUID()), LEASE),
                    Map.of("member", new DocumentOperationSelection.Expected(1, f.attempt)));
            case "command" -> {
                var original = f.command.intent().getMembers(0);
                var changed = new DocumentPublicationCommand(f.command.intent().toBuilder().setMembers(0,
                        original.toBuilder().setParts(0, original.getParts(0).toBuilder().setUpload(
                                original.getParts(0).getUpload().toBuilder().setSha256("a".repeat(64))))).build());
                selected = DocumentOperationUploadAdmission.prepare(changed, f.placements, Map.of("member", f.attempt), LEASE,
                        f.prepared.uploadTokens());
            }
            default -> { }
        }
        var retry = selected;
        assertThatThrownBy(() -> admission.admitOrReuseVerified(ADMIN, f.owner, retry))
                .isInstanceOf(kind.equals("command") ? RepositoryOperationLedger.CommandConflictException.class
                        : DocumentPartAttemptLedger.FenceException.class);
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt)).isPresent();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void preparationKeepsOwnerHeartbeatAndBorrowedBytesUntilCallbackEnds(boolean upload) throws Exception {
        var base = fixture(1, LEASE);
        var member = base.command.intent().getMembers(0);
        if (!upload) {
            var source = retain(base, List.of(0));
            var measured = source.stored().parts().getFirst();
            String physical = tx.readOnly(em -> em.createNativeQuery(
                    "SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:id")
                    .setParameter("id", base.attempt).getSingleResult().toString());
            var identity = PublicationObjectIdentity.newBuilder().setObjectId(physical).setBackendGeneration(GENERATION)
                    .setStorageRealm(profile.storageRealm()).setNamespace(NAMESPACE).setObjectKey(measured.key())
                    .setSizeBytes(measured.size()).setSha256(measured.sha256()).setContentType(member.getParts(0).getUpload().getContentType())
                    .setProviderVersion(measured.providerVersion());
            var condition = member.getDestination().toBuilder().clearIfAbsent()
                    .setExpectedMutationRevision(source.published().mutationRevision).build();
            member = member.toBuilder().setDestination(condition).setParts(0, member.getParts(0).toBuilder().clearUpload()
                    .setReuse(PublicationReuse.newBuilder().setSource(condition).setSourceSlot(member.getParts(0).getSlot())
                            .setObject(identity))).build();
        }
        var command = new DocumentPublicationCommand(base.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .setMembers(0, member).build());
        Duration lease = Duration.ofSeconds(3);
        var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), lease).owner().orElseThrow();
        UUID attempt = UUID.randomUUID();
        var prepared = DocumentOperationUploadAdmission.prepare(command, base.placements, upload ? Map.of("member", attempt) : Map.of(), lease);
        var budget = new PayloadBudget(1024 * 1024);
        var gets = new java.util.concurrent.atomic.AtomicInteger();
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, call) -> {
            if (method.equals("getBounded")) gets.incrementAndGet();
            if (method.equals("put")) puts.incrementAndGet();
            return call.call();
        });
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var expiredView = new java.util.concurrent.atomic.AtomicReference<DocumentUploadPayloads.View>();
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25));
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> coordinator.stageAndPrepare(ADMIN, owner, prepared,
                    upload ? base.bodies : Map.of(), Map.of(), () -> {}, (staged, bytes, active) -> {
                        assertThat(staged.members()).hasSize(upload ? 1 : 0);
                        assertThat(bytes.keys()).hasSize(upload ? 1 : 0);
                        if (upload) {
                            var key = new DocumentUploadPayloads.Key("member", 0);
                            var body = bytes.bytes(key);
                            assertThat(body).isEqualTo(java.nio.ByteBuffer.wrap(base.bodies.get(key).bytes()));
                            assertThatThrownBy(() -> body.put(0, (byte) 1)).isInstanceOf(java.nio.ReadOnlyBufferException.class);
                            assertThat(budget.reservedBytes()).isEqualTo(2L * body.remaining());
                        }
                        expiredView.set(bytes); entered.countDown();
                        try {
                            if (!release.await(8, TimeUnit.SECONDS)) throw new IllegalStateException("Preparation gate timed out");
                        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                        active.run();
                        return "prepared";
                    }));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var before = tx.readOnly(em -> (java.math.BigDecimal) em.createNativeQuery(
                        "SELECT extract(epoch FROM lease_until) FROM repository_operation_owners WHERE operation_id=:id")
                        .setParameter("id", command.operationId()).getSingleResult());
                var attemptBefore = upload ? tx.readOnly(em -> (java.math.BigDecimal) em.createNativeQuery(
                        "SELECT extract(epoch FROM lease_until) FROM document_part_attempts WHERE attempt_id=:id")
                        .setParameter("id", attempt).getSingleResult()) : null;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                boolean renewed;
                do {
                    renewed = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT extract(epoch FROM lease_until)>:before FROM repository_operation_owners WHERE operation_id=:id")
                            .setParameter("before", before).setParameter("id", command.operationId()).getSingleResult());
                    if (renewed && upload) renewed = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT extract(epoch FROM lease_until)>:before FROM document_part_attempts WHERE attempt_id=:id")
                            .setParameter("before", attemptBefore).setParameter("id", attempt).getSingleResult());
                    if (renewed || pending.isDone()) break;
                    Thread.sleep(10);
                } while (System.nanoTime() < deadline);
                assertThat(renewed).as("owner heartbeat continues during preparation, including zero uploads").isTrue();
            } finally { release.countDown(); }
            assertThat(pending.get(5, TimeUnit.SECONDS)).isEqualTo("prepared");
        }
        assertThatThrownBy(() -> expiredView.get().keys()).hasMessageContaining("closed");
        assertThat(gets.get()).isEqualTo(upload ? 1 : 0);
        assertThat(puts.get()).isEqualTo(upload ? 1 : 0);
        assertThat(budget.reservedBytes()).isZero();
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_revision_current WHERE node_id=:id")
                .setParameter("id", prepared.members().getFirst().nodeId()).getSingleResult()).longValue())).isEqualTo(upload ? 0 : 1);
    }

    @Test void preparationRefusesASelectionReplacedDuringItsCallback() {
        var f = fixture(1, LEASE); var budget = new PayloadBudget(1024 * 1024);
        UUID next = UUID.randomUUID();
        var replacement = DocumentOperationUploadAdmission.prepare(f.command, f.placements, Map.of("member", next), LEASE);
        try (var coordinator = coordinator(opened.store(), budget, Duration.ofMillis(25))) {
            assertThatThrownBy(() -> coordinator.stageAndPrepare(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {},
                    (staged, bytes, active) -> {
                        admission.retry(ADMIN, f.owner, replacement,
                                Map.of("member", new DocumentOperationSelection.Expected(1, f.attempt)));
                        return "must not escape";
                    })).isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
        }
        assertThat(budget.reservedBytes()).isZero();
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT selection_revision FROM document_operation_selection_current WHERE operation_id=:id")
                .setParameter("id", f.command.operationId()).getSingleResult()).longValue())).isEqualTo(2);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"callback", "cancel", "owner"})
    void preparationFailureCannotReturnSuccessOrLeakItsReservation(String failure) {
        var f = fixture(1, LEASE); var budget = new PayloadBudget(1024 * 1024);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var view = new java.util.concurrent.atomic.AtomicReference<DocumentUploadPayloads.View>();
        try (var coordinator = coordinator(opened.store(), budget, Duration.ofMillis(25))) {
            assertThatThrownBy(() -> coordinator.stageAndPrepare(ADMIN, f.owner, f.prepared, f.bodies, Map.of(),
                    () -> { if (cancelled.get()) throw new IllegalStateException("test cancellation"); }, (staged, bytes, active) -> {
                        view.set(bytes);
                        if (failure.equals("callback")) throw new IllegalStateException("test callback failure");
                        if (failure.equals("cancel")) cancelled.set(true);
                        if (failure.equals("owner")) tx.inTransaction(em -> {
                            // Test-only expiry injection: normal renewals correctly reject shortening a lease.
                            em.createNativeQuery("SET LOCAL session_replication_role='replica'").executeUpdate();
                            em.createNativeQuery("UPDATE repository_operation_owners SET lease_until=clock_timestamp()-interval '1 second' WHERE operation_id=:id")
                                    .setParameter("id", f.command.operationId()).executeUpdate();
                        });
                        return "must not escape";
                    })).isInstanceOf(RuntimeException.class);
        }
        assertThat(view.get()).isNotNull();
        assertThatThrownBy(() -> view.get().keys()).hasMessageContaining("closed");
        assertThat(budget.reservedBytes()).isZero();
        assertThat(verified(f)).isEqualTo(1);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"success,false", "cancel,false", "owner,false", "cleanup,false",
            "success,true", "cancel,true", "owner,true", "cleanup,true", "delivery,true"})
    void ownedPreparationTransfersOnlyAfterPostChecksAndDraining(String outcome, boolean explicitAuthority) throws Exception {
        var f = fixture(1, LEASE);
        var budget = new PayloadBudget(1024 * 1024);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var cancelledFailure = new java.util.concurrent.CancellationException("cancel after owned preparation");
        var cleanupFailure = new java.io.IOException("candidate cleanup failed");
        var deliveryFailure = new IllegalStateException("injected delivery denial after preparation");
        Runnable control = () -> { if (cancelled.get()) throw cancelledFailure; };
        DocumentUploadCoordinator.Preparation<AutoCloseable> preparation = (staged, bytes, active) -> {
            assertThat(bytes.keys()).hasSize(1);
            var lease = budget.reserve(19);
            try {
                if (outcome.equals("owner")) tx.inTransaction(em -> {
                    // Force expiration after the callback without waiting on wall-clock lease duration.
                    em.createNativeQuery("SET LOCAL session_replication_role='replica'").executeUpdate();
                    em.createNativeQuery("UPDATE repository_operation_owners SET lease_until=clock_timestamp()-interval '1 second' WHERE operation_id=:id")
                            .setParameter("id", f.command.operationId()).executeUpdate();
                });
                if (outcome.equals("cancel") || outcome.equals("cleanup")) cancelled.set(true);
                return () -> {
                    closes.incrementAndGet();
                    lease.close();
                    if (outcome.equals("cleanup")) throw cleanupFailure;
                };
            } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
            }
        };
        try (var coordinator = coordinator(opened.store(), budget, Duration.ofMillis(25))) {
            var authority = new DocumentUploadAuthority() {
                public DocumentOperationUploadAdmission.Admission admit() {
                    return admission.admitOrReuseVerified(ADMIN, f.owner, f.prepared);
                }
                public void renewOwnerAndSelections(List<DocumentSelectedAttemptLedger.Selected> selections) {
                    var renewal = selections.isEmpty() ? null : DocumentSelectedAttemptLedger.prepareRenewal(selections, LEASE);
                    tx.inTransaction(em -> { DocumentSelectedAttemptLedger.renewOwnerAndSelections(em, f.owner, renewal, LEASE); });
                }
                public List<DocumentPartAttemptLedger.Attempt> renewSelections(List<DocumentSelectedAttemptLedger.Selected> selections) {
                    var renewal = DocumentSelectedAttemptLedger.prepareRenewal(selections, LEASE);
                    return tx.inTransaction(em -> { return DocumentSelectedAttemptLedger.renew(em, f.owner, renewal); });
                }
                public void verify(DocumentSelectedAttemptLedger.Selected selection, List<DocumentSelectedAttemptLedger.Observation> rows) {
                    var encoded = DocumentSelectedAttemptLedger.prepareVerification(rows);
                    tx.inTransaction(em -> { DocumentSelectedAttemptLedger.verifyBatch(em, f.owner, selection, encoded); });
                }
                public void recheckPreparation(List<DocumentSelectedAttemptLedger.Selected> selections, Runnable active) {
                    active.run();
                    tx.inTransaction(em -> {
                        RepositoryOperationLedger.fenceLiveOwner(em, f.owner);
                        DocumentOperationUploadAdmission.requireInitialSelections(em, f.owner, f.prepared);
                    });
                    active.run();
                }
                public void afterDrain() {
                    assertThat(coordinator.providerActivity().active()).isZero();
                    control.run();
                    if (outcome.equals("delivery")) throw deliveryFailure;
                }
            };
            java.util.function.Supplier<AutoCloseable> run = () -> explicitAuthority
                    ? coordinator.stageAndPrepareAuthorizedOwned(f.prepared, f.bodies, Map.of(), control, authority, preparation)
                    : coordinator.stageAndPrepareOwned(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), control, preparation);
            if (outcome.equals("success")) {
                try (var candidate = run.get()) {
                    assertThat(closes).hasValue(0);
                    assertThat(budget.reservedBytes()).isEqualTo(19); // All upload workers/views have drained.
                }
            } else {
                var caught = catchThrowable(run::get);
                if (outcome.equals("owner")) assertThat(caught).isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                else if (outcome.equals("delivery")) assertThat(caught).isSameAs(deliveryFailure);
                else assertThat(caught).isSameAs(cancelledFailure);
                if (outcome.equals("cleanup")) assertThat(caught.getSuppressed()).containsExactly(cleanupFailure);
                else assertThat(caught.getSuppressed()).isEmpty();
            }
        }
        assertThat(closes).hasValue(1);
        assertThat(budget.reservedBytes()).isZero();
        assertThat(verified(f)).isEqualTo(1);
    }

    @Test void stages257RealObjectsAndDrainsTailWithoutPublishing() throws Exception {
        var f = fixture(257, LEASE);
        var budget = new PayloadBudget(16 * 1024 * 1024);
        try (var coordinator = coordinator(opened.store(), budget, Duration.ofSeconds(1))) {
            var result = coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {});
            assertThat(result.attempts()).singleElement().satisfies(a -> {
                assertThat(a.id()).isEqualTo(f.attempt);
                assertThat(a.state()).isEqualTo("VERIFIED");
                assertThat(a.plannedCount()).isEqualTo(257);
            });
            assertThat(verified(f)).isEqualTo(257);
            assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_revision_current WHERE node_id=:id")
                    .setParameter("id", ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(
                            f.command.intent().getMembers(0).getDestination().getAddress())).getSingleResult()).longValue())).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void finiteAgeFlushesOneObservationWhileAnotherPutWaitsAndHeartbeatKeepsLeaseLive() throws Exception {
        var f = fixture(2, Duration.ofSeconds(1));
        var waiting = new CountDownLatch(1); var release = new CountDownLatch(1);
        var store = intercept((method, args, call) -> {
            if (method.equals("put") && ((BlobStore.PutSpec) args[0]).key().endsWith("part-1")) {
                waiting.countDown();
                if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("PUT gate timed out");
            }
            return call.call();
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25));
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}));
            try {
                assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
                awaitVerified(f, 1);
                assertThat(pending.isDone()).isFalse();
                assertThat(budget.reservedBytes()).isPositive();
                Thread.sleep(1600); // Exceeds the original one-second attempt lease while provider I/O stays blocked.
                assertThat(tx.<Boolean>readOnly(em -> (Boolean) em.createNativeQuery(
                        "SELECT lease_until>clock_timestamp() FROM document_part_attempts WHERE attempt_id=:id")
                        .setParameter("id", f.attempt).getSingleResult())).isTrue();
                release.countDown();
                assertThat(pending.get(10, TimeUnit.SECONDS).attempts().getFirst().state()).isEqualTo("VERIFIED");
            } finally { release.countDown(); }
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"put", "getBounded"})
    void stoppingProviderStartsAllowsAlreadyPermittedTransferToVerify(String heldMethod) throws Exception {
        var f = fixture(1, LEASE);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, call) -> {
            if (method.equals("put")) puts.incrementAndGet();
            var result = call.call();
            if (method.equals(heldMethod)) {
                entered.countDown();
                if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("provider gate timed out");
            }
            return result;
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25));
                var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            assertThatThrownBy(() -> coordinator.awaitProviderIdle(Duration.ZERO)).hasMessageContaining("Stop provider starts");
            var pending = workers.submit(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                coordinator.stopProviderStarts();
                coordinator.stopProviderStarts();
                assertThat(coordinator.providerActivity().accepting()).isFalse();
                assertThat(coordinator.providerActivity().active()).isEqualTo(1);
                assertThat(coordinator.awaitProviderIdle(Duration.ofMillis(50))).isFalse();
                assertThat(budget.reservedBytes()).isPositive();
                release.countDown();
                assertThat(pending.get(10, TimeUnit.SECONDS).attempts().getFirst().state()).isEqualTo("VERIFIED");
                assertThat(coordinator.awaitProviderIdle(Duration.ofSeconds(1))).isTrue();
                assertThat(coordinator.providerActivity().active()).isZero();
                assertThat(coordinator.providerActivity().refused()).isZero();
                assertThat(puts).hasValue(1);
                assertThat(verified(f)).isEqualTo(1);
                assertThat(budget.reservedBytes()).isZero();
            } finally { release.countDown(); }
        }
    }

    @Test void queuedRefusalDoesNotCancelPermittedSiblingOrDiscardItsObservation() throws Exception {
        var f = fixture(3, LEASE);
        var entered = new CountDownLatch(2);
        var firstRelease = new CountDownLatch(1); var secondRelease = new CountDownLatch(1);
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, call) -> {
            int ordinal = method.equals("put") ? puts.incrementAndGet() : 0;
            var result = call.call();
            if (ordinal > 0 && ordinal <= 2) {
                entered.countDown();
                if (!(ordinal == 1 ? firstRelease : secondRelease).await(15, TimeUnit.SECONDS))
                    throw new IllegalStateException("provider gate timed out");
            }
            return result;
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25), 2);
                var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = workers.submit(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                coordinator.stopProviderStarts();
                secondRelease.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (coordinator.providerActivity().refused() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
                assertThat(coordinator.providerActivity().refused()).isEqualTo(1);
                assertThat(coordinator.providerActivity().active()).isEqualTo(1);
                assertThat(coordinator.awaitProviderIdle(Duration.ZERO)).isFalse();
                assertThat(pending.isDone()).isFalse();
                assertThat(budget.reservedBytes()).isPositive();
                firstRelease.countDown();
                assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS)).hasStackTraceContaining("incomplete attempts require reconciliation");
                assertThat(puts).hasValue(2);
                assertThat(verified(f)).isEqualTo(2);
                assertThat(coordinator.awaitProviderIdle(Duration.ofSeconds(1))).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            } finally { firstRelease.countDown(); secondRelease.countDown(); }
        }
    }

    @Test void callerInterruptionPreventsVerificationAndRetainsBytesUntilRealPutDrains() throws Exception {
        var f = fixture(1, LEASE);
        var committed = new CountDownLatch(1); var release = new CountDownLatch(1);
        var store = intercept((method, args, call) -> {
            var result = call.call();
            if (method.equals("put")) {
                committed.countDown();
                if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("PUT gate timed out");
            }
            return result;
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25))) {
            var outcome = new java.util.concurrent.CompletableFuture<Throwable>();
            Thread caller = Thread.ofVirtual().start(() -> {
                try {
                    coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {});
                    outcome.complete(null);
                } catch (Throwable failed) { outcome.complete(failed); }
            });
            try {
                assertThat(committed.await(5, TimeUnit.SECONDS)).isTrue();
                caller.interrupt();
                coordinator.close();
                assertThat(coordinator.awaitIdle(Duration.ofMillis(50))).isFalse();
                assertThat(outcome.isDone()).isFalse();
                assertThat(budget.reservedBytes()).isPositive();
                release.countDown();
                assertThat(outcome.get(10, TimeUnit.SECONDS)).isNotNull();
                caller.join(5000);
                assertThat(caller.isAlive()).isFalse();
                assertThat(verified(f)).isZero();
                assertThat(coordinator.awaitIdle(Duration.ofSeconds(1))).isTrue();
                assertThat(budget.reservedBytes()).isZero();
                // Borrowed adapter is still open; cancellation did not erase the committed object.
                var plan = DocumentUploadPlan.prepare(f.command, f.placements, Map.of("member", f.attempt));
                var key = plan.members().getFirst().attempt().orElseThrow().uploads().getFirst().object().objectKey();
                assertThat(opened.store().get(NAMESPACE, key).data()).isNotEmpty();
            } finally { release.countDown(); caller.join(5000); }
        }
    }

    @Test void lostAcknowledgmentRemainsFailureWithoutAutomaticPutRetry() throws Exception {
        var f = fixture(1, LEASE);
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var fault = new IllegalStateException("Injected lost PUT acknowledgement");
        var store = intercept((method, args, call) -> {
            var result = call.call();
            if (method.equals("put")) { puts.incrementAndGet(); throw fault; }
            return result;
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25))) {
            assertThatThrownBy(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}))
                    .hasRootCause(fault);
            assertThat(puts.get()).isEqualTo(1);
            assertThat(verified(f)).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void explicitRetryBindsNewAttemptAndRejectsWrongExpectedSelectionBeforePut() throws Exception {
        var f = fixture(2, LEASE);
        admission.admit(ADMIN, f.owner, f.prepared);
        var next = UUID.randomUUID();
        var prepared = DocumentOperationUploadAdmission.prepare(f.command, f.placements, Map.of("member", next), LEASE);
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, call) -> {
            if (method.equals("put")) puts.incrementAndGet();
            return call.call();
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25))) {
            assertThatThrownBy(() -> coordinator.retry(ADMIN, f.owner, prepared, f.bodies, Map.of(), () -> {},
                    Map.of("member", new DocumentOperationSelection.Expected(1, UUID.randomUUID()))))
                    .isInstanceOf(RuntimeException.class);
            assertThat(puts.get()).isZero();
            assertThat(budget.reservedBytes()).isZero();
            var result = coordinator.retry(ADMIN, f.owner, prepared, f.bodies, Map.of(), () -> {},
                    Map.of("member", new DocumentOperationSelection.Expected(1, f.attempt)));
            assertThat(result.attempts()).singleElement().satisfies(a -> {
                assertThat(a.id()).isEqualTo(next);
                assertThat(a.state()).isEqualTo("VERIFIED");
            });
            assertThat(puts.get()).isEqualTo(2);
            assertThat(verified(f)).isZero();
            assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT selection_revision FROM document_operation_selection_current WHERE operation_id=:id")
                    .setParameter("id", f.command.operationId()).getSingleResult()).longValue())).isEqualTo(2);
        }
    }

    @Test void unsupportedBackendFailsBeforeAdmissionAndReleasesPayloadReservation() {
        var f = fixture(1, LEASE);
        var budget = new PayloadBudget(1024 * 1024);
        var noCapabilities = new OpenedBlobStore(opened.store(), () -> {});
        try (var coordinator = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget,
                (generation, retained) -> new DocumentUploadCoordinator.Backend(profile.identity(), noCapabilities), 4, Duration.ofMillis(25), SQL_LIMITS)) {
            assertThatThrownBy(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}))
                    .hasMessageContaining("capabilities");
            assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_part_attempts WHERE attempt_id=:id")
                    .setParameter("id", f.attempt).getSingleResult()).longValue())).isZero();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void admissionLockTimeoutReturnsWhileBlockerStillOwnsRow() throws Exception {
        var f = fixture(1, LEASE);
        var budget = new PayloadBudget(1024 * 1024);
        try (var blocker = database.entityManagerFactory().createEntityManager();
                var coordinator = coordinator(opened.store(), budget, Duration.ofMillis(25));
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.getTransaction().begin();
            try {
                blocker.createNativeQuery("SELECT operation_id FROM repository_operation_owners WHERE operation_id=:id FOR UPDATE")
                        .setParameter("id", f.command.operationId()).getSingleResult();
                var pending = executor.submit(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}));
                assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS)).hasStackTraceContaining("lock timeout");
                assertThat(budget.reservedBytes()).isZero();
                assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_part_attempts WHERE attempt_id=:id")
                        .setParameter("id", f.attempt).getSingleResult()).longValue())).isZero();
            } finally { blocker.getTransaction().rollback(); }
        }
    }

    @Test void flusherLockTimeoutReturnsFailureAfterRealUploadWithoutFalseVerification() throws Exception {
        var f = fixture(1, LEASE);
        var read = new CountDownLatch(1); var release = new CountDownLatch(1);
        var store = intercept((method, args, call) -> {
            var result = call.call();
            if (method.equals("getBounded")) {
                read.countDown();
                if (!release.await(15, TimeUnit.SECONDS)) throw new IllegalStateException("Read gate timed out");
            }
            return result;
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var blocker = database.entityManagerFactory().createEntityManager();
                var coordinator = coordinator(store, budget, Duration.ofMillis(25));
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}));
            try {
                assertThat(read.await(5, TimeUnit.SECONDS)).isTrue();
                blocker.getTransaction().begin();
                blocker.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                        .setParameter("id", f.attempt).getSingleResult();
                release.countDown();
                assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS)).hasStackTraceContaining("lock timeout");
                assertThat(verified(f)).isZero();
                assertThat(budget.reservedBytes()).isZero();
            } finally {
                release.countDown();
                if (blocker.getTransaction().isActive()) blocker.getTransaction().rollback();
            }
        }
    }

    @Test void sixtyFourMembersShareOneQueueAndResolveTheirBackendOnce() {
        var base = fixture(4, LEASE);
        var template = base.command.intent().getMembers(0);
        var intent = base.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).clearMembers();
        var attempts = new java.util.HashMap<String, UUID>();
        var bodies = new java.util.HashMap<DocumentUploadPayloads.Key, PartObject>();
        for (int i = 0; i < 64; i++) {
            String id = "member-" + i;
            attempts.put(id, UUID.randomUUID());
            intent.addMembers(template.toBuilder().setMemberId(id).setDestination(template.getDestination().toBuilder()
                    .setAddress(template.getDestination().getAddress().toBuilder().setDocId(UUID.randomUUID().toString()))));
            for (int part = 0; part < 4; part++) bodies.put(new DocumentUploadPayloads.Key(id, part),
                    base.bodies.get(new DocumentUploadPayloads.Key("member", part)));
        }
        var command = new DocumentPublicationCommand(intent.build());
        var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        var prepared = DocumentOperationUploadAdmission.prepare(command, base.placements, attempts, LEASE);
        var resolutions = new java.util.concurrent.atomic.AtomicInteger();
        var budget = new PayloadBudget(16 * 1024 * 1024);
        try (var coordinator = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, retained) -> {
            resolutions.incrementAndGet();
            return new DocumentUploadCoordinator.Backend(profile.identity(), opened);
        }, 8, Duration.ofSeconds(1), SQL_LIMITS)) {
            var result = coordinator.stage(ADMIN, owner, prepared, bodies, Map.of(), () -> {});
            assertThat(result.attempts()).hasSize(64).allSatisfy(a -> {
                assertThat(a.state()).isEqualTo("VERIFIED");
                assertThat(a.plannedCount()).isEqualTo(4);
            });
            assertThat(result.members()).allSatisfy(member -> {
                assertThat(member.selection().attempt()).isEqualTo(attempts.get(member.selection().member()));
                assertThat(member.selection().attempt()).isEqualTo(member.attempt().id());
                assertThat(member.selection().token()).isEqualTo(member.attempt().token());
            });
            assertThat(resolutions.get()).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void oneOperationStagesAcrossS3AndRedisWithExactBackendBindings() throws Exception {
        var base = fixture(2, LEASE);
        String redisGeneration = "redis-" + UUID.randomUUID();
        var options = Map.of("uri", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                "ttl-seconds", "0", "max-object-bytes", "1048576", "key-prefix", redisGeneration);
        var redisProvider = new ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider();
        var redisProfile = new ManagedBackendLedger.Profile(redisProvider.managedIdentity(options), "redis-test-realm");
        new ManagedBackendLedger(tx).bind(redisGeneration, redisProfile);
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = redisGeneration; drive.driveType = "CUSTOM"; drive.provider = "redis"; drive.bucket = NAMESPACE;
        new DriveLedger(tx).insert(drive);
        var redisMember = base.command.intent().getMembers(0).toBuilder().setMemberId("redis-member")
                .setDriveId(drive.driveId.toString()).setDestination(base.command.intent().getMembers(0).getDestination()
                        .toBuilder().setAddress(base.command.intent().getMembers(0).getDestination().getAddress()
                                .toBuilder().setDocId(UUID.randomUUID().toString()))).build();
        var command = new DocumentPublicationCommand(base.command.intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).addMembers(redisMember).build());
        var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        var placements = new java.util.HashMap<>(base.placements);
        placements.put(drive.driveId, DocumentUploadPlan.Placement.sample(drive, redisGeneration, redisProfile));
        var attempts = Map.of("member", UUID.randomUUID(), "redis-member", UUID.randomUUID());
        var prepared = DocumentOperationUploadAdmission.prepare(command, placements, attempts, LEASE);
        var bodies = new java.util.HashMap<>(base.bodies);
        base.bodies.forEach((key, value) -> bodies.put(new DocumentUploadPayloads.Key("redis-member", key.revisionOrdinal()), value));
        var budget = new PayloadBudget(1024 * 1024);
        var resolutions = new java.util.HashMap<String, Integer>();
        try (var redis = redisProvider.open(options)) {
            // This qualifies transfer behavior only, not persistence across Redis restart or eviction.
            try (var wrong = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget,
                    (generation, retained) -> new DocumentUploadCoordinator.Backend(profile.identity(), opened),
                    4, Duration.ofMillis(25), SQL_LIMITS)) {
                assertThatThrownBy(() -> wrong.stage(ADMIN, owner, prepared, bodies, Map.of(), () -> {}))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("identity");
                assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_operation_selection_current WHERE operation_id=:id")
                        .setParameter("id", command.operationId()).getSingleResult()).longValue())).isZero();
                for (UUID attempt : attempts.values()) {
                    assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_part_attempts WHERE attempt_id=:id")
                            .setParameter("id", attempt).getSingleResult()).longValue())).isZero();
                }
                assertThat(budget.reservedBytes()).isZero();
            }
            try (var coordinator = new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, retained) -> {
                resolutions.merge(generation, 1, Integer::sum);
                if (generation.equals(GENERATION)) {
                    assertThat(retained).isEqualTo(profile);
                    return new DocumentUploadCoordinator.Backend(profile.identity(), opened);
                }
                assertThat(generation).isEqualTo(redisGeneration);
                assertThat(retained).isEqualTo(redisProfile);
                return new DocumentUploadCoordinator.Backend(redisProfile.identity(), redis);
            }, 4, Duration.ofMillis(25), SQL_LIMITS)) {
                var staged = coordinator.stage(ADMIN, owner, prepared, bodies, Map.of(), () -> {});
                assertThat(staged.members()).hasSize(2).allSatisfy(member -> {
                    assertThat(member.attempt().state()).isEqualTo("VERIFIED");
                    assertThat(member.attempt().id()).isEqualTo(attempts.get(member.selection().member()));
                });
                assertThat(resolutions).containsExactlyInAnyOrderEntriesOf(Map.of(GENERATION, 1, redisGeneration, 1));
                assertThat(budget.reservedBytes()).isZero();
                for (var member : DocumentUploadPlan.prepare(command, placements, attempts).members()) {
                    boolean onRedis = member.intent().getMemberId().equals("redis-member");
                    var actual = onRedis ? redis.store() : opened.store();
                    var other = onRedis ? opened.store() : redis.store();
                    for (var upload : member.attempt().orElseThrow().uploads()) {
                        String key = upload.object().objectKey();
                        var expected = bodies.get(new DocumentUploadPayloads.Key(member.intent().getMemberId(), upload.revisionOrdinal()));
                        assertThat(actual.get(NAMESPACE, key).data()).containsExactly(expected.bytes());
                        assertThatThrownBy(() -> other.get(NAMESPACE, key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
                    }
                }
            }
        }
    }

    @Test @Timeout(45) void sqlFailureReleasesAProducerWaitingOnAFullObservationQueue() throws Exception {
        var f = fixture(513, LEASE);
        var admitted = admission.admit(ADMIN, f.owner, f.prepared).getFirst();
        var selection = new DocumentSelectedAttemptLedger.Selected("member", 1, admitted.id(), admitted.token());
        var uploads = DocumentUploadPlan.prepare(f.command, f.placements, Map.of("member", f.attempt))
                .members().getFirst().attempt().orElseThrow().uploads();
        var observations = DocumentPartWorkers.run(uploads.size(), 8, new java.util.concurrent.Semaphore(8), () -> {}, (index, check) -> {
            var upload = uploads.get(index);
            return DocumentPartTransfer.upload(opened.store(), NAMESPACE, upload.object(),
                    f.bodies.get(new DocumentUploadPayloads.Key("member", upload.revisionOrdinal())).bytes(), Map.of(), check, check);
        });
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var extraProducer = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var producerChecks = new java.util.concurrent.atomic.AtomicInteger();
        var retriedOffer = new CountDownLatch(1);
        Runnable check = () -> {
            if (failure.get() != null) throw new IllegalStateException("Observation verification failed", failure.get());
            if (Thread.currentThread() == extraProducer.get() && producerChecks.incrementAndGet() >= 2) retriedOffer.countDown();
        };
        var flusher = new DocumentObservationFlusher(new DocumentSelectedAttemptLedger(tx.withTimeouts(
                new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(10)))), f.owner, List.of(selection),
                Duration.ofMinutes(1), check, cause -> failure.compareAndSet(null, cause));
        // A long age isolates aggregate-pressure flushing from age-based flushing.
        for (int i = 0; i < 256; i++) flusher.add(selection, observations.get(i));
        try (var blocker = database.entityManagerFactory().createEntityManager();
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.getTransaction().begin();
            try {
                int blockerPid = ((Number) blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                blocker.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                        .setParameter("id", f.attempt).getSingleResult();
                var flushing = executor.submit(flusher::run);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                boolean waiting;
                do {
                    waiting = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid)))")
                            .setParameter("pid", blockerPid).getSingleResult());
                    if (!waiting) Thread.sleep(10);
                } while (!waiting && System.nanoTime() < until);
                assertThat(waiting).as("flusher blocked in real SQL after draining first batch").isTrue();
                for (int i = 256; i < 512; i++) flusher.add(selection, observations.get(i));
                var producing = executor.submit(() -> {
                    extraProducer.set(Thread.currentThread());
                    flusher.add(selection, observations.get(512));
                });
                assertThat(retriedOffer.await(1, TimeUnit.SECONDS)).as("producer retried a full-queue offer").isTrue();
                assertThat(producing.isDone()).isFalse();
                assertThatThrownBy(() -> flushing.get(8, TimeUnit.SECONDS)).hasStackTraceContaining("lock timeout");
                assertThatThrownBy(() -> producing.get(2, TimeUnit.SECONDS)).hasStackTraceContaining("lock timeout");
                assertThat(verified(f)).isZero();
                // The failed SQL transaction did not erase already written provider bytes.
                assertThat(opened.store().get(NAMESPACE, uploads.get(512).object().objectKey()).data())
                        .containsExactly(f.bodies.get(new DocumentUploadPayloads.Key("member", 512)).bytes());
            } finally {
                failure.compareAndSet(null, new java.util.concurrent.CancellationException("Test cleanup"));
                flusher.finish();
                blocker.getTransaction().rollback();
            }
        }
    }

    private record RetainedSource(DocumentRecord published, DocumentManifest manifest, DriveRecord drive,
            DocumentPartAttemptLedger.PlannedObject core, DocumentPartStager.Staged stored) {}

    /** Real staged bytes and guarded SQL publication; not semantic/typed admission evidence. */
    private static RetainedSource retain(Fixture base, List<Integer> ordinals) throws Exception {
        var original = base.command.intent().getMembers(0);
        var address = original.getDestination().getAddress();
        var drive = new DriveLedger(tx).findById(UUID.fromString(original.getDriveId())).orElseThrow();
        var planned = DocumentUploadPlan.prepare(base.command, base.placements, Map.of("member", base.attempt))
                .members().getFirst().attempt().orElseThrow();
        var core = planned.uploads().getFirst().object();
        var objects = ordinals.stream().map(i -> planned.uploads().get(i).object()).toList();
        var fullPlan = new DocumentPartAttemptLedger.Plan(base.attempt, planned.location(), 0, Map.of(), objects);
        DocumentPartStager.Staged stored;
        try (var stager = new DocumentPartStager(tx, GENERATION, profile.identity(), opened)) {
            stored = stager.stage(fullPlan, ordinals.stream()
                    .map(i -> base.bodies.get(new DocumentUploadPayloads.Key("member", i))).toList(), LEASE, Map.of());
        }
        var measured = stored.parts().getFirst();
        assertThat(measured.providerVersion()).isNotBlank().isNotEqualTo("null");
        var manifestBuilder = DocumentManifest.newBuilder().setAddress(address).setDocVersion(1);
        for (var object : objects) manifestBuilder.addParts(PartManifestEntry.newBuilder()
                .setPart(object.part()).setSubKey(object.subKey()).setState(PartState.PART_STATE_PRESENT)
                .setObjectKey(object.objectKey()).setSizeBytes(object.size()).setSha256(object.sha256()));
        var manifest = manifestBuilder.build();
        var row = new DocumentRecord(); row.nodeId = planned.location().nodeId(); row.accountId = "account";
        row.docId = address.getDocId(); row.graphId = address.getGraphId(); row.graphAddressId = address.getGraphAddressId();
        row.rowKind = DocumentRowKind.PIPELINE; row.datasourceId = "source"; row.driveName = drive.name;
        row.objectKey = core.objectKey(); row.versionId = measured.providerVersion(); row.etag = measured.etag();
        row.sizeBytes = objects.stream().mapToLong(DocumentPartAttemptLedger.PlannedObject::size).sum();
        row.createdAt = java.time.Instant.now(); row.updatedAt = row.createdAt;
        row.writeSecurity(DocumentSecurity.getDefaultInstance()); row.writeManifest(manifest);
        row.checksum = DocumentPartCodec.rootChecksumFromManifest(manifest);
        var published = new DocumentLedger(tx).saveVerifiedAttempt(row, null, Map.of(), base.attempt, stored.attempt().token(),
                new DocumentPublicationTarget(new DriveLedger(tx), drive, GENERATION, profile.identity()), (em, saved) -> {});
        return new RetainedSource(published, manifest, drive, core, stored);
    }

    @Test void retainedCoreIsReusedWhileOnlyChangedChunksAreUploaded() throws Exception {
        var base = fixture(2, LEASE);
        var source = retain(base, List.of(0));
        var original = base.command.intent().getMembers(0);
        var address = original.getDestination().getAddress();
        var drive = source.drive();
        var core = source.core();
        var measured = source.stored().parts().getFirst();
        var published = source.published();
        var manifest = source.manifest();
        String physical = tx.readOnly(em -> em.createNativeQuery(
                "SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:id")
                .setParameter("id", base.attempt).getSingleResult().toString());
        var identity = PublicationObjectIdentity.newBuilder().setObjectId(physical).setBackendGeneration(GENERATION)
                .setStorageRealm(profile.storageRealm()).setNamespace(NAMESPACE).setObjectKey(core.objectKey())
                .setSizeBytes(core.size()).setSha256(core.sha256()).setContentType(core.contentType())
                .setProviderVersion(measured.providerVersion()).build();
        var condition = DocumentRevisionCondition.newBuilder().setAddress(address).setExpectedMutationRevision(published.mutationRevision).build();
        var member = original.toBuilder().setDestination(condition).setParts(0, original.getParts(0).toBuilder()
                .clearUpload().setReuse(PublicationReuse.newBuilder().setSource(condition)
                        .setSourceSlot(original.getParts(0).getSlot()).setObject(identity))).build();
        var command = new DocumentPublicationCommand(base.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .clearMembers().addMembers(member).build());
        var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        UUID next = UUID.randomUUID();
        var prepared = DocumentOperationUploadAdmission.prepare(command, base.placements, Map.of("member", next), LEASE);
        var readPlan = new DocumentOperationUploadAdmission(tx, new DriveLedger(tx))
                .captureRetainedReads(ADMIN, owner, prepared);
        assertThat(readPlan.entries()).singleElement().satisfies(entry -> {
            assertThat(entry.revisionOrdinal()).isZero();
            assertThat(entry.source().getObject()).isEqualTo(identity);
            assertThat(entry.binding().profile()).isEqualTo(profile);
            var object = entry.source().getObject();
            var actual = opened.store().getBounded(entry.binding().namespace(), object.getObjectKey(),
                    object.getProviderVersion(), Math.toIntExact(object.getSizeBytes()));
            assertThat(actual.versionId()).isEqualTo(object.getProviderVersion());
            assertThat(actual.contentType()).isEqualTo(object.getContentType());
            assertThat(DocumentPartCodec.sha256Hex(actual.data())).isEqualTo(object.getSha256());
        });
        assertThat(new DocumentPartAttemptLedger(tx).find(next)).isEmpty();
        var readBudget = new PayloadBudget(core.size() * 2);
        var resolves = new java.util.concurrent.atomic.AtomicInteger();
        try (var reader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, retainedProfile) -> {
            resolves.incrementAndGet();
            assertThat(generation).isEqualTo(GENERATION);
            assertThat(retainedProfile).isEqualTo(profile);
            return opened.store();
        }, 2, 1024 * 1024, readBudget)) {
            assertThatThrownBy(() -> reader.readRetained(readPlan, "unknown",
                    ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)).isInstanceOf(IllegalArgumentException.class);
            assertThat(resolves.get()).isZero();
            assertThatThrownBy(() -> reader.readRetained(readPlan, "member", new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                public boolean isCancelled() { return true; }
                public long remainingNanos() { return Long.MAX_VALUE; }
            })).isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                    e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.CANCELLED));
            assertThat(resolves.get()).isZero();
            try (var batch = reader.readRetained(readPlan, "member", ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
                assertThat(batch.parts()).singleElement().satisfies(part -> {
                    assertThat(part.part()).isEqualTo(DocumentPart.DOCUMENT_PART_CORE);
                    assertThat(part.bytes()).containsExactly(base.bodies.get(new DocumentUploadPayloads.Key("member", 0)).bytes());
                });
                assertThat(readBudget.reservedBytes()).isEqualTo(core.size() * 2);
                assertThatThrownBy(() -> reader.readRetained(readPlan, "member", ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(resolves.get()).isEqualTo(1);
            }
            assertThat(readBudget.reservedBytes()).isZero();
        }
        var bodies = Map.of(new DocumentUploadPayloads.Key("member", 1), base.bodies.get(new DocumentUploadPayloads.Key("member", 1)));
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, call) -> {
            if (method.equals("put")) {
                puts.incrementAndGet();
                assertThat(((BlobStore.PutSpec) args[0]).key()).endsWith("part-1").isNotEqualTo(core.objectKey());
            }
            return call.call();
        });
        var budget = new PayloadBudget(1024 * 1024);
        try (var coordinator = coordinator(store, budget, Duration.ofMillis(25))) {
            assertThatThrownBy(() -> coordinator.stage(ADMIN, owner, prepared, base.bodies, Map.of(), () -> {}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(puts.get()).isZero();
            assertThat(new DocumentPartAttemptLedger(tx).find(next)).isEmpty();
            assertThat(budget.reservedBytes()).isZero();
            var result = coordinator.stage(ADMIN, owner, prepared, bodies, Map.of(), () -> {});
            assertThat(result.attempts()).singleElement().satisfies(attempt -> {
                assertThat(attempt.id()).isEqualTo(next);
                assertThat(attempt.plannedCount()).isEqualTo(1);
                assertThat(attempt.state()).isEqualTo("VERIFIED");
            });
        }
        assertThat(puts.get()).isEqualTo(1);
        assertThat(budget.reservedBytes()).isZero();
        var uploadedSlots = tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery(
                "SELECT part,sub_key,verified FROM document_part_attempt_objects WHERE attempt_id=:id", Object[].class)
                .setParameter("id", next).getResultList());
        assertThat(uploadedSlots).singleElement().satisfies(values -> {
            assertThat(((Number) values[0]).intValue()).isEqualTo(DocumentPart.DOCUMENT_PART_CHUNKS.getNumber());
            assertThat(values[1]).isEqualTo(original.getParts(1).getSlot().getSubKey());
            assertThat(values[2]).isEqualTo(true);
        });
        var retained = opened.store().get(NAMESPACE, core.objectKey(), measured.providerVersion());
        assertThat(retained.data()).containsExactly(base.bodies.get(new DocumentUploadPayloads.Key("member", 0)).bytes());
        assertThat(retained.versionId()).isEqualTo(measured.providerVersion());
        var currentCore = opened.store().get(NAMESPACE, core.objectKey());
        assertThat(currentCore.versionId()).isEqualTo(measured.providerVersion());
        assertThat(currentCore.data()).containsExactly(retained.data());
        var current = new DocumentLedger(tx).findByNodeId(published.nodeId).orElseThrow();
        assertThat(current.mutationRevision).isEqualTo(published.mutationRevision);
        assertThat(current.readManifest()).isEqualTo(manifest);
        // A captured read never substitutes today's drive or latest provider version.
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET bucket='changed-after-capture' WHERE drive_id=:id")
                .setParameter("id", drive.driveId).executeUpdate(); });
        opened.store().put(new BlobStore.PutSpec(NAMESPACE, core.objectKey(), "text/plain", Map.of(), null), new byte[] {9});
        assertThat(opened.store().get(NAMESPACE, core.objectKey()).versionId()).isNotEqualTo(measured.providerVersion());
        try (var reader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, retainedProfile) -> opened.store());
                var batch = reader.readRetained(readPlan, "member", ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
            assertThat(batch.parts().getFirst().bytes()).containsExactly(retained.data());
        }
        var wrongType = intercept((method, args, call) -> {
            var result = call.call();
            if (!method.equals("getBounded")) return result;
            var actual = (BlobStore.GetResult) result;
            return new BlobStore.GetResult(actual.data(), "text/plain", actual.eTag(), actual.versionId());
        });
        try (var reader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, retainedProfile) -> wrongType,
                2, 1024 * 1024, readBudget)) {
            assertThatThrownBy(() -> reader.readRetained(readPlan, "member", ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS));
            assertThat(readBudget.reservedBytes()).isZero();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"read", "cancel", "inputs", "inputs-close", "invalid-inputs",
            "invalid-inputs-close", "wrong-operation", "wrong-owner", "lifecycle", "lifecycle-cancel"})
    void protectedRetainedReadsKeepPinsUntilBatchesAndActualWorkersEnd(String mode) throws Exception {
        boolean cancel = mode.equals("cancel") || mode.equals("lifecycle-cancel");
        boolean managedLifecycle = mode.startsWith("lifecycle");
        var base = fixture(3, LEASE);
        var source = retain(base, List.of(0, 2));
        var original = base.command.intent().getMembers(0);
        var condition = DocumentRevisionCondition.newBuilder().setAddress(original.getDestination().getAddress())
                .setExpectedMutationRevision(source.published().mutationRevision).build();
        var reused = new java.util.ArrayList<DocumentPublicationPart>();
        for (int index : List.of(2, 0)) {
            var slot = original.getParts(index).getSlot();
            var measured = source.stored().parts().stream()
                    .filter(p -> p.part() == slot.getPart() && p.subKey().equals(slot.getSubKey())).findFirst().orElseThrow();
            String physical = tx.readOnly(em -> em.createNativeQuery(
                    "SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:id AND object_key=:key")
                    .setParameter("id", base.attempt).setParameter("key", measured.key()).getSingleResult().toString());
            var body = base.bodies.get(new DocumentUploadPayloads.Key("member", index));
            var identity = PublicationObjectIdentity.newBuilder().setObjectId(physical).setBackendGeneration(GENERATION)
                    .setStorageRealm(profile.storageRealm()).setNamespace(NAMESPACE).setObjectKey(measured.key())
                    .setProviderVersion(measured.providerVersion()).setSizeBytes(body.bytes().length)
                    .setSha256(body.sha256()).setContentType(original.getParts(index).getUpload().getContentType());
            reused.add(DocumentPublicationPart.newBuilder().setSlot(slot).setReuse(PublicationReuse.newBuilder()
                    .setSource(condition).setSourceSlot(slot).setObject(identity)).build());
        }
        var member = original.toBuilder().setDestination(condition).clearParts()
                .addParts(original.getParts(1)).addParts(reused.get(0))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_BLOBS)).setEmpty(true))
                .addParts(reused.get(1)).build();
        var command = new DocumentPublicationCommand(base.command.intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).clearMembers().addMembers(member).build());
        var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        UUID attempt = UUID.randomUUID();
        var prepared = DocumentOperationUploadAdmission.prepare(command, base.placements, Map.of("member", attempt), LEASE);
        UUID incarnation = UUID.randomUUID();
        var ledger = new DocumentReadLedger(tx, incarnation);
        var protectedPlan = ledger.capture(admission, ADMIN, owner, prepared);
        DocumentRetainedReadPlan plan;
        try (var inspection = protectedPlan.use()) { plan = inspection.plan(); }
        assertThat(plan.entries()).extracting(DocumentRetainedReadPlan.Entry::revisionOrdinal).containsExactly(1, 3);
        long size = plan.entries().stream().mapToLong(e -> e.source().getObject().getSizeBytes()).sum();
        var budget = new PayloadBudget(size * 2);
        var gets = new java.util.concurrent.atomic.AtomicInteger();
        var entered = new CountDownLatch(2); var unblock = new CountDownLatch(1);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var counted = intercept((method, args, call) -> {
            Object result = call.call();
            if (method.equals("getBounded")) {
                gets.incrementAndGet(); entered.countDown();
                if (cancel) {
                    // Actual S3 GET completed; the provider wrapper deliberately ignores cancellation.
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    boolean interrupted = false;
                    try {
                        while (true) {
                            long remaining = end - System.nanoTime();
                            if (remaining <= 0) throw new IllegalStateException("Provider gate timed out");
                            try {
                                if (!unblock.await(remaining, TimeUnit.NANOSECONDS))
                                    throw new IllegalStateException("Provider gate timed out");
                                break;
                            } catch (InterruptedException ignoredForFaultInjection) { interrupted = true; }
                        }
                    } finally { if (interrupted) Thread.currentThread().interrupt(); }
                }
            }
            return result;
        });
        try (var reader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, retainedProfile) -> counted,
                2, 1024 * 1024, budget)) {
            DocumentRetainedReader retainedReader = reader;
            var lifecycle = new DocumentReadLifecycle(ledger, reader, 2);
            if (mode.startsWith("inputs") || mode.startsWith("invalid-inputs") || mode.startsWith("wrong-")) {
                var uploadBudget = new PayloadBudget(1024 * 1024);
                var snapshotBudget = new PayloadBudget(1024 * 1024);
                var bodies = Map.of(new DocumentUploadPayloads.Key("member", 0),
                        base.bodies.get(new DocumentUploadPayloads.Key("member", 1)));
                try (var payloads = prepared.preparePayloads(java.util.Set.of("member"), bodies, uploadBudget);
                        var use = prepared.claimPayloads(payloads, java.util.Set.of("member"));
                        var view = use.view()) {
                    if (mode.startsWith("wrong-")) {
                        var another = new DocumentPublicationCommand(command.intent().toBuilder()
                                .setOperationId(UUID.randomUUID().toString()).build());
                        assertThat(another.canonical()).isEqualTo(command.canonical());
                        var anotherOwner = new RepositoryOperationLedger.Owner(owner.key(), owner.generation() + 1,
                                UUID.randomUUID(), owner.leaseUntil());
                        assertThatThrownBy(() -> {
                            try (var unexpected = DocumentPublicationInputs.capture(mode.equals("wrong-operation") ? another : command,
                                    mode.equals("wrong-owner") ? anotherOwner : owner, view, protectedPlan,
                                    retainedReader, ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
                                fail("mismatched operation or owner returned inputs");
                            }
                        }).hasMessageContaining("differs from publication command or owner");
                    } else if (mode.startsWith("invalid-inputs") || mode.equals("inputs-close")) {
                        // Corrupt only the returned batch shape after real provider reads.
                        var closeFailure = new IllegalStateException("injected retained batch close failure");
                        DocumentRetainedReader wrongCount = (pinned, id, control) -> {
                            var actual = retainedReader.readRetained(pinned, id, control);
                            return new DocumentRetainedReader.Batch() {
                                @Override public List<PartObject> parts() {
                                    return mode.equals("inputs-close") ? actual.parts() : actual.parts().subList(0, 1);
                                }
                                @Override public void close() {
                                    actual.close();
                                    if (mode.endsWith("-close")) throw closeFailure;
                                }
                            };
                        };
                        var failure = catchThrowable(() -> {
                            try (var inputs = DocumentPublicationInputs.capture(command, owner, view, protectedPlan,
                                    wrongCount, ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
                                assertThat(inputs.fragments().get("member")).containsOnlyKeys(0, 1, 3);
                            }
                        });
                        if (mode.equals("inputs-close")) assertThat(failure).isSameAs(closeFailure);
                        else {
                            assertThat(failure).hasMessageContaining("wrong part count");
                            if (mode.endsWith("-close")) assertThat(failure.getSuppressed()).containsExactly(closeFailure);
                        }
                    } else {
                        try (var inputs = DocumentPublicationInputs.capture(command, owner, view, protectedPlan,
                                retainedReader, ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE);
                                var snapshot = DocumentPublicationFragments.capture(command, inputs.fragments(), snapshotBudget, () -> {})) {
                            assertThat(inputs.fragments().get("member")).containsOnlyKeys(0, 1, 3);
                            assertThat(budget.reservedBytes()).isEqualTo(size * 2);
                            inputs.close();
                            view.close();
                            assertThat(budget.reservedBytes()).isZero();
                            var copied = snapshot.fragments().get("member");
                            assertThat(copied.get(0).toByteArray()).containsExactly(bodies.values().iterator().next().bytes());
                            assertThat(copied.get(1).toByteArray()).containsExactly(base.bodies.get(new DocumentUploadPayloads.Key("member", 2)).bytes());
                            assertThat(copied.get(3).toByteArray()).containsExactly(base.bodies.get(new DocumentUploadPayloads.Key("member", 0)).bytes());
                            assertThatThrownBy(inputs::fragments).hasMessageContaining("closed");
                        }
                    }
                }
                assertThat(uploadBudget.reservedBytes()).isZero();
                assertThat(snapshotBudget.reservedBytes()).isZero();
                assertThat(budget.reservedBytes()).isZero();
                protectedPlan.close(); ledger.fence();
            } else if (cancel) {
                try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                    var result = executor.submit(() -> retainedReader.readRetained(protectedPlan, "member",
                            new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                                @Override public boolean isCancelled() { return cancelled.get(); }
                            }));
                    try {
                        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                        cancelled.set(true);
                        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasStackTraceContaining("Document read cancelled");
                        if (managedLifecycle) assertThat(lifecycle.shutdownStep(Duration.ZERO)).isFalse();
                        else { protectedPlan.close(); ledger.fence(); }
                        assertThat(protectedPlan.awaitDrained(Duration.ZERO)).isFalse();
                        assertThatThrownBy(protectedPlan::release).isInstanceOf(IllegalStateException.class);
                        assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
                        assertThat(documentPins(incarnation)).isEqualTo(2);
                        assertThat(budget.reservedBytes()).isEqualTo(size * 2);
                    } finally { unblock.countDown(); }
                }
                reader.close();
                assertThat(reader.awaitIdle(Duration.ofSeconds(5))).isTrue();
            } else {
                assertThatThrownBy(() -> retainedReader.readRetained(protectedPlan, "unknown",
                        ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)).isInstanceOf(IllegalArgumentException.class);
                try (var batch = retainedReader.readRetained(protectedPlan, "member", ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
                    assertThat(batch.parts()).hasSize(2);
                    assertThat(batch.parts().get(0).bytes()).containsExactly(base.bodies.get(new DocumentUploadPayloads.Key("member", 2)).bytes());
                    assertThat(batch.parts().get(1).bytes()).containsExactly(base.bodies.get(new DocumentUploadPayloads.Key("member", 0)).bytes());
                    assertThatThrownBy(() -> retainedReader.readRetained(protectedPlan, "member",
                            ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE))
                            .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                    error -> assertThat(error.code()).isEqualTo(
                                            ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED));
                    if (managedLifecycle) assertThat(lifecycle.shutdownStep(Duration.ZERO)).isFalse();
                    else { protectedPlan.close(); ledger.fence(); }
                    assertThat(protectedPlan.isDrained()).isFalse();
                    assertThatThrownBy(protectedPlan::release).isInstanceOf(IllegalStateException.class);
                    assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
                    assertThat(documentPins(incarnation)).isEqualTo(2);
                    assertThat(budget.reservedBytes()).isEqualTo(size * 2);
                }
            }
            assertThat(protectedPlan.awaitDrained(Duration.ofSeconds(5))).isTrue();
            if (managedLifecycle) {
                assertThat(lifecycle.shutdownStep(Duration.ofSeconds(5))).isFalse(); // one durable batch, not yet an empty pass
                assertThat(lifecycle.shutdownStep(Duration.ofSeconds(5))).isTrue();
                assertThat(lifecycle.shutdownStep(Duration.ZERO)).isTrue();
                assertThat(ledger.outstandingReads()).isZero();
                assertThatThrownBy(lifecycle::tick).hasMessageContaining("stopping");
            } else { ledger.attestLocalQuiescence(); protectedPlan.release(); }
            assertThat(documentPins(incarnation)).isZero();
        }
        assertThat(gets.get()).isEqualTo(mode.startsWith("wrong-") ? 0 : 2);
        assertThat(budget.reservedBytes()).isZero();
        assertThat(new DocumentPartAttemptLedger(tx).find(attempt)).isEmpty();
    }

    private static long documentPins(UUID incarnation) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_read_pins WHERE reader_incarnation=:id")
                .setParameter("id", incarnation).getSingleResult()).longValue());
    }

    private static DocumentUploadCoordinator coordinator(BlobStore store, PayloadBudget budget, Duration age) {
        return coordinator(store, budget, age, 4);
    }

    private static DocumentUploadCoordinator coordinator(BlobStore store, PayloadBudget budget, Duration age, int parallelism) {
        // A fault-injecting wrapper delegates to the real adapter; the underlying handle remains borrowed.
        var borrowed = new OpenedBlobStore(store, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
        return new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, retained) -> {
            assertThat(generation).isEqualTo(GENERATION);
            assertThat(retained).isEqualTo(profile);
            return new DocumentUploadCoordinator.Backend(profile.identity(), borrowed);
        }, parallelism, age, SQL_LIMITS);
    }

    private static long verified(Fixture f) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND verified")
                .setParameter("id", f.attempt).getSingleResult()).longValue());
    }
    private static void awaitVerified(Fixture f, long expected) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (verified(f) != expected && System.nanoTime() < end) Thread.sleep(10);
        assertThat(verified(f)).isEqualTo(expected);
    }

    @FunctionalInterface private interface Invocation { Object call() throws Exception; }
    @FunctionalInterface private interface Interceptor { Object invoke(String method, Object[] args, Invocation call) throws Exception; }
    private static BlobStore intercept(Interceptor interceptor) {
        return (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class},
                (proxy, method, args) -> interceptor.invoke(method.getName(), args, () -> {
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) {
                        if (failure.getCause() instanceof Exception exception) throw exception;
                        throw (Error) failure.getCause();
                    }
                }));
    }

    private static Fixture fixture(int count, Duration lease) {
        String docId = UUID.randomUUID().toString();
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "coordinator-" + drive.driveId;
        drive.driveType = "CUSTOM"; drive.provider = "s3"; drive.bucket = NAMESPACE; new DriveLedger(tx).insert(drive);
        var placements = Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, GENERATION, profile));
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId(docId)))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE);
        var bodies = new java.util.HashMap<DocumentUploadPayloads.Key, PartObject>();
        // Serialized protobuf bytes exercise real transfer identity. Slot semantics await typed admission.
        for (int i = 0; i < count; i++) {
            byte[] bytes = Document.newBuilder().setDocId(docId + "/" + i).build().toByteArray();
            var part = new PartObject(i == 0 ? DocumentPart.DOCUMENT_PART_CORE : DocumentPart.DOCUMENT_PART_CHUNKS,
                    i == 0 ? "" : "chunks-" + i, bytes, DocumentPartCodec.sha256Hex(bytes));
            bodies.put(new DocumentUploadPayloads.Key("member", i), part);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.length).setSha256(part.sha256()).setContentType("application/protobuf")));
        }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var owner = new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        UUID attempt = UUID.randomUUID();
        return new Fixture(command, owner, DocumentOperationUploadAdmission.prepare(command, placements, Map.of("member", attempt), lease),
                Map.copyOf(bodies), placements, attempt);
    }
}

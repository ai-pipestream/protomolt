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
    private static LedgerDatabase database;
    private static Tx tx;
    private static OpenedBlobStore opened;
    private static ManagedBackendLedger.Profile profile;
    private static DocumentOperationUploadAdmission admission;
    private static final String GENERATION="selected-transfer";
    private static final String NAMESPACE="selected-transfer";
    private static final Duration LEASE=Duration.ofMinutes(5);
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
                (generation, retained) -> new DocumentUploadCoordinator.Backend(profile.identity(), noCapabilities), 4, Duration.ofMillis(25))) {
            assertThatThrownBy(() -> coordinator.stage(ADMIN, f.owner, f.prepared, f.bodies, Map.of(), () -> {}))
                    .hasMessageContaining("capabilities");
            assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_part_attempts WHERE attempt_id=:id")
                    .setParameter("id", f.attempt).getSingleResult()).longValue())).isZero();
            assertThat(budget.reservedBytes()).isZero();
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
        }, 8, Duration.ofSeconds(1))) {
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

    private static DocumentUploadCoordinator coordinator(BlobStore store, PayloadBudget budget, Duration age) {
        // A fault-injecting wrapper delegates to the real adapter; the underlying handle remains borrowed.
        var borrowed = new OpenedBlobStore(store, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
        return new DocumentUploadCoordinator(tx, new DriveLedger(tx), budget, (generation, retained) -> {
            assertThat(generation).isEqualTo(GENERATION);
            assertThat(retained).isEqualTo(profile);
            return new DocumentUploadCoordinator.Backend(profile.identity(), borrowed);
        }, 4, age);
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

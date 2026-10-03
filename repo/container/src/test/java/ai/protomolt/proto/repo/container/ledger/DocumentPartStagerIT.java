package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.v1.Document;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPartStagerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static LedgerDatabase database;
    static Tx tx;
    static OpenedBlobStore opened;
    static BackendIdentity identity;
    static final String GENERATION = "stager-original";
    static final String NAMESPACE = "stager-original";

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false"));
        opened.ensureNamespace(NAMESPACE);
        try (var admin = software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(software.amazon.awssdk.regions.Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .build()) {
            admin.putBucketVersioning(b -> b.bucket(NAMESPACE).versioningConfiguration(v ->
                    v.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        }
        identity = S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true);
        new ManagedBackendLedger(tx).bind(GENERATION, new ManagedBackendLedger.Profile(identity, "stager-realm"));
    }
    @AfterAll static void close() throws Exception {
        if (opened != null) opened.close();
        if (database != null) database.close();
    }
    private record Input(DocumentPartAttemptLedger.Plan plan, List<PartObject> payloads) {}
    private static Input input() {
        UUID node = UUID.randomUUID(), attempt = UUID.randomUUID();
        var parts = DocumentPartCodec.split(Document.newBuilder().setDocId(node.toString())
                .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().addSemanticResults(
                        ai.protomolt.proto.repo.v1.SemanticProcessingResult.newBuilder().setResultId("chunk-set"))).build(), PartLayouts.document());
        String prefix = "documents/account/" + node + "/attempts/" + attempt + "/";
        var objects = parts.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(p.part(), p.subKey(),
                DocumentPartCodec.objectKey(prefix, p.part(), p.subKey()), p.bytes().length, p.sha256(), "application/protobuf")).toList();
        return new Input(new DocumentPartAttemptLedger.Plan(attempt,
                new DocumentPartAttemptLedger.Location(node, "account", GENERATION, NAMESPACE), 0, Map.of(), objects), parts);
    }
    private static OpenedBlobStore borrowed(BlobStore store) {
        return new OpenedBlobStore(store, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
    }
    @FunctionalInterface interface AfterCall { Object apply(String method, Object[] arguments, Object result) throws Exception; }
    private static BlobStore intercept(AfterCall after) {
        return (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class},
                (proxy, method, args) -> {
                    try { return after.apply(method.getName(), args, method.invoke(opened.store(), args)); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
    }

    @Test void completePlanExistsBeforePutAndVerifiedBytesRemainUnpublished() {
        var input = input();
        var seen = new java.util.concurrent.atomic.AtomicBoolean();
        BlobStore checking = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("put")) {
                        var admission = new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow();
                        assertThat(admission.state()).isEqualTo("STAGING");
                        assertThat(admission.plannedCount()).isEqualTo(input.payloads().size());
                        var key = ((BlobStore.PutSpec) args[0]).key();
                        int planned = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND object_key=:key")
                                .setParameter("id", admission.id()).setParameter("key", key).getSingleResult()).intValue());
                        assertThat(planned).isEqualTo(1);
                        seen.set(true);
                    }
                    if (method.getName().equals("get")) assertThat((String) args[2]).isNotBlank().isNotEqualTo("null");
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(checking))) {
            var staged = stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of());
            assertThat(seen.get()).isTrue();
            assertThat(staged.attempt().state()).isEqualTo("VERIFIED");
            assertThat(staged.parts()).hasSize(input.payloads().size());
            assertThat(staged.parts()).allSatisfy(part -> assertThat(part.providerVersion()).isNotBlank().isNotEqualTo("null"));
            assertThat(new DocumentLedger(tx).findByNodeId(input.plan().location().nodeId())).isEmpty();
        }
        assertThat(opened.store().get(NAMESPACE, input.plan().objects().getFirst().objectKey()).data())
                .isEqualTo(input.payloads().getFirst().bytes());
    }

    @Test void ambiguousPutRetainsUnverifiedAttemptWithoutRetryOrReadBack() {
        var input = input();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var original = new IllegalStateException("injected after provider committed PUT");
        var store = intercept((method, args, result) -> {
            if (method.equals("put")) { calls.incrementAndGet(); throw original; }
            throw new AssertionError("Unexpected provider call after ambiguous PUT: " + method);
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOfSatisfying(DocumentPartStager.StageFailure.class, failure -> {
                        assertThat(failure.attemptId()).isEqualTo(input.plan().attemptId());
                        assertThat(failure.getCause()).isSameAs(original);
                    });
        }
        assertThat(calls.get()).isEqualTo(1);
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow().state()).isEqualTo("STAGING");
        assertThat(opened.store().get(NAMESPACE, input.plan().objects().getFirst().objectKey()).data()).isEqualTo(input.payloads().getFirst().bytes());
    }

    @Test void corruptReadCannotBecomeVerified() {
        var input = input();
        var store = intercept((method, args, result) -> {
            if (!method.equals("get")) return result;
            var actual = (BlobStore.GetResult) result;
            byte[] corrupted = actual.data().clone(); corrupted[0] ^= 1;
            return new BlobStore.GetResult(corrupted, actual.contentType(), actual.eTag(), actual.versionId());
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOf(DocumentPartStager.StageFailure.class).hasStackTraceContaining("Read-back differs");
        }
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow().state()).isEqualTo("STAGING");
    }

    @Test void secondPutFailurePreservesPartialVerificationAndNeverPublishes() {
        var input = input();
        var puts = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, result) -> {
            if (method.equals("put") && puts.incrementAndGet() == 2) throw new IllegalStateException("second acknowledgement lost");
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOf(DocumentPartStager.StageFailure.class).hasStackTraceContaining("second acknowledgement lost");
        }
        int verified = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND verified")
                .setParameter("id", input.plan().attemptId()).getSingleResult()).intValue());
        assertThat(verified).isEqualTo(1);
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow().state()).isEqualTo("STAGING");
        assertThat(new DocumentLedger(tx).findByNodeId(input.plan().location().nodeId())).isEmpty();
    }

    @Test void leaseLossAfterPutStopsReadBackAndCannotReacquireOwnership() {
        var input = input();
        var gets = new java.util.concurrent.atomic.AtomicInteger();
        var store = intercept((method, args, result) -> {
            if (method.equals("get")) gets.incrementAndGet();
            if (method.equals("put")) tx.inTransaction(em -> {
                em.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                        .setParameter("id", input.plan().attemptId()).getSingleResult();
                // Hold the real row lock past expiry so heartbeat renewal cannot keep ownership alive.
                em.createNativeQuery("SELECT pg_sleep(2.2)").getSingleResult();
            });
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(1), Map.of()))
                    .isInstanceOf(DocumentPartStager.StageFailure.class).hasStackTraceContaining("lease expired");
        }
        assertThat(gets.get()).isZero();
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow().state()).isEqualTo("STAGING");
    }

    @Test void closeDuringPutLeavesAttemptForRecoveryAndBorrowedProviderOpen() {
        var input = input();
        var stagerRef = new java.util.concurrent.atomic.AtomicReference<DocumentPartStager>();
        var store = intercept((method, args, result) -> {
            if (method.equals("put")) stagerRef.get().close();
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            stagerRef.set(stager);
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOf(DocumentPartStager.StageFailure.class).hasCauseInstanceOf(java.util.concurrent.CancellationException.class);
        }
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow().state()).isEqualTo("STAGING");
        assertThat(opened.store().get(NAMESPACE, input.plan().objects().getFirst().objectKey()).data()).isEqualTo(input.payloads().getFirst().bytes());
    }

    @Test void callerMutationAfterAdmissionCannotChangeWrittenBytes() {
        var input = input();
        byte[] expected = input.payloads().getFirst().bytes().clone();
        var store = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    // Mutate the caller's array before the real adapter consumes the PUT body.
                    if (method.getName().equals("put")) input.payloads().getFirst().bytes()[0] ^= 1;
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            assertThat(stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()).attempt().state()).isEqualTo("VERIFIED");
        }
        assertThat(opened.store().get(NAMESPACE, input.plan().objects().getFirst().objectKey()).data()).isEqualTo(expected);
    }

    @Test void mismatchedPayloadFailsBeforeAdmission() {
        var input = input();
        input.payloads().getFirst().bytes()[0] ^= 1;
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, opened)) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId())).isEmpty();
    }

    @Test void repeatedAdmissionReportsAttemptIdentityWithoutOverwritingKeys() {
        var input = input();
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, opened)) {
            stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of());
        }
        BlobStore forbidden = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    throw new AssertionError("Repeated admission reached provider " + method.getName());
                });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(forbidden))) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOfSatisfying(DocumentPartStager.StageFailure.class,
                            failure -> assertThat(failure.attemptId()).isEqualTo(input.plan().attemptId()))
                    .hasMessageContaining("admission");
        }
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId()).orElseThrow().state()).isEqualTo("VERIFIED");
    }

    @Test void unsupportedOrMismatchedBackendIsRejectedBeforeStaging() {
        var expiring = new OpenedBlobStore(opened.store(), () -> {}, java.util.Set.of(BlobCapability.OBJECT_EXPIRY), opened::ensureNamespace);
        assertThatThrownBy(() -> new DocumentPartStager(tx, GENERATION, identity, expiring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-expiring writes");
        var wrong = S3BackendIdentity.of("https://different.example", S3.getRegion(), true);
        assertThatThrownBy(() -> new DocumentPartStager(tx, GENERATION, wrong, opened))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retained profile");
    }

    @Test void heartbeatKeepsLeaseLiveAcrossSlowProviderCall() {
        var input = input();
        var store = intercept((method, args, result) -> {
            if (method.equals("put")) Thread.sleep(2200);
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store))) {
            assertThat(stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(1), Map.of()).attempt().state()).isEqualTo("VERIFIED");
        }
    }

    @Test void byteLimitRefusesAdmissionBeforeCopyingOrWriting() {
        var input = input();
        long bytes = input.payloads().stream().mapToLong(p -> p.bytes().length).sum();
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, opened, bytes - 1)) {
            assertThatThrownBy(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("byte capacity exhausted");
        }
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId())).isEmpty();
    }

    @Test void recoveryReclaimsExactVersionsAndRechecksLateWrites() {
        var input = input();
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, opened)) {
            stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(1), Map.of());
        }
        expire(input.plan().attemptId());
        String unrelated = "unrelated/" + UUID.randomUUID();
        opened.store().put(new BlobStore.PutSpec(NAMESPACE, unrelated, "text/plain", Map.of(), null), new byte[] {1});
        var recovery = new DocumentAttemptRecovery(new DocumentAttemptCleanupLedger(tx), (generation, profile) -> {
            assertThat(generation).isEqualTo(GENERATION);
            assertThat(profile.identity()).isEqualTo(identity);
            assertThat(profile.storageRealm()).isEqualTo("stager-realm");
            return opened.reclaimer();
        });
        assertThat(recovery.recover(input.plan().attemptId(), Duration.ofSeconds(5)).outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.ABSENT);
        for (var object : input.plan().objects())
            assertThatThrownBy(() -> opened.store().get(NAMESPACE, object.objectKey())).isInstanceOf(BlobStore.BlobNotFoundException.class);
        // Simulate a late provider commit after the first absence observation.
        var object = input.plan().objects().getFirst();
        opened.store().put(new BlobStore.PutSpec(NAMESPACE, object.objectKey(), object.contentType(), Map.of(), object.sha256()),
                input.payloads().getFirst().bytes());
        assertThat(recovery.recover(input.plan().attemptId(), Duration.ofSeconds(5)).outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.ABSENT);
        assertThatThrownBy(() -> opened.store().get(NAMESPACE, object.objectKey())).isInstanceOf(BlobStore.BlobNotFoundException.class);
        assertThat(opened.store().get(NAMESPACE, unrelated).data()).containsExactly((byte) 1);
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan().attemptId())).isPresent();
    }

    @Test void recoveryKeepsFailuresAndRetriesAfterBackendReturns() {
        var input = input();
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, opened)) {
            stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(1), Map.of());
        }
        expire(input.plan().attemptId());
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        var unavailable = new DocumentAttemptRecovery(cleanup, (generation, profile) -> null);
        var failed = unavailable.recover(input.plan().attemptId(), Duration.ofSeconds(5));
        assertThat(failed.outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.RETRY);
        assertThat(failed.failure()).isInstanceOf(IllegalStateException.class);
        String diagnostic = tx.readOnly(em -> (String) em.createNativeQuery(
                "SELECT last_error FROM document_part_attempt_cleanup WHERE attempt_id=:id")
                .setParameter("id", input.plan().attemptId()).getSingleResult());
        assertThat(diagnostic).contains("IllegalStateException");
        assertThat(opened.store().get(NAMESPACE, input.plan().objects().getFirst().objectKey()).data()).isNotEmpty();
        var recovered = new DocumentAttemptRecovery(cleanup, (generation, profile) -> opened.reclaimer());
        assertThat(recovered.recover(input.plan().attemptId(), Duration.ofSeconds(5)).outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.ABSENT);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void recoveryDoesNotTurnUnconfirmedOrLostDeletionAcknowledgementIntoSuccess(boolean lostAcknowledgement) {
        var input = input();
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, opened)) {
            stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(1), Map.of());
        }
        expire(input.plan().attemptId());
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        var fault = new IllegalStateException("Injected lost deletion acknowledgement");
        var visited = new java.util.ArrayList<String>();
        var recovery = new DocumentAttemptRecovery(cleanup, (generation, profile) -> (namespace, key) -> {
            visited.add(key);
            opened.reclaimer().reclaim(namespace, key);
            if (lostAcknowledgement) throw fault;
            return false; // Real deletion occurred, but the caller cannot confirm absence.
        });
        var result = recovery.recover(input.plan().attemptId(), Duration.ofSeconds(5));
        assertThat(result.outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.RETRY);
        if (lostAcknowledgement) assertThat(result.failure()).isSameAs(fault);
        else {
            assertThat(result.failure()).isNull();
            assertThat(visited).containsExactlyElementsOf(input.plan().objects().stream().map(DocumentPartAttemptLedger.PlannedObject::objectKey).toList());
        }
        assertThat(cleanup.candidates(Duration.ZERO, 1000)).contains(input.plan().attemptId());
        var retried = new DocumentAttemptRecovery(cleanup, (generation, profile) -> opened.reclaimer());
        assertThat(retried.recover(input.plan().attemptId(), Duration.ofSeconds(5)).outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.ABSENT);
    }

    private static void expire(UUID attempt) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05)
                FROM document_part_attempts WHERE attempt_id=:id
                """).setParameter("id", attempt).getSingleResult());
    }

    @Test void closeReportsBusyUntilProviderReturnsAndReleasesBorrowedResources() throws Exception {
        var input = input();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var store = intercept((method, args, result) -> {
            if (method.equals("put")) {
                entered.countDown();
                if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Fault gate timed out");
            }
            return result;
        });
        long bytes = input.payloads().stream().mapToLong(p -> p.bytes().length).sum();
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, borrowed(store), bytes);
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> stager.stage(input.plan(), input.payloads(), Duration.ofSeconds(5), Map.of()));
            try {
                assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                // The budget is shared, not a per-call maximum.
                var another = input();
                assertThatThrownBy(() -> stager.stage(another.plan(), another.payloads(), Duration.ofSeconds(5), Map.of()))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("byte capacity exhausted");
                assertThat(new DocumentPartAttemptLedger(tx).find(another.plan().attemptId())).isEmpty();
                stager.close();
                assertThat(stager.awaitIdle(Duration.ofMillis(20))).isFalse();
                release.countDown();
                assertThatThrownBy(() -> pending.get(10, java.util.concurrent.TimeUnit.SECONDS))
                        .hasCauseInstanceOf(DocumentPartStager.StageFailure.class);
                assertThat(stager.awaitIdle(Duration.ofSeconds(10))).isTrue();
                assertThatThrownBy(() -> stager.stage(another.plan(), another.payloads(), Duration.ofSeconds(5), Map.of()))
                        .hasMessage("Document stager is closed");
            } finally { release.countDown(); }
        }
        assertThat(opened.store().get(NAMESPACE, input.plan().objects().getFirst().objectKey()).data()).isEqualTo(input.payloads().getFirst().bytes());
    }
}

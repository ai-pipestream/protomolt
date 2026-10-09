package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.v1.Document;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPartParallelIT {
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
        opened = BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", S3.getEndpoint().toString()),
                Map.entry("region", S3.getRegion()),
                Map.entry("access-key", S3.getAccessKey()),
                Map.entry("secret-key", S3.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")));
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
    private record Input(DocumentPartAttemptLedger.Plan plan, List<PartObject> parts) {}
    private static Input input() {
        UUID node = UUID.randomUUID(), attempt = UUID.randomUUID();
        var doc = Document.newBuilder().setDocId(node.toString());
        var search = ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder();
        for (int i = 0; i < 7; i++) search.addSemanticResults(ai.protomolt.proto.repo.v1.SemanticProcessingResult.newBuilder().setResultId("set-" + i));
        var parts = DocumentPartCodec.split(doc.setSearchMetadata(search).build(), PartLayouts.document());
        assertThat(parts).hasSize(8);
        String prefix = "documents/account/" + node + "/attempts/" + attempt + "/";
        var objects = parts.stream().map(p -> new DocumentPartAttemptLedger.PlannedObject(p.part(), p.subKey(),
                DocumentPartCodec.objectKey(prefix, p.part(), p.subKey()), p.bytes().length, p.sha256(), "application/protobuf")).toList();
        return new Input(new DocumentPartAttemptLedger.Plan(attempt,
                new DocumentPartAttemptLedger.Location(node, "account", GENERATION, NAMESPACE), 0, Map.of(), objects), parts);
    }
    @FunctionalInterface interface AfterCall { Object apply(String method, Object[] args, Object result) throws Exception; }
    private static OpenedBlobStore intercepted(AfterCall after) {
        var store = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[] {BlobStore.class},
                (proxy, method, args) -> {
                    try { return after.apply(method.getName(), args, method.invoke(opened.store(), args)); }
                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        return new OpenedBlobStore(store, () -> {}, opened.capabilities(), opened::ensureNamespace, opened.reclaimer());
    }
    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
    }
    private static void unpublished(Input input) {
        assertThat(new DocumentLedger(tx).findByNodeId(input.plan.location().nodeId())).isEmpty();
        assertThat(new DocumentPartAttemptLedger(tx).find(input.plan.attemptId()).orElseThrow().state()).isEqualTo("STAGING");
    }

    @Test void fourWorkersOverlapAndReturnExactOrderedVersions() throws Exception {
        var input = input(); var entered = new CountDownLatch(4); var release = new CountDownLatch(1);
        var puts = new AtomicInteger(); var active = new AtomicInteger(); var peak = new AtomicInteger();
        var provider = intercepted((method, args, result) -> {
            if (method.equals("put")) {
                int current = active.incrementAndGet(); peak.accumulateAndGet(current, Math::max);
                try { if (puts.incrementAndGet() <= 4) { entered.countDown(); await(release); } }
                finally { active.decrementAndGet(); }
            }
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, provider);
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> stager.stage(input.plan, input.parts, Duration.ofSeconds(10), Map.of()));
            try {
                await(entered); assertThat(puts.get()).isEqualTo(4); release.countDown();
                var result = pending.get(20, TimeUnit.SECONDS);
                assertThat(peak.get()).isEqualTo(4);
                assertThat(result.attempt().state()).isEqualTo("VERIFIED");
                assertThat(result.parts()).extracting(DocumentPublicationLedger.Part::key)
                        .containsExactlyElementsOf(input.plan.objects().stream().map(DocumentPartAttemptLedger.PlannedObject::objectKey).toList());
                for (int i = 0; i < result.parts().size(); i++) {
                    var part = result.parts().get(i); assertThat(part.providerVersion()).isNotBlank().isNotEqualTo("null");
                    assertThat(opened.store().get(NAMESPACE, part.key(), part.providerVersion()).data()).isEqualTo(input.parts.get(i).bytes());
                }
            } finally { release.countDown(); }
        }
    }

    @Test void sharedLimitAndShutdownRetainAllStartedCalls() throws Exception {
        var entered = new CountDownLatch(32); var release = new CountDownLatch(1); var puts = new AtomicInteger();
        var admitted = new CountDownLatch(9);
        var provider = intercepted((method, args, result) -> {
            if (method.equals("put")) { assertThat(puts.incrementAndGet()).isLessThanOrEqualTo(32); entered.countDown(); await(release); }
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, provider);
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = new java.util.ArrayList<Future<?>>();
            for (int i = 0; i < 9; i++) {
                var input = input();
                var observed = new AtomicBoolean();
                pending.add(executor.submit(() -> stager.stage(input.plan, input.parts, Duration.ofSeconds(10), Map.of(), () -> {
                    if (!observed.get() && new DocumentPartAttemptLedger(tx).find(input.plan.attemptId()).isPresent()
                            && observed.compareAndSet(false, true)) admitted.countDown();
                })));
            }
            try {
                await(admitted); await(entered); assertThat(puts.get()).isEqualTo(32);
                stager.close(); assertThat(stager.awaitIdle(Duration.ofMillis(30))).isFalse();
                release.countDown();
                for (var future : pending) assertThatThrownBy(() -> future.get(20, TimeUnit.SECONDS)).hasRootCauseInstanceOf(CancellationException.class);
                assertThat(stager.awaitIdle(Duration.ofSeconds(10))).isTrue();
                assertThat(puts.get()).isEqualTo(32);
            } finally { release.countDown(); }
        }
    }

    @Test void failureDrainsPeersAndDoesNotStartRemainingParts() throws Exception {
        var input = input(); var entered = new CountDownLatch(4); var release = new CountDownLatch(1);
        var failed = new CountDownLatch(1); var puts = new AtomicInteger();
        var original = new IllegalStateException("Lost PUT acknowledgement");
        var provider = intercepted((method, args, result) -> {
            if (method.equals("put")) {
                int index = puts.incrementAndGet(); entered.countDown(); await(entered);
                if (index == 1) { failed.countDown(); throw original; }
                await(release);
            }
            return result;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, provider);
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> stager.stage(input.plan, input.parts, Duration.ofSeconds(10), Map.of()));
            try {
                await(failed);
                assertThatThrownBy(() -> pending.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                release.countDown();
                assertThatThrownBy(() -> pending.get(20, TimeUnit.SECONDS)).hasRootCause(original);
                assertThat(puts.get()).isEqualTo(4); unpublished(input);
            } finally { release.countDown(); }
        }
    }

    @Test void callerInterruptionDrainsWorkersBeforeReturningAndRestoresFlag() throws Exception {
        var input = input(); var entered = new CountDownLatch(4); var release = new CountDownLatch(1);
        var result = new AtomicReference<Throwable>(); var flag = new AtomicBoolean();
        var provider = intercepted((method, args, value) -> {
            if (method.equals("put")) { entered.countDown(); await(release); }
            return value;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, provider)) {
            var owner = Thread.ofVirtual().start(() -> {
                result.set(catchThrowable(() -> stager.stage(input.plan, input.parts, Duration.ofSeconds(10), Map.of())));
                flag.set(Thread.currentThread().isInterrupted());
            });
            try {
                await(entered); owner.interrupt();
                assertThat(owner.join(Duration.ofMillis(100))).isFalse();
                release.countDown(); assertThat(owner.join(Duration.ofSeconds(20))).isTrue();
                assertThat(result.get()).isInstanceOf(DocumentPartStager.StageFailure.class).hasCauseInstanceOf(CancellationException.class);
                assertThat(flag).isTrue(); unpublished(input);
            } finally { release.countDown(); owner.join(Duration.ofSeconds(20)); }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"corrupt", "expired", "cancelled"})
    void invalidOrCancelledParallelWritesNeverBecomeVerified(String fault) {
        var input = input(); var fired = new AtomicBoolean(); var cancelled = new AtomicBoolean();
        var provider = intercepted((method, args, value) -> {
            if (fault.equals("corrupt") && method.equals("getBounded") && fired.compareAndSet(false, true)) {
                var actual = (BlobStore.GetResult) value; var bytes = actual.data().clone(); bytes[0] ^= 1;
                return new BlobStore.GetResult(bytes, actual.contentType(), actual.eTag(), actual.versionId());
            }
            if (!fault.equals("corrupt") && method.equals("put") && fired.compareAndSet(false, true)) {
                if (fault.equals("cancelled")) cancelled.set(true);
                else tx.inTransaction(em -> {
                    em.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                            .setParameter("id", input.plan.attemptId()).getSingleResult();
                    em.createNativeQuery("SELECT pg_sleep(2.2)").getSingleResult();
                });
            }
            return value;
        });
        try (var stager = new DocumentPartStager(tx, GENERATION, identity, provider)) {
            var failure = catchThrowable(() -> stager.stage(input.plan, input.parts, Duration.ofSeconds(1), Map.of(), () -> {
                if (cancelled.get()) throw new CancellationException("Caller cancelled");
            }));
            assertThat(fired).isTrue();
            assertThat(failure).isInstanceOf(DocumentPartStager.StageFailure.class);
            if (fault.equals("corrupt")) assertThat(failure).hasStackTraceContaining("Read-back differs");
            if (fault.equals("expired")) assertThat(failure).hasStackTraceContaining("lease expired");
            if (fault.equals("cancelled")) assertThat(failure).hasCauseInstanceOf(CancellationException.class);
            unpublished(input);
        }
    }
}

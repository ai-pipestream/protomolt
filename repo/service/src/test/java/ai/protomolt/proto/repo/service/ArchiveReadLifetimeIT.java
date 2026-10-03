package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.archive.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.*;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL and S3; delay/cancellation is injected around the real read. */
@Testcontainers
@Timeout(60)
class ArchiveReadLifetimeIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final RepositoryCaller CALLER = new RepositoryCaller("read-lifetime", true);
    private static LedgerDatabase database;
    private static Tx tx;
    private static ArchiveLedger ledger;
    private static DriveLedger drives;
    private static OpenedBlobStore opened;

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new ArchiveLedger(tx);
        drives = new DriveLedger(tx);
        var providers = BlobStores.discover();
        var options = Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false");
        opened = providers.open("s3", options);
        opened.ensureNamespace("read-lifetime");
        new ManagedBackendLedger(tx).bind("read-original", new ManagedBackendLedger.Profile(
                providers.managedIdentity("s3", options), "read-realm"));
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "read-drive";
        drive.bucket = "read-lifetime"; drive.prefix = "archive"; drive.driveType = "CUSTOM";
        drives.insert(drive);
        operations(opened.store()).createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName("records").setDriveName("read-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
    }

    @AfterAll static void close() throws Exception {
        try { if (opened != null) opened.close(); }
        finally { if (database != null) database.close(); }
    }

    private static ArchiveOperations operations(BlobStore readerStore) {
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, UUID.randomUUID()), (generation, realm) -> {
            assertThat(generation).isEqualTo("read-original"); assertThat(realm).isEqualTo("read-realm");
            return readerStore;
        });
        return new ArchiveOperations(ledger, drives, opened.store(), BridgeEngine.standard(), reader,
                new ArchiveObjectWriter(new ArchiveUploadLedger(tx), opened.store(), "read-original", opened.capabilities(), Duration.ofMinutes(5)));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void cleanupWaitsForActualProviderCompletionAfterLogicalDeletion(boolean transport, boolean cancel) throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var actualBytes = new CompletableFuture<byte[]>();
        BlobStore delayed = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getBounded")) {
                        entered.countDown();
                        boolean waiting = true;
                        while (waiting) {
                            try {
                                if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("Read was never released");
                                waiting = false;
                            } catch (InterruptedException ignoredForFaultInjection) {
                                // Explicitly model provider work continuing after interruption.
                                interrupted.countDown();
                            }
                        }
                    }
                    try {
                        Object result = method.invoke(opened.store(), args);
                        if (method.getName().equals("getBounded")) actualBytes.complete(((BlobStore.GetResult) result).data());
                        return result;
                    } catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
        var repository = operations(delayed);
        var bytes = ByteString.copyFromUtf8("retained while a provider read is active");
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
        var put = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original")).setData(bytes)).build();
        var saved = repository.putEntry(CALLER, put);
        var rendition = saved.getManifest().getRenditions(0);
        UUID object = UUID.fromString(rendition.getStorageObjectId());
        String name = "archive-read-lifetime-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(repository)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        var context = io.grpc.Context.current().withCancellation();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var get = GetEntryRequest.newBuilder().setAddress(address).build();
                var response = executor.submit(() -> transport
                        ? context.call(() -> ArchiveServiceGrpc.newBlockingStub(channel).getEntry(get))
                        : repository.getEntry(CALLER, get));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(pinCount(object)).isEqualTo(1);
                if (cancel) {
                    if (transport) {
                        context.cancel(new java.util.concurrent.CancellationException("Injected client cancellation"));
                        assertThatThrownBy(() -> response.get(5, TimeUnit.SECONDS))
                                .hasCauseInstanceOf(io.grpc.StatusRuntimeException.class);
                    } else {
                        assertThat(response.cancel(true)).isTrue();
                        assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
                    }
                }
                var mutations = new ArchiveMutationOperations(ledger, new ArchiveMutationLedger(tx), new ArchiveMutationObservations(tx));
                // This must finish while the provider is blocked: no database transaction spans GET.
                var mutation = executor.submit(() -> mutations.mutateArchive(CALLER, ArchiveMutationRequest.newBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(address)).build()));
                assertThat(mutation.get(5, TimeUnit.SECONDS).getEntryDeleted()).isTrue();
                assertThat(ledger.findVersion(UUID.fromString(saved.getEntryUuid()), 1)).isEmpty();
                assertThat(pinCount(object)).isEqualTo(1);
                var recovery = new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx),
                        (generation, profile) -> opened.reclaimer());
                assertThat(recovery.recover(object, Instant.now().plusSeconds(60))).isEqualTo(ArchiveObjectRecovery.Outcome.SKIPPED);
                assertThat(new ArchiveReadLedger(tx, UUID.randomUUID()).acquire(UUID.fromString(saved.getEntryUuid()), 1, object)).isEmpty();
                assertThat(opened.store().get("read-lifetime", rendition.getObjectKey()).data()).isEqualTo(bytes.toByteArray());
                release.countDown();
                assertThat(actualBytes.get(10, TimeUnit.SECONDS)).isEqualTo(bytes.toByteArray());
                if (!cancel) assertThat(response.get(10, TimeUnit.SECONDS).getRenditions(0).getData()).isEqualTo(bytes);
                awaitReleased(object);
                assertThat(recovery.recover(object, Instant.now().plusSeconds(60))).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
                assertThatThrownBy(() -> opened.store().get("read-lifetime", rendition.getObjectKey()))
                        .isInstanceOf(BlobStore.BlobNotFoundException.class);
            } finally { release.countDown(); }
        } finally {
            context.close();
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @org.junit.jupiter.api.Test
    void idleReaderCannotClaimQuiescenceWhileItsFenceIsBlocked() throws Exception {
        UUID incarnation = UUID.randomUUID();
        var reader = new ArchiveObjectReader(new ArchiveReadLedger(tx, incarnation), (generation, realm) -> opened.store());
        try (var owner = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            owner.setAutoCommit(false);
            try {
                int pid;
                try (var statement = owner.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                    assertThat(result.next()).isTrue(); pid = result.getInt(1);
                }
                try (var statement = owner.prepareStatement("SELECT incarnation FROM repository_reader_incarnations WHERE incarnation=? FOR UPDATE")) {
                    statement.setObject(1, incarnation);
                    try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); }
                }
                var closing = executor.submit(reader::close);
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                boolean blocked = false;
                while (System.nanoTime() < deadline) {
                    blocked = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid))")
                            .setParameter("pid", pid).getSingleResult()).longValue()) > 0;
                    if (blocked) break;
                    Thread.sleep(10);
                }
                assertThat(blocked).isTrue();
                assertThatThrownBy(() -> reader.awaitIdle(Duration.ZERO))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("Complete the reader fence");
                owner.rollback();
                closing.get(10, TimeUnit.SECONDS);
                assertThat(reader.awaitIdle(Duration.ZERO)).isTrue();
            } finally { owner.rollback(); }
        }
    }

    private static long pinCount(UUID object) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_read_pins WHERE object_id=:id")
                .setParameter("id", object).getSingleResult()).longValue());
    }

    private static void awaitReleased(UUID object) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (pinCount(object) != 0 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(pinCount(object)).isZero();
    }
}

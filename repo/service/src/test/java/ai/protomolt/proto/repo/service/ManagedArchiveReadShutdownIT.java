package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import com.google.protobuf.ByteString;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Escaped library reads use actual S3/SQL; only provider delay and interruption are injected. */
@Testcontainers
@Timeout(60)
class ManagedArchiveReadShutdownIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    enum Failure { TIMEOUT, INTERRUPTED, PIN_RELEASE, FENCE }

    @ParameterizedTest @EnumSource(Failure.class)
    void hostRetainsBorrowedResourcesUntilEscapedReaderActuallyFinishes(Failure scenario) throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var delay = new AtomicBoolean();
        var providerClosed = new AtomicBoolean();
        var reads = new AtomicInteger();
        var installed = BlobStores.discover();
        var decorated = BlobStores.of(List.of(new BlobStoreProvider() {
            @Override public String id() { return "s3"; }
            @Override public BackendIdentity managedIdentity(Map<String, String> options) {
                return installed.managedIdentity("s3", options);
            }
            @Override public OpenedBlobStore open(Map<String, String> options) {
                var backing = installed.open("s3", options);
                BlobStore blocked = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                        new Class<?>[]{BlobStore.class}, (proxy, method, args) -> {
                            if (delay.get() && method.getName().equals("getBounded")) {
                                reads.incrementAndGet(); entered.countDown();
                                while (release.getCount() != 0) {
                                    try {
                                        if (!release.await(30, TimeUnit.SECONDS)) throw new AssertionError("Provider was never released");
                                    } catch (InterruptedException ignoredForFaultInjection) {
                                        // Explicitly model a provider call that continues after interruption.
                                    }
                                }
                                assertThat(providerClosed).isFalse();
                            }
                            try { return method.invoke(backing.store(), args); }
                            catch (InvocationTargetException failure) { throw failure.getCause(); }
                        });
                return new OpenedBlobStore(blocked, () -> { providerClosed.set(true); backing.close(); },
                        backing.capabilities(), backing::ensureNamespace, backing.reclaimer());
            }
        }));
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(),
                "read-shutdown", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("read-shutdown", "shutdown-realm", true));
        var host = new RepoServices(config, BridgeEngine.standard(), decorated);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var caller = new RepositoryCaller("read-shutdown", true);
                String account = "shutdown-" + UUID.randomUUID();
                host.driveRepository().createDrive(caller, CreateDriveRequest.newBuilder().setAccountId(account).setName("storage").build());
                var repository = host.archiveRepository();
                repository.createArchive(caller, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                        .setAccountId(account).setName("records").setDriveName("storage")
                        .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
                var address = EntryAddress.newBuilder().setAccountId(account).setArchive("records").setEntryId("entry").build();
                var data = ByteString.copyFromUtf8("must outlive an attempted host shutdown");
                var saved = repository.putEntry(caller, PutEntryRequest.newBuilder().setAddress(address)
                        .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                                .setData(data)).build());
                UUID object = UUID.fromString(saved.getManifest().getRenditions(0).getStorageObjectId());
                var get = GetEntryRequest.newBuilder().setAddress(address).build();
                delay.set(true);
                var result = executor.submit(() -> repository.getEntry(caller, get));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(pins(object)).isEqualTo(1);
                if (scenario == Failure.FENCE) {
                    sql("CREATE FUNCTION reject_shutdown_fence() RETURNS trigger LANGUAGE plpgsql AS $$ "
                            + "BEGIN IF EXISTS(SELECT 1 FROM archive_read_pins WHERE object_id='" + object
                            + "'::uuid AND reader_incarnation=OLD.incarnation) THEN "
                            + "RAISE EXCEPTION 'injected fence failure'; END IF; RETURN NEW; END $$");
                    try {
                        sql("CREATE TRIGGER reject_shutdown_fence BEFORE UPDATE ON repository_reader_incarnations "
                                + "FOR EACH ROW EXECUTE FUNCTION reject_shutdown_fence()");
                        assertThatThrownBy(() -> host.close(Duration.ofSeconds(1)))
                                .hasStackTraceContaining("injected fence failure");
                    } finally {
                        sql("DROP TRIGGER IF EXISTS reject_shutdown_fence ON repository_reader_incarnations");
                        sql("DROP FUNCTION reject_shutdown_fence()");
                    }
                } else if (scenario == Failure.INTERRUPTED) {
                    var closeFailure = new java.util.concurrent.CompletableFuture<Throwable>();
                    var interruptPreserved = new AtomicBoolean();
                    var closer = Thread.ofPlatform().start(() -> {
                        var failure = catchThrowable(() -> host.close(Duration.ofSeconds(20)));
                        interruptPreserved.set(Thread.currentThread().isInterrupted());
                        closeFailure.complete(failure);
                    });
                    try {
                        awaitReaderDrain(closer);
                        closer.interrupt();
                        assertThat(closeFailure.get(5, TimeUnit.SECONDS))
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("Archive reader drain interrupted")
                                .hasCauseInstanceOf(InterruptedException.class);
                        assertThat(interruptPreserved).isTrue();
                    } finally {
                        closer.interrupt();
                        assertThat(closer.join(Duration.ofSeconds(5))).isTrue();
                    }
                } else {
                    var failure = catchThrowable(() -> host.close(Duration.ofSeconds(1)));
                    assertThat(failure).isInstanceOf(IllegalStateException.class).hasMessageContaining("resources retained");
                    assertThat(failure).hasMessageContaining("archive reads still active");
                }
                assertThat(providerClosed).isFalse();
                assertThat(result.isDone()).isFalse();
                assertThat(pins(object)).isEqualTo(1);
                assertThatThrownBy(() -> repository.getEntry(caller, get))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                assertThat(reads.get()).isEqualTo(1);
                // A retry must not forget the still-running reader or close its resources.
                assertThatThrownBy(() -> host.close(Duration.ofSeconds(1))).hasMessageContaining("archive reads still active");
                assertThat(providerClosed).isFalse();
                if (scenario == Failure.PIN_RELEASE) {
                    sql("CREATE FUNCTION reject_shutdown_pin_release() RETURNS trigger LANGUAGE plpgsql AS $$ "
                            + "BEGIN RAISE EXCEPTION 'injected pin release failure'; END $$");
                    try {
                        sql("CREATE TRIGGER reject_shutdown_pin_release BEFORE DELETE ON archive_read_pins "
                                + "FOR EACH ROW WHEN (OLD.object_id = '" + object + "'::uuid) "
                                + "EXECUTE FUNCTION reject_shutdown_pin_release()");
                        release.countDown();
                        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                                .isInstanceOf(java.util.concurrent.ExecutionException.class)
                                .hasStackTraceContaining("injected pin release failure");
                        assertThat(pins(object)).isEqualTo(1);
                        host.close(Duration.ofSeconds(5));
                        assertThat(providerClosed).isTrue();
                        // Local drain does not grant permission to erase failed durable releases.
                        assertThat(pins(object)).isEqualTo(1);
                    } finally {
                        sql("DROP TRIGGER IF EXISTS reject_shutdown_pin_release ON archive_read_pins");
                        sql("DROP FUNCTION reject_shutdown_pin_release()");
                    }
                } else {
                    release.countDown();
                    assertThat(result.get(10, TimeUnit.SECONDS).getRenditions(0).getData()).isEqualTo(data);
                    assertThat(pins(object)).isZero();
                    host.close(Duration.ofSeconds(5));
                    assertThat(providerClosed).isTrue();
                }
            } finally { release.countDown(); }
        } finally { host.close(); }
    }

    private static long pins(UUID object) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.prepareStatement("SELECT count(*) FROM archive_read_pins WHERE object_id=?")) {
            statement.setObject(1, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getLong(1); }
        }
    }

    private static void sql(String sql) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void awaitReaderDrain(Thread closer) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline && closer.isAlive()) {
            for (var frame : closer.getStackTrace()) {
                if (frame.getClassName().equals("ai.protomolt.proto.repo.engine.ArchiveObjectReader")
                        && frame.getMethodName().equals("awaitIdle")) return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Shutdown did not enter the archive reader drain");
    }
}

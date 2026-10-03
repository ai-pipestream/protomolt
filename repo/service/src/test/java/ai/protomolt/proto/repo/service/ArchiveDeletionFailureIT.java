package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.ArchiveOperations;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Regression baseline: a failed physical delete must not report completed deletion. */
@Testcontainers
class ArchiveDeletionFailureIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static LedgerDatabase database;
    static ArchiveLedger ledger;
    static DriveLedger drives;
    static OpenedBlobStore opened;
    static ArchiveOperations normal;
    static final RepositoryCaller CALLER = new RepositoryCaller("archive-test", true);

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        var tx = new Tx(database.entityManagerFactory());
        ledger = new ArchiveLedger(tx);
        drives = new DriveLedger(tx);
        opened = BlobStores.discover().open("s3", Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false"));
        opened.ensureNamespace("archive-failures");
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID();
        drive.accountId = "account";
        drive.name = "archive-drive";
        drive.bucket = "archive-failures";
        drive.prefix = "archive";
        drive.driveType = "CUSTOM";
        drives.insert(drive);
        normal = new ArchiveOperations(ledger, drives, opened.store());
        normal.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName("records").setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
    }
    @AfterAll static void close() throws Exception {
        try { if (opened != null) opened.close(); }
        finally { if (database != null) database.close(); }
    }

    @Test void libraryDeletePropagatesStorageFailure() throws Exception { assertDeleteFailure(false); }
    @Test void grpcDeletePropagatesStorageFailure() throws Exception { assertDeleteFailure(true); }

    @Test void librarySqlFailureDoesNotDestroyRetainedBytes() throws Exception { assertSqlFailure(false); }
    @Test void grpcSqlFailureDoesNotDestroyRetainedBytes() throws Exception { assertSqlFailure(true); }

    private static void assertSqlFailure(boolean transport) throws Exception {
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
        var saved = normal.putEntry(CALLER, PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(RenditionContent.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original").setMediaType("text/plain"))
                        .setData(ByteString.copyFromUtf8("retain on rollback"))).build());
        var manifest = normal.getManifest(CALLER, GetEntryManifestRequest.newBuilder().setAddress(address).build()).getManifest();
        String key = manifest.getRenditions(0).getObjectKey();
        // Fault only this entry, inside real PostgreSQL after the version DELETE
        // but before entry deletion can commit. All object calls use the real S3 adapter.
        String function = "reject_archive_delete_" + UUID.randomUUID().toString().replace("-", "");
        UUID entryId = UUID.fromString(saved.getEntryUuid());
        var tx = new Tx(database.entityManagerFactory());
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION " + function + "() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'injected archive SQL failure' USING ERRCODE = '40001'; END; $$")
                    .executeUpdate();
            em.createNativeQuery("CREATE TRIGGER " + function + " BEFORE DELETE ON archive_entries "
                    + "FOR EACH ROW WHEN (OLD.entry_uuid = '" + entryId + "'::uuid) EXECUTE FUNCTION " + function + "()")
                    .executeUpdate();
        });
        try {
            var request = DeleteEntryRequest.newBuilder().setAddress(address).build();
            if (!transport) {
                assertThatThrownBy(() -> normal.deleteEntry(CALLER, request))
                        .hasStackTraceContaining("injected archive SQL failure");
            } else {
                String name = "archive-sql-failure-" + UUID.randomUUID();
                var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(normal)).build().start();
                var channel = InProcessChannelBuilder.forName(name).build();
                try {
                    assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(channel).deleteEntry(request))
                            .isInstanceOf(StatusRuntimeException.class);
                } finally { channel.shutdownNow(); server.shutdownNow(); }
            }
            assertThat(ledger.findEntry(entryId)).isPresent();
            assertThat(ledger.findVersion(entryId, 1)).isPresent();
            assertThat(opened.store().get("archive-failures", key).data())
                    .isEqualTo("retain on rollback".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER " + function + " ON archive_entries").executeUpdate();
                em.createNativeQuery("DROP FUNCTION " + function + "()").executeUpdate();
            });
        }
    }

    private static void assertDeleteFailure(boolean transport) throws Exception {
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
        normal.putEntry(CALLER, PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original").setMediaType("text/plain"))
                .setData(ByteString.copyFromUtf8("retained bytes"))).build());
        var manifest = normal.getManifest(CALLER, GetEntryManifestRequest.newBuilder().setAddress(address).build()).getManifest();
        String key = manifest.getRenditions(0).getObjectKey();
        var attempted = new AtomicBoolean();
        BlobStore failing = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(ArchiveDeletionFailureIT.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("delete")) {
                        attempted.set(true);
                        throw new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "injected delete outage", null);
                    }
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                });
        var operations = new ArchiveOperations(ledger, drives, failing);
        var request = DeleteEntryRequest.newBuilder().setAddress(address).build();
        if (!transport) {
            assertThatThrownBy(() -> operations.deleteEntry(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
        } else {
            String name = "archive-failure-" + UUID.randomUUID();
            var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(operations)).build().start();
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(channel).deleteEntry(request))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                error -> assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
            } finally { channel.shutdownNow(); server.shutdownNow(); }
        }
        assertThat(attempted).isTrue();
        assertThat(opened.store().get("archive-failures", key).data()).isEqualTo("retained bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}

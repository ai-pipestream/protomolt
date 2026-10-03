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

    @Test void boundReadsUseOriginalBackendIdentityAndNeverTheCurrentDrive() throws Exception {
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
        var bytes = ByteString.copyFromUtf8("bytes at the original location");
        var saved = normal.putEntry(CALLER, PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder()
                        .setName("original")).setData(bytes)).build());
        var tx = new Tx(database.entityManagerFactory());
        var objects = new ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger(tx);
        var profiles = new ManagedBackendLedger(tx);
        String generation = "bound-read-" + UUID.randomUUID();
        var profile = new ManagedBackendLedger.Profile(ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true), generation);
        profiles.bind(generation, profile);
        var entry = ledger.findEntry(UUID.fromString(saved.getEntryUuid())).orElseThrow();
        var item = saved.getManifest().getRenditions(0);
        String originalBucket = "bound-read-" + UUID.randomUUID();
        opened.ensureNamespace(originalBucket);
        assertThat(S3.execInContainer("awslocal", "s3api", "put-bucket-versioning", "--bucket", originalBucket,
                "--versioning-configuration", "Status=Enabled").getExitCode()).isZero();
        opened.store().put(new BlobStore.PutSpec(originalBucket, item.getObjectKey(), "text/plain", Map.of(), null), bytes.toByteArray());
        var uploads = new ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger(tx);
        var admission = uploads.begin(new ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger.Location(
                entry.entryUuid, entry.accountId, entry.archive, generation, originalBucket, item.getObjectKey()),
                bytes.size(), "text/plain", java.time.Duration.ofMinutes(1));
        var stored = opened.store().get(originalBucket, item.getObjectKey());
        assertThat(stored.versionId()).isNotBlank().isNotEqualTo("null");
        uploads.verify(admission.upload().objectId(), admission.upload().leaseToken(), stored.data().length,
                ai.protomolt.proto.repo.container.archive.ArchiveManifests.sha256Hex(stored.data()), stored.versionId(), stored.eTag());
        var version = ledger.findVersion(entry.entryUuid, 1).orElseThrow();
        version.version = 2;
        version.manifest = ai.protomolt.proto.repo.container.archive.ArchiveManifests.toJson(saved.getManifest().toBuilder()
                .setVersion(2).setRenditions(0, item.toBuilder().setStorageObjectId(admission.upload().objectId().toString())).build());
        entry.currentVersion = 2;
        ledger.commitSave(entry, 1, version, 0, ArchiveLedger.StatsDelta.none(),
                Map.of(admission.upload().objectId(), admission.upload().leaseToken()));
        opened.store().put(new BlobStore.PutSpec(originalBucket, item.getObjectKey(), "text/plain", Map.of(), null),
                ByteString.copyFromUtf8("a later provider revision").toByteArray());
        var reader = new ai.protomolt.proto.repo.engine.ArchiveObjectReader(objects, (identity, realm) -> {
            assertThat(identity).isEqualTo(generation);
            assertThat(realm).isEqualTo(generation);
            assertThat(profiles.find(identity)).contains(profile);
            return opened.store();
        });
        var bound = new ArchiveOperations(ledger, drives, opened.store(),
                ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), reader);
        var request = GetEntryRequest.newBuilder().setAddress(address).setVersion(2).build();
        var published = ai.protomolt.proto.repo.container.archive.ArchiveManifests.fromJson(version.manifest).getRenditions(0);
        var noIo = new ai.protomolt.proto.repo.engine.ArchiveObjectReader(objects, (identity, realm) -> {
            throw new AssertionError("Invalid manifest must fail before provider resolution");
        });
        for (var invalid : java.util.List.of(published.toBuilder().setObjectKey("wrong-key").build(),
                published.toBuilder().setSizeBytes(bytes.size() + 1).build(),
                published.toBuilder().setSha256("0".repeat(64)).build())) {
            assertThatThrownBy(() -> noIo.read(entry, 2, invalid)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        }
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE drives SET bucket='unrelated-current-location' WHERE account_id='account' AND name='archive-drive'")
                    .executeUpdate();
        });
        try {
            assertThat(bound.getEntry(CALLER, request).getRenditions(0).getData()).isEqualTo(bytes);
            String name = "bound-read-" + UUID.randomUUID();
            var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(bound)).build().start();
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                assertThat(ArchiveServiceGrpc.newBlockingStub(channel).getEntry(request).getRenditions(0).getData()).isEqualTo(bytes);
            } finally { channel.shutdownNow(); server.shutdownNow(); }
            assertThatThrownBy(() -> normal.getEntry(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
            var unavailable = new ArchiveOperations(ledger, drives, opened.store(),
                    ai.protomolt.proto.asset.bridge.BridgeEngine.standard(),
                    new ai.protomolt.proto.repo.engine.ArchiveObjectReader(objects, (identity, realm) -> {
                        throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "original backend offline");
                    }));
            assertThatThrownBy(() -> unavailable.getEntry(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
            String failedName = "bound-read-unavailable-" + UUID.randomUUID();
            var failedServer = InProcessServerBuilder.forName(failedName).addService(new ArchiveGrpcService(unavailable)).build().start();
            var failedChannel = InProcessChannelBuilder.forName(failedName).build();
            try {
                assertThatThrownBy(() -> ArchiveServiceGrpc.newBlockingStub(failedChannel).getEntry(request))
                        .isInstanceOfSatisfying(StatusRuntimeException.class,
                                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
            } finally { failedChannel.shutdownNow(); failedServer.shutdownNow(); }
            assertThat(S3.execInContainer("awslocal", "s3api", "delete-object", "--bucket", originalBucket,
                    "--key", item.getObjectKey(), "--version-id", stored.versionId()).getExitCode()).isZero();
            assertThatThrownBy(() -> bound.getEntry(CALLER, request)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("UPDATE drives SET bucket='archive-failures' WHERE account_id='account' AND name='archive-drive'")
                        .executeUpdate();
            });
        }
    }

    @Test void legacyDestructivePathsRefuseBoundManifestsBeforeDeletingObjects() {
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
        var firstRequest = PutEntryRequest.newBuilder().setAddress(address).addRenditions(RenditionContent.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setData(ByteString.copyFromUtf8("first bytes"))).build();
        var first = normal.putEntry(CALLER, firstRequest);
        normal.putEntry(CALLER, firstRequest.toBuilder().setRenditions(0,
                firstRequest.getRenditions(0).toBuilder().setData(ByteString.copyFromUtf8("second bytes"))).build());
        // Even an unresolvable binding must fail before touching storage.
        new Tx(database.entityManagerFactory()).inTransaction(em -> {
            em.createNativeQuery("""
                    UPDATE archive_versions SET manifest=jsonb_set(manifest,'{renditions,0,storageObjectId}',to_jsonb(CAST(:binding AS text)))
                    WHERE entry_uuid=:entry
                    """).setParameter("binding", UUID.randomUUID().toString())
                    .setParameter("entry", UUID.fromString(first.getEntryUuid())).executeUpdate();
        });
        for (Runnable operation : new Runnable[] {
                () -> normal.deleteEntry(CALLER, DeleteEntryRequest.newBuilder().setAddress(address).build()),
                () -> normal.deleteRendition(CALLER, DeleteRenditionRequest.newBuilder().setAddress(address)
                        .setRendition("original").setReason("test").build()),
                () -> normal.pruneVersions(CALLER, PruneVersionsRequest.newBuilder().setAddress(address).setKeepLatest(1).build())}) {
            assertThatThrownBy(operation::run).isInstanceOfSatisfying(RepositoryException.class,
                    error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        }
        assertThat(opened.store().get("archive-failures", first.getManifest().getRenditions(0).getObjectKey()).data())
                .isEqualTo("first bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(ledger.allVersions(UUID.fromString(first.getEntryUuid()))).hasSize(2);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void streamedDuplicateReportsCandidateCleanupFailureAndPreservesRetainedBytes(boolean lostAcknowledgement) {
        var address = EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
        var descriptor = RenditionDescriptor.newBuilder().setName("original").setMediaType("text/plain").build();
        byte[] bytes = "duplicate stream".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var saved = normal.putEntry(CALLER, PutEntryRequest.newBuilder().setAddress(address)
                .addRenditions(RenditionContent.newBuilder().setRendition(descriptor)
                        .setData(ByteString.copyFrom(bytes))).build());
        var retained = saved.getManifest().getRenditions(0);
        var attempted = new AtomicBoolean();
        BlobStore failing = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("delete")) {
                        assertThat(args[1]).isNotEqualTo(retained.getObjectKey());
                        attempted.set(true);
                        if (lostAcknowledgement) opened.store().delete((String) args[0], (String) args[1]);
                        throw new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "candidate cleanup outage", null);
                    }
                    try { return method.invoke(opened.store(), args); }
                    catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                });
        var operations = new ArchiveOperations(ledger, drives, failing);
        assertThatThrownBy(() -> operations.uploadStream(CALLER, address, descriptor, bytes.length,
                retained.getSha256(), null, null, null, null, new java.io.ByteArrayInputStream(bytes)))
                .isInstanceOfSatisfying(RepositoryException.class,
                        error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.UNAVAILABLE));
        assertThat(attempted).isTrue();
        assertThat(opened.store().get("archive-failures", retained.getObjectKey()).data()).isEqualTo(bytes);
        assertThat(normal.getManifest(CALLER, GetEntryManifestRequest.newBuilder().setAddress(address).build())
                .getManifest()).isEqualTo(saved.getManifest());
    }

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

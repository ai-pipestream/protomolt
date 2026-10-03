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
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real SQL publication and S3 writes, including uncertain provider outcomes. */
@Testcontainers
class ArchiveManagedUploadIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static LedgerDatabase database;
    static Tx tx;
    static ArchiveLedger ledger;
    static DriveLedger drives;
    static OpenedBlobStore opened;
    static ArchiveOperations managed;
    static final RepositoryCaller CALLER = new RepositoryCaller("archive-managed-test", true);

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new ArchiveLedger(tx);
        drives = new DriveLedger(tx);
        var providers = BlobStores.discover();
        var options = Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey(), "path-style", "true", "conditional-writes", "false");
        opened = providers.open("s3", options);
        opened.ensureNamespace("managed-archive");
        new ManagedBackendLedger(tx).bind("original", new ManagedBackendLedger.Profile(providers.managedIdentity("s3", options), "original-realm"));
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID();
        drive.accountId = "account";
        drive.name = "archive-drive";
        drive.bucket = "managed-archive";
        drive.prefix = "archive";
        drive.driveType = "CUSTOM";
        drives.insert(drive);
        managed = operations(opened.store());
        managed.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName("records").setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_RETAINED)).build());
    }

    @AfterAll static void close() throws Exception {
        try { if (opened != null) opened.close(); }
        finally { if (database != null) database.close(); }
    }

    static ArchiveOperations operations(BlobStore writerStore) {
        var reader = new ArchiveObjectReader(new ArchiveObjectLedger(tx), (generation, realm) -> {
            if (!generation.equals("original") || !realm.equals("original-realm"))
                throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Original backend unavailable");
            return opened.store();
        });
        return new ArchiveOperations(ledger, drives, opened.store(), BridgeEngine.standard(), reader,
                new ArchiveObjectWriter(new ArchiveUploadLedger(tx), writerStore, "original", opened.capabilities(), Duration.ofMinutes(5)));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void publicationReuseAndHistoricalReadsShareLibraryAndGrpcBehavior(boolean transport) throws Exception {
        String name = "archive-managed-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(name).addService(new ArchiveGrpcService(managed)).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var stub = ArchiveServiceGrpc.newBlockingStub(channel);
            var request = request("first bytes");
            java.util.function.Function<PutEntryRequest, PutEntryResponse> put = transport ? stub::putEntry : r -> managed.putEntry(CALLER, r);
            var first = put.apply(request);
            var same = put.apply(request);
            assertThat(same.getVersion()).isEqualTo(first.getVersion());
            assertThat(same.getManifest().getRenditions(0).getStorageObjectId()).isEqualTo(first.getManifest().getRenditions(0).getStorageObjectId());
            assertThat(uploadCount(UUID.fromString(first.getEntryUuid()))).isEqualTo(1);
            var second = put.apply(request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                    .setData(ByteString.copyFromUtf8("second bytes"))).build());
            assertThat(second.getVersion()).isEqualTo(2);
            assertThat(uploadCount(UUID.fromString(first.getEntryUuid()))).isEqualTo(2);
            var get = GetEntryRequest.newBuilder().setAddress(request.getAddress()).setVersion(1).build();
            var historical = transport ? stub.getEntry(get) : managed.getEntry(CALLER, get);
            assertThat(historical.getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("first bytes"));
            assertThat(new ArchiveObjectLedger(tx).readable(UUID.fromString(first.getEntryUuid()), 1,
                    UUID.fromString(first.getManifest().getRenditions(0).getStorageObjectId()))).isPresent();
            assertThatThrownBy(() -> new ArchiveOperations(ledger, drives, opened.store()).putEntry(CALLER, request))
                    .isInstanceOf(RepositoryException.class);
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @Test void lostWriteAcknowledgementLeavesOneStagedObjectAndNoPublishedVersion() {
        var request = request("landed without acknowledgement");
        BlobStore failing = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(opened.store(), args);
                        if (method.getName().equals("put"))
                            throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Injected lost write acknowledgement");
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                });
        assertThatThrownBy(() -> operations(failing).putEntry(CALLER, request)).isInstanceOf(RepositoryException.class);
        UUID entry = ArchiveIds.entryUuid(request.getAddress());
        assertThat(ledger.findEntry(entry)).isEmpty();
        assertThat(uploadCount(entry)).isEqualTo(1);
        tx.readOnly(em -> {
            var row = (Object[]) em.createNativeQuery("SELECT b.object_key,u.state FROM archive_object_bindings b JOIN archive_object_uploads u USING(object_id) WHERE b.entry_uuid=:entry")
                    .setParameter("entry", entry).getSingleResult();
            assertThat(row[1]).isEqualTo("STAGING");
            assertThat(opened.store().get("managed-archive", (String) row[0]).data()).isEqualTo(request.getRenditions(0).getData().toByteArray());
            return null;
        });
    }

    static long uploadCount(UUID entry) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_object_bindings WHERE entry_uuid=:entry")
                .setParameter("entry", entry).getSingleResult()).longValue());
    }

    @Test void sqlPublicationFailureLeavesVerifiedCandidateWithoutBlindRetry() {
        var request = request("bytes awaiting publication");
        UUID entry = ArchiveIds.entryUuid(request.getAddress());
        String function = "reject_managed_" + UUID.randomUUID().toString().replace("-", "");
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION " + function + "() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'injected managed publication failure' USING ERRCODE='40001'; END; $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER " + function + " BEFORE INSERT ON archive_version_object_refs "
                    + "FOR EACH ROW WHEN (NEW.entry_uuid='" + entry + "'::uuid) EXECUTE FUNCTION " + function + "()")
                    .executeUpdate();
        });
        try {
            assertThatThrownBy(() -> managed.putEntry(CALLER, request)).hasStackTraceContaining("injected managed publication failure");
            assertThat(ledger.findEntry(entry)).isEmpty();
            assertThat(uploadCount(entry)).isEqualTo(1);
            tx.readOnly(em -> {
                assertThat(em.createNativeQuery("SELECT u.state FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id) WHERE b.entry_uuid=:entry")
                        .setParameter("entry", entry).getSingleResult()).isEqualTo("VERIFIED");
                return null;
            });
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER " + function + " ON archive_version_object_refs").executeUpdate();
                em.createNativeQuery("DROP FUNCTION " + function + "()").executeUpdate();
            });
        }
    }

    @Test void managedModeRefusesUnintegratedWritePathsBeforeReadingInput() {
        var request = request("unused");
        assertThatThrownBy(() -> managed.uploadStream(CALLER, request.getAddress(), request.getRenditions(0).getRendition(),
                1, "", null, "", null, null, new java.io.InputStream() {
                    @Override public int read() { throw new AssertionError("Unadmitted input was consumed"); }
                })).isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        assertThatThrownBy(() -> managed.bridgeEntry(CALLER, BridgeEntryRequest.newBuilder().setAddress(request.getAddress()).build()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        assertThat(uploadCount(ArchiveIds.entryUuid(request.getAddress()))).isZero();
    }

    @Test void latestOnlyPublicationDropsTheOldReferenceWithoutDeletingBytesInline() {
        String archive = "latest-" + UUID.randomUUID();
        managed.createArchive(CALLER, CreateArchiveRequest.newBuilder().setArchive(Archive.newBuilder()
                .setAccountId("account").setName(archive).setDriveName("archive-drive")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE)).build());
        var original = request("original");
        var request = original.toBuilder().setAddress(original.getAddress().toBuilder().setArchive(archive)).build();
        var first = managed.putEntry(CALLER, request);
        var second = managed.putEntry(CALLER, request.toBuilder().setRenditions(0, request.getRenditions(0).toBuilder()
                .setData(ByteString.copyFromUtf8("replacement"))).build());
        UUID entry = UUID.fromString(first.getEntryUuid());
        var object = first.getManifest().getRenditions(0);
        assertThat(ledger.findVersion(entry, first.getVersion())).isEmpty();
        assertThat(ledger.findVersion(entry, second.getVersion())).isPresent();
        assertThat(managed.getEntry(CALLER, GetEntryRequest.newBuilder().setAddress(request.getAddress()).build())
                .getRenditions(0).getData()).isEqualTo(ByteString.copyFromUtf8("replacement"));
        assertThat(new ArchiveObjectLedger(tx).readable(entry, first.getVersion(), UUID.fromString(object.getStorageObjectId()))).isEmpty();
        assertThat(opened.store().get("managed-archive", object.getObjectKey()).data())
                .isEqualTo(ByteString.copyFromUtf8("original").toByteArray());
        var recovery = recovery();
        assertThat(recovery.recover(UUID.fromString(second.getManifest().getRenditions(0).getStorageObjectId()),
                java.time.Instant.now().plusSeconds(60))).isEqualTo(ArchiveObjectRecovery.Outcome.SKIPPED);
        assertThat(recovery.recover(UUID.fromString(object.getStorageObjectId()), java.time.Instant.now().plusSeconds(60)))
                .isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", object.getObjectKey()))
                .isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    static ArchiveObjectRecovery recovery() {
        return new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx), (generation, profile) -> {
            assertThat(generation).isEqualTo("original");
            assertThat(profile.storageRealm()).isEqualTo("original-realm");
            assertThat(profile.identity().location().get("endpoint")).isEqualTo(S3.getEndpoint().toString());
            return opened.reclaimer();
        });
    }

    @Test void failedCleanupRetriesOriginalBackendAndReconcilesLateWrites() {
        byte[] bytes = ByteString.copyFromUtf8("abandoned bytes").toByteArray();
        String key = "abandoned/" + UUID.randomUUID();
        var rendition = RenditionManifestEntry.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                .setState(RenditionState.RENDITION_STATE_PRESENT).setObjectKey(key).setSizeBytes(bytes.length)
                .setSha256(ArchiveManifests.sha256Hex(bytes)).build();
        var staged = new ArchiveObjectWriter(new ArchiveUploadLedger(tx), opened.store(), "original", opened.capabilities(), Duration.ofMinutes(1))
                .stage(UUID.randomUUID(), "account", "records", "managed-archive", rendition, "text/plain", bytes);
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                    .setParameter("id", staged.objectId()).executeUpdate();
        });
        var offline = new ArchiveObjectRecovery(new ArchiveCleanupLedger(tx), new ManagedBackendLedger(tx), (generation, profile) -> {
            throw new IllegalStateException("Original backend offline");
        });
        var cutoff = java.time.Instant.now().plusSeconds(60);
        assertThatThrownBy(() -> offline.recover(staged.objectId(), cutoff)).hasRootCauseMessage("Original backend offline");
        tx.readOnly(em -> {
            var row = (Object[]) em.createNativeQuery("SELECT state,cleanup_error FROM archive_object_uploads WHERE object_id=:id")
                    .setParameter("id", staged.objectId()).getSingleResult();
            assertThat(row).containsExactly("DELETING", "BACKEND_RECLAMATION_FAILED");
            return null;
        });
        assertThat(opened.store().get("managed-archive", key).data()).isEqualTo(bytes);
        assertThat(recovery().recover(staged.objectId(), cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
        // Simulate an expired upload whose provider request completes after reclamation.
        opened.store().put(new BlobStore.PutSpec("managed-archive", key, "text/plain", Map.of(), rendition.getSha256()), bytes);
        assertThat(new ArchiveCleanupLedger(tx).candidates(cutoff, 1000)).contains(staged.objectId());
        assertThat(recovery().recover(staged.objectId(), cutoff)).isEqualTo(ArchiveObjectRecovery.Outcome.RECLAIMED);
        assertThatThrownBy(() -> opened.store().get("managed-archive", key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }

    static PutEntryRequest request(String value) {
        return PutEntryRequest.newBuilder().setAddress(EntryAddress.newBuilder().setAccountId("account")
                .setArchive("records").setEntryId(UUID.randomUUID().toString()))
                .addRenditions(RenditionContent.newBuilder().setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setData(ByteString.copyFromUtf8(value))).build();
    }
}

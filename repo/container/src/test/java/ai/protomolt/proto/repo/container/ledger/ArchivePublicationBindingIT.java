package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.container.archive.*;
import com.google.protobuf.util.JsonFormat;
import java.time.Instant;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchivePublicationBindingIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void aManifestCannotPublishAnInventedStorageBinding() throws Exception {
        try (var database = database()) {
            var ledger = new ArchiveLedger(new Tx(database.entityManagerFactory()));
            var entry = entry();
            var version = version(entry, UUID.randomUUID());
            assertThatThrownBy(() -> ledger.commitSave(entry, 0, version, 0, ArchiveLedger.StatsDelta.none()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(ledger.findEntry(entry.entryUuid)).isEmpty();
            assertThat(ledger.findVersion(entry.entryUuid, 1)).isEmpty();
        }
    }

    @Test void verifiedPublicationAndCarryForwardCommitReferencesWithTheVersion() throws Exception {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            var entry = entry();
            var admission = admission(tx, entry);
            var upload = admission.upload();
            var uploads = new ArchiveUploadLedger(tx);
            var objects = new ArchiveObjectLedger(tx);
            assertThat(objects.readable(entry.entryUuid, 1, upload.objectId())).isEmpty();
            uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), "provider-revision", null);
            assertThat(objects.readable(entry.entryUuid, 1, upload.objectId())).isEmpty();
            var first = version(entry, upload.objectId());
            assertThatThrownBy(() -> ledger.commitSave(entry, 0, first, 0, ArchiveLedger.StatsDelta.none()))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            assertThat(ledger.findEntry(entry.entryUuid)).isEmpty();
            ledger.commitSave(entry, 0, first, 0, ArchiveLedger.StatsDelta.none(), Map.of(upload.objectId(), upload.leaseToken()));
            assertThat(entry.mutationRevision).isPositive()
                    .isEqualTo(ledger.findEntry(entry.entryUuid).orElseThrow().mutationRevision);
            assertThat(first.mutationRevision).isPositive()
                    .isEqualTo(ledger.findVersion(entry.entryUuid, 1).orElseThrow().mutationRevision);
            assertThat(referenceCount(tx, upload.objectId())).isEqualTo(1);
            var cleanup = new ArchiveCleanupLedger(tx);
            assertThat(cleanup.claim(upload.objectId(), Instant.now().plusSeconds(60))).isEmpty();
            assertThat(cleanup.candidates(Instant.now().plusSeconds(60), 1000)).doesNotContain(upload.objectId());
            var readable = objects.readable(entry.entryUuid, 1, upload.objectId()).orElseThrow();
            assertThat(readable.binding()).isEqualTo(admission.binding());
            assertThat(readable.size()).isEqualTo(7);
            assertThat(readable.sha256()).isEqualTo("a".repeat(64));
            assertThat(readable.providerVersion()).isEqualTo("provider-revision");
            assertThat(objects.readable(UUID.randomUUID(), 1, upload.objectId())).isEmpty();
            assertThat(objects.readable(entry.entryUuid, 2, upload.objectId())).isEmpty();
            assertThatThrownBy(() -> uploads.renew(upload.objectId(), upload.leaseToken(), Duration.ofMinutes(1)))
                    .isInstanceOf(ArchiveUploadLedger.FenceException.class);
            var current = ledger.findEntry(entry.entryUuid).orElseThrow();
            long previousRevision = current.mutationRevision;
            current.currentVersion = 2;
            var replacement = version(current, upload.objectId());
            ledger.commitSave(current, 1, replacement, 1, ArchiveLedger.StatsDelta.none());
            assertThat(current.mutationRevision).isGreaterThan(previousRevision)
                    .isEqualTo(ledger.findEntry(entry.entryUuid).orElseThrow().mutationRevision);
            assertThat(replacement.mutationRevision).isPositive()
                    .isEqualTo(ledger.findVersion(entry.entryUuid, 2).orElseThrow().mutationRevision);
            assertThat(ledger.findVersion(entry.entryUuid, 1)).isEmpty();
            assertThat(objects.readable(entry.entryUuid, 1, upload.objectId())).isEmpty();
            assertThat(objects.readable(entry.entryUuid, 2, upload.objectId())).contains(readable);
            assertThat(referenceCount(tx, upload.objectId())).isEqualTo(1);
            assertThat(referenceCount(tx, upload.objectId())).isEqualTo(1);
            // Simulate administratively removed references; LIVE alone must never authorize resurrection.
            tx.inTransaction(em -> {
                em.createNativeQuery("DELETE FROM archive_entries WHERE entry_uuid=:id")
                        .setParameter("id", entry.entryUuid).executeUpdate();
            });
            assertThat(referenceCount(tx, upload.objectId())).isZero();
            assertThat(objects.readable(entry.entryUuid, 2, upload.objectId())).isEmpty();
            var recreated = current;
            recreated.currentVersion = 1;
            var resurrected = version(recreated, upload.objectId());
            assertThatThrownBy(() -> ledger.commitSave(recreated, 0, resurrected, 0, ArchiveLedger.StatsDelta.none()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot be republished");
            assertThat(ledger.findEntry(entry.entryUuid)).isEmpty();
        }
    }

    @Test void rejectedBindingsDoNotPublishVersionsOrAlterUploadVerification() throws Exception {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            var owner = entry();
            var admission = admission(tx, owner);
            var upload = admission.upload();
            var uploads = new ArchiveUploadLedger(tx);
            uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), null, null);
            var other = entry();
            var stolen = version(other, upload.objectId());
            assertThatThrownBy(() -> ledger.commitSave(other, 0, stolen, 0, ArchiveLedger.StatsDelta.none(),
                    Map.of(upload.objectId(), upload.leaseToken())))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scope");
            var stripped = version(owner, upload.objectId());
            var builder = VersionManifest.newBuilder();
            JsonFormat.parser().merge(stripped.manifest, builder);
            builder.setRenditions(0, builder.getRenditions(0).toBuilder().clearStorageObjectId());
            stripped.manifest = JsonFormat.printer().print(builder.build());
            assertThatThrownBy(() -> ledger.commitSave(owner, 0, stripped, 0, ArchiveLedger.StatsDelta.none()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot lose");
            assertThat(ledger.findEntry(owner.entryUuid)).isEmpty();
            assertThat(ledger.findEntry(other.entryUuid)).isEmpty();
            assertThat(referenceCount(tx, upload.objectId())).isZero();
            assertThat(uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), null, null).state())
                    .isEqualTo("VERIFIED");
        }
    }

    private static ArchiveUploadLedger.Admission admission(Tx tx, ArchiveEntryRecord entry) {
        String generation = "publication-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of("https://storage.example", "us-east-1", true), generation));
        return new ArchiveUploadLedger(tx).begin(new ArchiveObjectLedger.Location(entry.entryUuid,
                entry.accountId, entry.archive, generation, "bucket", "unique-key"), 7, "text/plain", Duration.ofMinutes(1));
    }

    @Test void failureAfterMarkingUploadLiveRollsBackTheEntirePublication() throws Exception {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            var entry = entry();
            var upload = admission(tx, entry).upload();
            var uploads = new ArchiveUploadLedger(tx);
            uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), null, null);
            var version = version(entry, upload.objectId());
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION reject_archive_pin() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'injected archive pin failure'; END; $$
                        """).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER reject_archive_pin BEFORE INSERT ON archive_version_object_refs "
                        + "FOR EACH ROW WHEN (NEW.object_id='" + upload.objectId() + "'::uuid) EXECUTE FUNCTION reject_archive_pin()")
                        .executeUpdate();
            });
            try {
                assertThatThrownBy(() -> ledger.commitSave(entry, 0, version, 0, ArchiveLedger.StatsDelta.none(),
                        Map.of(upload.objectId(), upload.leaseToken())))
                        .hasStackTraceContaining("injected archive pin failure");
                assertThat(ledger.findEntry(entry.entryUuid)).isEmpty();
                assertThat(ledger.findVersion(entry.entryUuid, 1)).isEmpty();
                assertThat(referenceCount(tx, upload.objectId())).isZero();
                assertThat(uploads.verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), null, null).state())
                        .isEqualTo("VERIFIED");
            } finally {
                tx.inTransaction(em -> {
                    em.createNativeQuery("DROP TRIGGER reject_archive_pin ON archive_version_object_refs").executeUpdate();
                    em.createNativeQuery("DROP FUNCTION reject_archive_pin()").executeUpdate();
                });
            }
        }
    }
    private static long referenceCount(Tx tx, UUID objectId) {
        long nativeCount = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM archive_version_object_refs WHERE object_id=:id")
                .setParameter("id", objectId).getSingleResult()).longValue());
        long sharedCount = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_object_references WHERE object_id=:id AND owner_kind='ARCHIVE_VERSION'")
                .setParameter("id", objectId).getSingleResult()).longValue());
        assertThat(sharedCount).isEqualTo(nativeCount);
        return nativeCount;
    }

    @Test void publicationReferenceLockPreventsConcurrentCleanupClaim() throws Exception {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ArchiveLedger(tx);
            var entry = entry();
            var upload = admission(tx, entry).upload();
            new ArchiveUploadLedger(tx).verify(upload.objectId(), upload.leaseToken(), 7, "a".repeat(64), null, null);
            ledger.commitSave(entry, 0, version(entry, upload.objectId()), 0, ArchiveLedger.StatsDelta.none(),
                    Map.of(upload.objectId(), upload.leaseToken()));
            tx.inTransaction(em -> {
                em.createNativeQuery("DELETE FROM archive_version_object_refs WHERE object_id=:id")
                        .setParameter("id", upload.objectId()).executeUpdate();
            });
            var cleanup = new ArchiveCleanupLedger(tx);
            try (var connection = java.sql.DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                 var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                connection.setAutoCommit(false);
                try {
                    try (var insert = connection.prepareStatement("INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES (?,1,?)")) {
                        insert.setObject(1, entry.entryUuid);
                        insert.setObject(2, upload.objectId());
                        insert.executeUpdate();
                    }
                    var started = new java.util.concurrent.CountDownLatch(1);
                    var attempt = executor.submit(() -> {
                        started.countDown();
                        return cleanup.claim(upload.objectId(), Instant.now().plusSeconds(60));
                    });
                    assertThat(started.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(() -> attempt.get(200, java.util.concurrent.TimeUnit.MILLISECONDS))
                            .isInstanceOf(java.util.concurrent.TimeoutException.class);
                    connection.commit();
                    assertThat(attempt.get(5, java.util.concurrent.TimeUnit.SECONDS)).isEmpty();
                    assertThat(referenceCount(tx, upload.objectId())).isEqualTo(1);
                } finally { connection.rollback(); }
            }
        }
    }

    private static LedgerDatabase database() {
        return new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }
    private static ArchiveEntryRecord entry() {
        var entry = new ArchiveEntryRecord();
        entry.entryUuid = UUID.randomUUID();
        entry.entryId = entry.entryUuid.toString();
        entry.accountId = "account";
        entry.archive = "records";
        entry.currentVersion = 1;
        entry.createdAt = Instant.now();
        entry.updatedAt = entry.createdAt;
        return entry;
    }
    private static ArchiveVersionRecord version(ArchiveEntryRecord entry, UUID objectId) throws Exception {
        var manifest = VersionManifest.newBuilder().setAddress(EntryAddress.newBuilder()
                .setAccountId(entry.accountId).setArchive(entry.archive).setEntryId(entry.entryId))
                .setVersion(entry.currentVersion).setRootChecksum("a".repeat(64)).setTotalBytes(7)
                .addRenditions(RenditionManifestEntry.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setState(RenditionState.RENDITION_STATE_PRESENT).setSizeBytes(7)
                        .setSha256("a".repeat(64)).setObjectKey("unique-key").setStorageObjectId(objectId.toString())).build();
        var version = new ArchiveVersionRecord();
        version.entryUuid = entry.entryUuid;
        version.version = entry.currentVersion;
        version.manifest = JsonFormat.printer().print(manifest);
        version.rootChecksum = manifest.getRootChecksum();
        version.totalBytes = 7;
        version.createdAt = Instant.now();
        return version;
    }
}

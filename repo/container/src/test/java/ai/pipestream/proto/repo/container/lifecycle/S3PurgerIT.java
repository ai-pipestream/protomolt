package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentStatus;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase B drain against real PostgreSQL + LocalStack: happy path, the
 * revive race, staleness-guard timestamp precision, row-already-gone, and the
 * attempts/retry/DLQ ladder (via a poison-key failing BlobStore wrapper
 * around the real one).
 */
@Testcontainers(disabledWithoutDocker = true)
class S3PurgerIT extends AbstractLifecycleIT {

    /** A BlobStore that fails batch deletes containing the poison key. */
    private static final class PoisonStore implements BlobStore {
        private final BlobStore delegate;
        private final String poisonKey;

        PoisonStore(BlobStore delegate, String poisonKey) {
            this.delegate = delegate;
            this.poisonKey = poisonKey;
        }

        @Override
        public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
            if (keys.contains(poisonKey)) {
                throw new IllegalStateException("poison key in batch: " + poisonKey);
            }
            return delegate.deleteAll(bucket, keys);
        }

        @Override
        public PutResult put(PutSpec spec, byte[] body) {
            return delegate.put(spec, body);
        }

        @Override
        public PutResult put(PutSpec spec, java.io.InputStream body, long contentLength) {
            return delegate.put(spec, body, contentLength);
        }

        @Override
        public GetResult get(String bucket, String key, String versionId) {
            return delegate.get(bucket, key, versionId);
        }

        @Override
        public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
            delegate.copy(srcBucket, srcKey, dstBucket, dstKey);
        }

        @Override
        public boolean delete(String bucket, String key) {
            return delegate.delete(bucket, key);
        }

        @Override
        public List<ListedObject> list(String bucket, String prefix) {
            return delegate.list(bucket, prefix);
        }

        @Override
        public void headBucket(String bucket) {
            delegate.headBucket(bucket);
        }

        @Override
        public void headObject(String bucket, String key) {
            delegate.headObject(bucket, key);
        }
    }

    @Test
    void drainPurgesObjectsAndRowAndMarksRecord() {
        String account = "acct-purger-happy";
        DriveRecord drive = createDrive(account, "docs", "purger-happy", "pfx");
        UUID nodeId = UUID.randomUUID();
        List<String> keys = List.of("pfx/documents/" + account + "/" + nodeId + "/core.pb",
                "pfx/documents/" + account + "/" + nodeId + "/parsed.pb");
        DocumentRecord row = intakeRow(nodeId, account, "doc-happy", "ds-1", "docs", keys);
        documents.save(row);
        keys.forEach(k -> putObject(drive.bucket, k));
        // The intake row's raw blob rides the snapshot too.
        String rawBlobKey = PurgeSnapshots.rawBlobKey(drive.prefix, account, "doc-happy", "ds-1");
        putObject(drive.bucket, rawBlobKey);

        documents.tombstone(nodeId);
        DocumentPurgeRecord record = enqueuePurge(documents.findByNodeId(nodeId).orElseThrow(),
                drive.prefix, Instant.now());

        int purged = purger.drainOnce(store, 100);

        assertThat(purged).isEqualTo(1);
        assertThat(documents.findByNodeId(nodeId)).isEmpty();
        for (String key : keys) {
            assertThat(objectExists(drive.bucket, key)).isFalse();
        }
        assertThat(objectExists(drive.bucket, rawBlobKey)).isFalse();
        DocumentPurgeRecord settled = findPurge(record.purgeId).orElseThrow();
        assertThat(settled.status).isEqualTo(DocumentPurgeRecord.STATUS_PURGED);

        // Idempotent: the settled record is no longer PENDING, so a re-drain
        // is a no-op.
        assertThat(purger.drainOnce(store, 100)).isZero();
    }

    @Test
    void reviveRaceVoidsPurgeAndLeavesObjectsAndRowAlone() {
        String account = "acct-purger-revive";
        DriveRecord drive = createDrive(account, "docs", "purger-revive", "pfx");
        UUID nodeId = UUID.randomUUID();
        List<String> keys = List.of("pfx/documents/" + account + "/" + nodeId + "/core.pb");
        DocumentRecord row = intakeRow(nodeId, account, "doc-revive", "ds-2", "docs", keys);
        documents.save(row);
        keys.forEach(k -> putObject(drive.bucket, k));

        documents.tombstone(nodeId);
        // The purge was requested BEFORE the re-stage below.
        DocumentPurgeRecord record = enqueuePurge(documents.findByNodeId(nodeId).orElseThrow(),
                drive.prefix, Instant.now().minusSeconds(60));

        // The revive: a body re-stage flips AVAILABLE and bumps updated_at.
        DocumentRecord revived = documents.findByNodeId(nodeId).orElseThrow();
        revived.status = DocumentStatus.AVAILABLE;
        revived.updatedAt = Instant.now();
        documents.save(revived);

        int purged = purger.drainOnce(store, 100);

        assertThat(purged).isZero();
        DocumentPurgeRecord settled = findPurge(record.purgeId).orElseThrow();
        assertThat(settled.status).isEqualTo(DocumentPurgeRecord.STATUS_VOID);
        for (String key : keys) {
            assertThat(objectExists(drive.bucket, key)).isTrue();
        }
        DocumentRecord after = documents.findByNodeId(nodeId).orElseThrow();
        assertThat(after.status).isEqualTo(DocumentStatus.AVAILABLE);
    }

    @Test
    void stalenessGuardIsPreciseAboutRequestedAtVsUpdatedAt() {
        String account = "acct-purger-stale";
        DriveRecord drive = createDrive(account, "docs", "purger-stale", "pfx");
        Instant t = Instant.now();

        // (a) request BEFORE the last body write (updated_at > requested_at):
        // stale → VOID even though the row is still PENDING_PURGE.
        UUID staleNode = UUID.randomUUID();
        List<String> staleKeys = List.of("pfx/documents/" + account + "/" + staleNode + "/core.pb");
        DocumentRecord staleRow = intakeRow(staleNode, account, "doc-stale", "ds-3", "docs", staleKeys);
        staleRow.updatedAt = t;
        documents.save(staleRow);
        putObject(drive.bucket, staleKeys.get(0));
        documents.tombstone(staleNode);
        DocumentPurgeRecord staleRecord = enqueuePurge(
                documents.findByNodeId(staleNode).orElseThrow(), drive.prefix, t.minusSeconds(1));

        // (b) request AFTER the last body write (updated_at < requested_at):
        // not stale → purged. (requested_at is stored after the row write, so
        // plus-one-second is safely past any timestamptz micro truncation.)
        UUID freshNode = UUID.randomUUID();
        List<String> freshKeys = List.of("pfx/documents/" + account + "/" + freshNode + "/core.pb");
        DocumentRecord freshRow = intakeRow(freshNode, account, "doc-fresh", "ds-3", "docs", freshKeys);
        freshRow.updatedAt = t;
        documents.save(freshRow);
        putObject(drive.bucket, freshKeys.get(0));
        documents.tombstone(freshNode);
        DocumentPurgeRecord freshRecord = enqueuePurge(
                documents.findByNodeId(freshNode).orElseThrow(), drive.prefix,
                t.plusSeconds(1));

        int purged = purger.drainOnce(store, 100);

        assertThat(purged).isEqualTo(1);
        assertThat(findPurge(staleRecord.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_VOID);
        assertThat(objectExists(drive.bucket, staleKeys.get(0))).isTrue();
        assertThat(documents.findByNodeId(staleNode)).isPresent();
        assertThat(findPurge(freshRecord.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);
        assertThat(objectExists(drive.bucket, freshKeys.get(0))).isFalse();
        assertThat(documents.findByNodeId(freshNode)).isEmpty();
    }

    @Test
    void rowAlreadyGoneStillDeletesSnapshotObjectsAndMarksPurged() {
        String account = "acct-purger-gone";
        DriveRecord drive = createDrive(account, "docs", "purger-gone", "pfx");
        UUID nodeId = UUID.randomUUID();
        List<String> keys = List.of("pfx/documents/" + account + "/" + nodeId + "/core.pb");
        DocumentRecord row = intakeRow(nodeId, account, "doc-gone", "ds-4", "docs", keys);
        documents.save(row);
        keys.forEach(k -> putObject(drive.bucket, k));

        documents.tombstone(nodeId);
        DocumentPurgeRecord record = enqueuePurge(documents.findByNodeId(nodeId).orElseThrow(),
                drive.prefix, Instant.now());
        // A competing path (synchronous purge) removes the row first.
        documents.deleteByNodeId(nodeId);

        int purged = purger.drainOnce(store, 100);

        assertThat(purged).isEqualTo(1);
        assertThat(objectExists(drive.bucket, keys.get(0))).isFalse();
        assertThat(findPurge(record.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);
    }

    @Test
    void failedDrainIncrementsAttemptsAndRetrySucceeds() {
        String account = "acct-purger-retry";
        DriveRecord drive = createDrive(account, "docs", "purger-retry", "pfx");
        UUID nodeId = UUID.randomUUID();
        String poisonKey = "pfx/documents/" + account + "/" + nodeId + "/core.pb";
        List<String> keys = List.of(poisonKey,
                "pfx/documents/" + account + "/" + nodeId + "/parsed.pb");
        DocumentRecord row = intakeRow(nodeId, account, "doc-retry", "ds-5", "docs", keys);
        documents.save(row);
        keys.forEach(k -> putObject(drive.bucket, k));
        documents.tombstone(nodeId);
        DocumentPurgeRecord record = enqueuePurge(documents.findByNodeId(nodeId).orElseThrow(),
                drive.prefix, Instant.now());

        // Poisoned store: the batch delete blows up → attempts=1, still PENDING.
        assertThat(purger.drainOnce(new PoisonStore(store, poisonKey), 100)).isZero();
        DocumentPurgeRecord failed = findPurge(record.purgeId).orElseThrow();
        assertThat(failed.status).isEqualTo(DocumentPurgeRecord.STATUS_PENDING);
        assertThat(failed.attempts).isEqualTo(1);
        assertThat(failed.lastError).contains("poison key");
        // Nothing was deleted, nothing was voided.
        keys.forEach(k -> assertThat(objectExists(drive.bucket, k)).isTrue());
        assertThat(documents.findByNodeId(nodeId)).isPresent();

        // Retry with the healthy store: drains clean.
        assertThat(purger.drainOnce(store, 100)).isEqualTo(1);
        assertThat(findPurge(record.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);
        assertThat(documents.findByNodeId(nodeId)).isEmpty();
    }

    @Test
    void tenFailuresLandTheRecordInFailedAndTheRowInPurgeFailed() {
        String account = "acct-purger-dlq";
        DriveRecord drive = createDrive(account, "docs", "purger-dlq", "pfx");
        UUID nodeId = UUID.randomUUID();
        String poisonKey = "pfx/documents/" + account + "/" + nodeId + "/core.pb";
        DocumentRecord row = intakeRow(nodeId, account, "doc-dlq", "ds-6", "docs", List.of(poisonKey));
        documents.save(row);
        putObject(drive.bucket, poisonKey);
        documents.tombstone(nodeId);
        DocumentPurgeRecord record = enqueuePurge(documents.findByNodeId(nodeId).orElseThrow(),
                drive.prefix, Instant.now());

        BlobStore poisoned = new PoisonStore(store, poisonKey);
        for (int i = 1; i <= DocumentPurgeRecord.MAX_ATTEMPTS; i++) {
            assertThat(purger.drainOnce(poisoned, 100)).isZero();
            DocumentPurgeRecord current = findPurge(record.purgeId).orElseThrow();
            assertThat(current.attempts).isEqualTo(i);
            assertThat(current.status).isEqualTo(i < DocumentPurgeRecord.MAX_ATTEMPTS
                    ? DocumentPurgeRecord.STATUS_PENDING : DocumentPurgeRecord.STATUS_FAILED);
        }

        // The row landed in the DLQ too — out of every automatic retry path.
        assertThat(documents.findByNodeId(nodeId).orElseThrow().status)
                .isEqualTo(DocumentStatus.PURGE_FAILED);
        // FAILED records are never claimed again, so further drains are no-ops.
        assertThat(purger.drainOnce(store, 100)).isZero();
        assertThat(objectExists(drive.bucket, poisonKey)).isTrue();
    }
}

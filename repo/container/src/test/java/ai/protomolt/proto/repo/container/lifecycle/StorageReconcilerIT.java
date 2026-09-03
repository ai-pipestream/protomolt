package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reconciler's orphan diff against real LocalStack: an owned object, an
 * old orphan, a young orphan and a raw blob walk into a bucket — dry-run
 * reports and deletes nothing, armed deletes only the old orphan. S3 cannot
 * backdate objects, so the "old" orphan is aged by a list() wrapper around
 * the real store (deletes still go against real LocalStack).
 */
@Testcontainers(disabledWithoutDocker = true)
class StorageReconcilerIT extends AbstractLifecycleIT {

    /** Real store, but list() backdates one key — S3 cannot age objects. */
    private static final class AgingStore implements BlobStore {
        private final BlobStore delegate;
        private final String agedKey;
        private final long ageMillis;

        AgingStore(BlobStore delegate, String agedKey, long ageMillis) {
            this.delegate = delegate;
            this.agedKey = agedKey;
            this.ageMillis = ageMillis;
        }

        @Override
        public List<ListedObject> list(String bucket, String prefix) {
            return delegate.list(bucket, prefix).stream()
                    .map(o -> o.key().equals(agedKey)
                            ? new ListedObject(o.key(), o.sizeBytes(),
                                    o.lastModifiedEpochMs() - ageMillis)
                            : o)
                    .toList();
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
        public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
            return delegate.deleteAll(bucket, keys);
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
    void dryRunReportsAndArmedDeletesOnlyTheOldOrphan() {
        String account = "acct-reconcile";
        DriveRecord drive = createDrive(account, "docs", "reconcile-it", "pfx");
        String bucket = drive.bucket;
        UUID nodeId = UUID.randomUUID();
        String ownedKey = "pfx/documents/" + account + "/" + nodeId + "/core.pb";
        DocumentRecord row = intakeRow(nodeId, account, "doc-owned", "ds-1", "docs",
                List.of(ownedKey));
        documents.save(row);

        String oldOrphan = "pfx/documents/" + account + "/ghost-node/core.pb";
        String youngOrphan = "pfx/documents/" + account + "/fresh-node/core.pb";
        String rawBlob = "pfx/blobs/" + account + "/" + UUID.randomUUID() + ".bin";
        putObject(bucket, ownedKey);
        putObject(bucket, oldOrphan);
        putObject(bucket, youngOrphan);
        putObject(bucket, rawBlob);

        BlobStore aging = new AgingStore(store, oldOrphan, Duration.ofHours(3).toMillis());
        Duration minAge = Duration.ofHours(1);

        // Dry-run: reports the old orphan, skips the young one, deletes
        // nothing; owned and raw-blob keys are never orphans.
        StorageReconciler.ReconcileReport dry =
                reconciler.reconcile(aging, bucket, "pfx", minAge, true);
        assertThat(dry.scanned()).isEqualTo(4);
        assertThat(dry.orphans()).isEqualTo(1);
        assertThat(dry.orphanKeys()).containsExactly(oldOrphan);
        assertThat(dry.skippedTooYoung()).isEqualTo(1);
        assertThat(dry.deleted()).isZero();
        assertThat(dry.dryRun()).isTrue();
        assertThat(objectExists(bucket, oldOrphan)).isTrue();

        // Armed: only the old orphan goes away.
        StorageReconciler.ReconcileReport armed =
                reconciler.reconcile(aging, bucket, "pfx", minAge, false);
        assertThat(armed.orphans()).isEqualTo(1);
        assertThat(armed.deleted()).isEqualTo(1);
        assertThat(objectExists(bucket, oldOrphan)).isFalse();
        assertThat(objectExists(bucket, youngOrphan)).isTrue();
        assertThat(objectExists(bucket, ownedKey)).isTrue();
        assertThat(objectExists(bucket, rawBlob)).isTrue();

        // Second armed pass: nothing left to report.
        StorageReconciler.ReconcileReport again =
                reconciler.reconcile(aging, bucket, "pfx", minAge, false);
        assertThat(again.orphans()).isZero();
        assertThat(again.deleted()).isZero();
    }

    @Test
    void keysOfTombstonedRowsAreStillOwnedUntilThePurgerLands() {
        String account = "acct-reconcile-pending";
        DriveRecord drive = createDrive(account, "docs", "reconcile-pending", "pfx");
        String bucket = drive.bucket;
        UUID nodeId = UUID.randomUUID();
        String key = "pfx/documents/" + account + "/" + nodeId + "/core.pb";
        DocumentRecord row = intakeRow(nodeId, account, "doc-pending", "ds-2", "docs", List.of(key));
        documents.save(row);
        putObject(bucket, key);
        documents.tombstone(nodeId);

        // A PENDING_PURGE row still owns its keys: the purger, not the
        // reconciler, deletes them. Aging the key proves ownership, not the
        // min-age guard, is what protects it.
        BlobStore aging = new AgingStore(store, key, Duration.ofHours(3).toMillis());
        StorageReconciler.ReconcileReport report =
                reconciler.reconcile(aging, bucket, "pfx", Duration.ofHours(1), false);
        assertThat(report.orphans()).isZero();
        assertThat(objectExists(bucket, key)).isTrue();
    }
}

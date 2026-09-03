package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Reconciles a drive bucket against the ledger and reclaims orphans.
 * <p>
 * The object store is app-owned and intentionally non-ACID: object writes and
 * the row that references them are not committed atomically, and teardown
 * paths can drop rows without their objects (or the reverse). The standing
 * rule is "no live row references it → orphan → eligible for deletion"; this
 * is the sweep that enforces it, in the ledger → bucket direction (the
 * {@link CoherenceProbe} covers the other).
 * <p>
 * <b>The owned set</b> is the union of every document row's manifest-PRESENT
 * object keys, harvested over keyset pages of the whole ledger (rows of every
 * status count: a PENDING_PURGE row's keys are still owned until the purger
 * deletes them). Plus a key-shape convention: anything under a
 * {@code blobs/} namespace segment ({@code <drive.prefix>/blobs/...}) is
 * owned BY CONVENTION — raw blobs written by PutBlob and the upload route are
 * content-addressed and deliberately untracked by the ledger, so the
 * reconciler never flags them; their lifecycle is DeleteBlob's, not this
 * sweep's.
 * <p>
 * Two safety rails make deletion safe despite the non-ACID model:
 * <ul>
 *   <li><b>dry-run by default</b> — pass {@code dryRun=true} and the sweep
 *       only reports;</li>
 *   <li><b>min-age guard</b> — an object modified more recently than
 *       {@code minAge} is never swept, so an in-flight upload that has
 *       written its bytes but not yet committed its row is protected.</li>
 * </ul>
 */
public final class StorageReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(StorageReconciler.class);

    /** Cap on the orphan keys carried by the report (full count is always exact). */
    static final int ORPHAN_SAMPLE_LIMIT = 50;
    /** Ledger harvest page size. */
    private static final int ROW_PAGE_SIZE = 500;

    private final DocumentLedger documents;

    /**
     * @param documents the document-row ledger (the owned-key harvest)
     */
    public StorageReconciler(DocumentLedger documents) {
        this.documents = documents;
    }

    /**
     * Outcome of one reconciliation.
     *
     * @param scanned objects examined under the scope
     * @param orphans objects with no owning row and past the min-age guard
     * @param orphanKeys a bounded sample of orphan keys (capped at
     *        {@value #ORPHAN_SAMPLE_LIMIT}; {@code orphans} is the exact count)
     * @param skippedTooYoung objects with no owning row left alone because
     *        they were younger than the min-age guard
     * @param deleted objects actually deleted (0 on a dry run)
     * @param dryRun whether this run was report-only
     */
    public record ReconcileReport(long scanned, long orphans, List<String> orphanKeys,
            long skippedTooYoung, long deleted, boolean dryRun) {
    }

    /**
     * Reconcile one bucket scope against the ledger.
     *
     * @param store the object-storage port
     * @param bucket the bucket to scan
     * @param prefix the key prefix to scan under; empty scans the whole bucket
     * @param minAge never sweep objects younger than this ({@link Duration#ZERO}
     *        disables the guard — tests and forced sweeps)
     * @param dryRun {@code true} to report only; {@code false} to delete orphans
     * @return the reconciliation outcome
     */
    public ReconcileReport reconcile(BlobStore store, String bucket, String prefix,
            Duration minAge, boolean dryRun) {
        Set<String> owned = harvestOwnedKeys();
        List<BlobStore.ListedObject> objects = store.list(bucket, prefix);

        long cutoff = System.currentTimeMillis() - minAge.toMillis();
        long scanned = 0;
        long skippedTooYoung = 0;
        List<BlobStore.ListedObject> orphans = new ArrayList<>();
        List<String> sample = new ArrayList<>();
        for (BlobStore.ListedObject object : objects) {
            scanned++;
            if (owned.contains(object.key()) || isRawBlobKey(object.key())) {
                continue;
            }
            if (object.lastModifiedEpochMs() > cutoff) {
                skippedTooYoung++;
                continue;
            }
            orphans.add(object);
            if (sample.size() < ORPHAN_SAMPLE_LIMIT) {
                sample.add(object.key());
            }
        }

        long deleted = 0;
        if (!dryRun && !orphans.isEmpty()) {
            BlobStore.BatchDeleteResult result = store.deleteAll(bucket,
                    orphans.stream().map(BlobStore.ListedObject::key).toList());
            deleted = orphans.size() - result.failedKeys().size();
            if (!result.allSucceeded()) {
                LOG.warn("Reconcile of {}/{}: {} of {} orphan deletes failed: {}",
                        bucket, prefix, result.failedKeys().size(), orphans.size(), result.failedKeys());
            }
        }

        LOG.info("Reconcile of {}/{}: scanned={} orphans={} skippedTooYoung={} deleted={} dryRun={}",
                bucket, prefix, scanned, orphans.size(), skippedTooYoung, deleted, dryRun);
        return new ReconcileReport(scanned, orphans.size(), List.copyOf(sample),
                skippedTooYoung, deleted, dryRun);
    }

    /**
     * Every manifest-PRESENT object key of every document row, over keyset
     * pages (offset pagination could skip rows under concurrent writes and
     * mis-flag their live objects as orphans).
     */
    private Set<String> harvestOwnedKeys() {
        Set<String> keys = new HashSet<>();
        UUID after = null;
        List<DocumentRecord> page;
        do {
            page = documents.listPage(after, ROW_PAGE_SIZE);
            for (DocumentRecord row : page) {
                DocumentManifest manifest = row.readManifest();
                if (manifest != null) {
                    for (PartManifestEntry entry : manifest.getPartsList()) {
                        if (entry.getState() == PartState.PART_STATE_PRESENT
                                && !entry.getObjectKey().isBlank()) {
                            keys.add(entry.getObjectKey());
                        }
                    }
                }
                after = row.nodeId;
            }
        } while (page.size() == ROW_PAGE_SIZE);
        return keys;
    }

    /**
     * The raw-blob key-shape convention: {@code <drive.prefix>/blobs/...}
     * objects are content-addressed raw blobs the ledger deliberately does not
     * track (PutBlob, the upload route) — owned by convention, never orphaned.
     */
    static boolean isRawBlobKey(String key) {
        return key.startsWith("blobs/") || key.contains("/blobs/");
    }
}

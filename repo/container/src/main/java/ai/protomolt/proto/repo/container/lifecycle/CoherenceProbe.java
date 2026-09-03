package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The other direction of storage coherence: the ledger says a part object is
 * PRESENT, but the object is physically gone from the bucket (settlement
 * teardown, an out-of-band bucket cleanup, a purge that deleted bytes but
 * never finalized). Where {@link StorageReconciler} reclaims objects no row
 * owns, this probe repairs ROWS whose objects no longer exist.
 * <p>
 * For a bounded sample of AVAILABLE rows, every manifest-PRESENT key is
 * probed with a HEAD (existence only — a full GET of a multi-megabyte part
 * just to prove existence would be waste). A confirmed-missing object gets
 * its manifest entry tombstoned to {@code PART_STATE_DELETED} with
 * {@code deleted_reason = "COHERENCE_PROBE"} (size/sha/object_key retained,
 * per the manifest contract) and the row is re-saved.
 * <p>
 * The row's STATUS deliberately stays AVAILABLE: the remaining parts are
 * still valid and readable — the manifest now honestly says which parts are
 * gone, and readers (missing-part attribution in PartStorage) report exactly
 * that. Flipping the row would throw away good parts to punish a partial
 * loss. The probe also does NOT bump {@code updated_at} — this is bookkeeping,
 * not a body rewrite, and the purge staleness guard must not move for it.
 */
public final class CoherenceProbe {

    private static final Logger LOG = LoggerFactory.getLogger(CoherenceProbe.class);

    /** The {@code deleted_reason} stamped on probe-tombstoned manifest entries. */
    public static final String DELETED_REASON = "COHERENCE_PROBE";

    private final DocumentLedger documents;
    private final DriveLedger drives;

    /**
     * @param documents the document-row ledger (sample + manifest repair)
     * @param drives the drive ledger (drive name → bucket)
     */
    public CoherenceProbe(DocumentLedger documents, DriveLedger drives) {
        this.documents = documents;
        this.drives = drives;
    }

    /**
     * Outcome of one probe.
     *
     * @param rowsExamined AVAILABLE rows sampled
     * @param objectsChecked manifest-PRESENT objects HEAD-probed
     * @param missingByPart part name → count of confirmed-missing objects
     *        (and therefore of manifest entries tombstoned)
     */
    public record ProbeReport(int rowsExamined, int objectsChecked,
            Map<String, Integer> missingByPart) {

        /**
         * Total confirmed-missing objects across all parts.
         *
         * @return the total
         */
        public int totalMissing() {
            return missingByPart.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    /**
     * Probe a bounded sample of AVAILABLE rows and tombstone the manifest
     * entries of confirmed-missing objects.
     *
     * @param store the object-storage port
     * @param sampleSize the row-sample bound
     * @return the probe outcome
     */
    public ProbeReport probe(BlobStore store, int sampleSize) {
        List<DocumentRecord> sample = documents.listByStatus(DocumentStatus.AVAILABLE, sampleSize);
        int objectsChecked = 0;
        Map<String, Integer> missingByPart = new TreeMap<>();
        for (DocumentRecord row : sample) {
            DocumentManifest manifest = row.readManifest();
            if (manifest == null) {
                continue;
            }
            Optional<DriveRecord> drive = drives.findByName(row.accountId, row.driveName);
            if (drive.isEmpty()) {
                // Without the bucket nothing can be probed — and nothing
                // proven gone, so the row is left alone.
                LOG.warn("Coherence probe skipped node_id={}: drive '{}' gone (account {})",
                        row.nodeId, row.driveName, row.accountId);
                continue;
            }
            String bucket = drive.get().bucket;

            DocumentManifest.Builder repaired = manifest.toBuilder();
            boolean dirty = false;
            for (int i = 0; i < repaired.getPartsCount(); i++) {
                PartManifestEntry entry = repaired.getParts(i);
                if (entry.getState() != PartState.PART_STATE_PRESENT
                        || entry.getObjectKey().isBlank()) {
                    continue;
                }
                objectsChecked++;
                try {
                    store.headObject(bucket, entry.getObjectKey());
                } catch (BlobStore.BlobNotFoundException notFound) {
                    // Proven gone: tombstone the entry, keep its size/sha/key.
                    repaired.setParts(i, entry.toBuilder()
                            .setState(PartState.PART_STATE_DELETED)
                            .setDeletedReason(DELETED_REASON));
                    dirty = true;
                    missingByPart.merge(entry.getPart().name(), 1, Integer::sum);
                    LOG.warn("Coherence probe: object gone for node_id={} part={} key={} — "
                            + "manifest entry tombstoned", row.nodeId, entry.getPart(),
                            entry.getObjectKey());
                } catch (RuntimeException other) {
                    // A transient/other error proves nothing — do not tombstone.
                    LOG.warn("Coherence probe: HEAD of {} failed ({}), not tombstoning",
                            entry.getObjectKey(), other.getMessage());
                }
            }
            if (dirty) {
                row.writeManifest(repaired.build());
                documents.save(row);
            }
        }
        return new ProbeReport(sample.size(), objectsChecked, Map.copyOf(missingByPart));
    }
}

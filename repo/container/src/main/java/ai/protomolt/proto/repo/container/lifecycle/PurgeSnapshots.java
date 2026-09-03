package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;

import java.util.ArrayList;
import java.util.List;

/**
 * The object-key snapshot taken at tombstone time (Phase A of the two-phase
 * delete): every key Phase B must delete, captured ONCE so the purger never
 * recomputes — a body re-staged after the snapshot belongs to the revive, and
 * the staleness guard voids the purge instead of deleting the new body's keys.
 * <p>
 * The snapshot is the manifest's PRESENT part keys plus, for INTAKE rows
 * only, the derived raw-blob key
 * ({@code <drive.prefix>/blobs/<accountId>/<blobId>.bin}, the upload route's
 * layout). Only intake rows carry it: the raw blob belongs to the logical
 * document's intake artifact, and {@code blobId} is deterministic over
 * {@code (doc_id, datasource_id, account_id)} — a PIPELINE row of the same
 * logical document derives the SAME key, so letting pipeline rows snapshot it
 * could delete the raw blob out from under a surviving intake row. Deleting a
 * key that does not exist is success (NoSuchKey-is-success), so deriving it
 * for an intake row that never carried a raw blob is harmless.
 */
public final class PurgeSnapshots {

    private PurgeSnapshots() {
    }

    /**
     * Compute the purge snapshot for a tombstoned row.
     *
     * @param row the (tombstoned) document row
     * @param drivePrefix the row's drive key prefix (empty for bucket root);
     *        {@code null} when the drive is unresolvable — the raw-blob key is
     *        then simply not derivable and left out (a missing drive fails the
     *        drain anyway)
     * @return every object key Phase B must delete (may be empty)
     */
    public static List<String> objectKeysOf(DocumentRecord row, String drivePrefix) {
        List<String> keys = new ArrayList<>();
        DocumentManifest manifest = row.readManifest();
        if (manifest != null) {
            for (PartManifestEntry entry : manifest.getPartsList()) {
                if (entry.getState() == PartState.PART_STATE_PRESENT
                        && !entry.getObjectKey().isBlank()) {
                    keys.add(entry.getObjectKey());
                }
            }
        }
        if (DocumentRowKind.INTAKE.equals(row.rowKind) && drivePrefix != null) {
            keys.add(rawBlobKey(drivePrefix, row.accountId, row.docId, row.datasourceId));
        }
        return List.copyOf(keys);
    }

    /**
     * The raw-blob object key: {@code <drive.prefix>/blobs/<accountId>/<blobId>.bin}
     * — the same layout the HTTP upload route writes, with the deterministic
     * {@link DocumentIds#blobId}.
     *
     * @param drivePrefix the drive key prefix (empty for bucket root)
     * @param accountId the owning account
     * @param docId the document id
     * @param datasourceId the datasource id
     * @return the raw blob's object key
     */
    public static String rawBlobKey(String drivePrefix, String accountId, String docId,
            String datasourceId) {
        String prefix = drivePrefix == null ? "" : drivePrefix;
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String blobId = DocumentIds.blobId(docId, datasourceId, accountId).toString();
        return (prefix.isBlank() ? "" : prefix + "/") + "blobs/" + accountId + "/" + blobId + ".bin";
    }
}

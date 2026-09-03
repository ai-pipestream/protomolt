package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link PurgeSnapshots}: the tombstone-time key snapshot is the manifest's
 * PRESENT part keys plus — INTAKE rows only — the derived raw-blob key, and
 * the raw-blob key layout matches the upload route's
 * {@code <prefix>/blobs/<accountId>/<blobId>.bin}.
 */
class PurgeSnapshotsTest {

    private static final String ACCOUNT = "acct-1";
    private static final String DOC = "doc-1";
    private static final String DATASOURCE = "ds-1";

    @Test
    void intakeRowsSnapshotThePresentPartKeysPlusTheRawBlobKey() {
        DocumentRecord row = row(DocumentRowKind.INTAKE, manifest());
        String rawBlob = PurgeSnapshots.rawBlobKey("drive/pfx", ACCOUNT, DOC, DATASOURCE);

        List<String> keys = PurgeSnapshots.objectKeysOf(row, "drive/pfx");

        assertThat(keys).containsExactly("pfx/core.pb", "pfx/chunks/a-1.pb", rawBlob);
    }

    @Test
    void pipelineRowsNeverSnapshotTheRawBlob() {
        // The raw blob belongs to the intake artifact; a pipeline row of the
        // same logical document derives the SAME key, so snapshotting it here
        // could delete it out from under a surviving intake row.
        DocumentRecord row = row(DocumentRowKind.PIPELINE, manifest());

        assertThat(PurgeSnapshots.objectKeysOf(row, "drive/pfx"))
                .containsExactly("pfx/core.pb", "pfx/chunks/a-1.pb");
    }

    @Test
    void anUnresolvableDriveLeavesTheRawBlobKeyOut() {
        DocumentRecord row = row(DocumentRowKind.INTAKE, manifest());

        assertThat(PurgeSnapshots.objectKeysOf(row, null))
                .containsExactly("pfx/core.pb", "pfx/chunks/a-1.pb");
    }

    @Test
    void aRowWithoutManifestSnapshotsOnlyTheRawBlob() {
        DocumentRecord intake = row(DocumentRowKind.INTAKE, null);
        assertThat(PurgeSnapshots.objectKeysOf(intake, "pfx"))
                .containsExactly(PurgeSnapshots.rawBlobKey("pfx", ACCOUNT, DOC, DATASOURCE));

        DocumentRecord pipeline = row(DocumentRowKind.PIPELINE, null);
        assertThat(PurgeSnapshots.objectKeysOf(pipeline, "pfx")).isEmpty();
    }

    @Test
    void theSnapshotIsImmutable() {
        DocumentRecord row = row(DocumentRowKind.INTAKE, manifest());
        List<String> keys = PurgeSnapshots.objectKeysOf(row, "pfx");

        assertThatThrownBy(() -> keys.add("later"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rawBlobKeyNormalizesThePrefixAndDerivesTheDeterministicBlobId() {
        String blobId = DocumentIds.blobId(DOC, DATASOURCE, ACCOUNT).toString();

        assertThat(PurgeSnapshots.rawBlobKey("pfx", ACCOUNT, DOC, DATASOURCE))
                .isEqualTo("pfx/blobs/" + ACCOUNT + "/" + blobId + ".bin");
        // A trailing slash is not doubled.
        assertThat(PurgeSnapshots.rawBlobKey("pfx/", ACCOUNT, DOC, DATASOURCE))
                .isEqualTo("pfx/blobs/" + ACCOUNT + "/" + blobId + ".bin");
        // Bucket-root drives (empty or null prefix) have no leading slash.
        assertThat(PurgeSnapshots.rawBlobKey("", ACCOUNT, DOC, DATASOURCE))
                .isEqualTo("blobs/" + ACCOUNT + "/" + blobId + ".bin");
        assertThat(PurgeSnapshots.rawBlobKey(null, ACCOUNT, DOC, DATASOURCE))
                .isEqualTo("blobs/" + ACCOUNT + "/" + blobId + ".bin");
    }

    /** A manifest with one PRESENT core, one PRESENT chunk, and noise that must be skipped. */
    private static DocumentManifest manifest() {
        return DocumentManifest.newBuilder()
                .setDocVersion(1)
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setObjectKey("pfx/core.pb"))
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                        .setState(PartState.PART_STATE_DELETED) // not PRESENT: skipped
                        .setObjectKey("pfx/chunks/gone-1.pb"))
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                        .setState(PartState.PART_STATE_PRESENT) // blank key: skipped
                        .setObjectKey(" "))
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setObjectKey("pfx/chunks/a-1.pb"))
                .build();
    }

    private static DocumentRecord row(String rowKind, DocumentManifest manifest) {
        DocumentRecord row = new DocumentRecord();
        row.nodeId = UUID.randomUUID();
        row.docId = DOC;
        row.graphAddressId = DATASOURCE;
        row.accountId = ACCOUNT;
        row.graphId = DocumentRowKind.INTAKE.equals(rowKind) ? "intake:" + ACCOUNT : "graph-1";
        row.rowKind = rowKind;
        row.datasourceId = DATASOURCE;
        row.driveName = "docs";
        if (manifest != null) {
            row.writeManifest(manifest);
        }
        return row;
    }
}

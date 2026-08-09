package ai.pipestream.proto.repo.container.lifecycle;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link StorageReconciler#isRawBlobKey}: the key-shape convention that keeps
 * content-addressed raw blobs (anything under a {@code blobs/} namespace
 * segment) owned-by-convention so the sweep never flags them as orphans.
 * The ledger/bucket-backed sweep itself is covered by StorageReconcilerIT.
 */
class StorageReconcilerTest {

    @Test
    void keysUnderABlobsNamespaceSegmentAreOwnedByConvention() {
        assertThat(StorageReconciler.isRawBlobKey("blobs/acct-1/x.bin")).isTrue();
        assertThat(StorageReconciler.isRawBlobKey("pfx/blobs/acct-1/x.bin")).isTrue();
        assertThat(StorageReconciler.isRawBlobKey("a/b/c/blobs/x")).isTrue();
    }

    @Test
    void lookalikeKeysAreNotRawBlobs() {
        // A "blobs" substring that is not a full path segment must not count.
        assertThat(StorageReconciler.isRawBlobKey("blobsx/acct-1/x.bin")).isFalse();
        assertThat(StorageReconciler.isRawBlobKey("xblobs/acct-1/x.bin")).isFalse();
        assertThat(StorageReconciler.isRawBlobKey("pfx/blob/x.bin")).isFalse();
        assertThat(StorageReconciler.isRawBlobKey("documents/acct-1/node/core.pb")).isFalse();
        assertThat(StorageReconciler.isRawBlobKey("")).isFalse();
    }

    @Test
    void theOrphanSampleCapIsDocumentedAndBounded() {
        assertThat(StorageReconciler.ORPHAN_SAMPLE_LIMIT).isEqualTo(50);
    }

    @Test
    void theReportCarriesTheRunShape() {
        StorageReconciler.ReconcileReport report = new StorageReconciler.ReconcileReport(
                10, 3, List.of("a", "b", "c"), 2, 0, true);

        assertThat(report.scanned()).isEqualTo(10);
        assertThat(report.orphans()).isEqualTo(3);
        assertThat(report.orphanKeys()).containsExactly("a", "b", "c");
        assertThat(report.skippedTooYoung()).isEqualTo(2);
        assertThat(report.deleted()).isZero(); // a dry run deletes nothing
        assertThat(report.dryRun()).isTrue();
    }
}

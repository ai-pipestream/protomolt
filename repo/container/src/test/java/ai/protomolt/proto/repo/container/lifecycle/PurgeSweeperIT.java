package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sweeper's recovery scan: rows tombstoned with NO purge record (crashed
 * Phase A, or pre-lifecycle tombstones) get enqueued by {@code sweepOnce} and
 * then drained; rows that already have a PENDING record are left alone.
 */
@Testcontainers(disabledWithoutDocker = true)
class PurgeSweeperIT extends AbstractLifecycleIT {

    @Test
    void stuckTombstoneIsEnqueuedThenDrained() {
        String account = "acct-sweeper";
        DriveRecord drive = createDrive(account, "docs", "sweeper-it", "pfx");
        UUID nodeId = UUID.randomUUID();
        List<String> keys = List.of("pfx/documents/" + account + "/" + nodeId + "/core.pb");
        DocumentRecord row = intakeRow(nodeId, account, "doc-stuck", "ds-1", "docs", keys);
        documents.save(row);
        keys.forEach(k -> putObject(drive.bucket, k));
        // Hand-tombstoned: no purge record (the pre-lifecycle delete shape).
        documents.tombstone(nodeId);

        assertThat(sweeper.sweepOnce()).isEqualTo(1);
        // The enqueued record snapshots the row's manifest keys.
        List<DocumentPurgeRecord> claimed = queue.claimBatch(100);
        assertThat(claimed).hasSize(1);
        assertThat(claimed.get(0).nodeId).isEqualTo(nodeId);
        assertThat(claimed.get(0).readObjectKeys()).containsExactlyElementsOf(
                PurgeSnapshots.objectKeysOf(row, drive.prefix));

        // Claimed records are still PENDING (claim does not settle); the
        // drain finalizes everything.
        assertThat(purger.drainOnce(store, 100)).isEqualTo(1);
        assertThat(documents.findByNodeId(nodeId)).isEmpty();
        assertThat(objectExists(drive.bucket, keys.get(0))).isFalse();
        assertThat(findPurge(claimed.get(0).purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);

        // Nothing left to sweep.
        assertThat(sweeper.sweepOnce()).isZero();
    }

    @Test
    void rowWithPendingRecordIsNotDoubleEnqueued() {
        String account = "acct-sweeper-dupe";
        DriveRecord drive = createDrive(account, "docs", "sweeper-dupe", "pfx");
        UUID nodeId = UUID.randomUUID();
        DocumentRecord row = intakeRow(nodeId, account, "doc-dupe", "ds-2", "docs", List.of());
        documents.save(row);
        documents.tombstone(nodeId);
        DocumentPurgeRecord existing = enqueuePurge(documents.findByNodeId(nodeId).orElseThrow(),
                drive.prefix, Instant.now());

        assertThat(sweeper.sweepOnce()).isZero();
        List<DocumentPurgeRecord> claimed = queue.claimBatch(100);
        assertThat(claimed).extracting(r -> r.purgeId).containsExactly(existing.purgeId);

        // Settle it so no test order coupling leaks.
        assertThat(purger.drainOnce(store, 100)).isEqualTo(1);
    }
}

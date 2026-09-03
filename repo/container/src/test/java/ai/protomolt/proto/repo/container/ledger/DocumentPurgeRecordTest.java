package ai.protomolt.proto.repo.container.ledger;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DocumentPurgeRecord}'s object-key snapshot (de)serialization — the
 * JSON array Phase B deletes from verbatim — plus the queue-state defaults.
 */
class DocumentPurgeRecordTest {

    @Test
    void objectKeysRoundTripVerbatim() {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        List<String> keys = List.of(
                "docs/acct-1/node/core.pb",
                "docs/acct-1/node/chunks/title 384-x.pb",
                "prefix/blobs/acct-1/" + UUID.randomUUID() + ".bin",
                "unicode/ren\u00e9-\u4e2d\u6587\u6587\u4ef6.pb");

        record.writeObjectKeys(keys);

        assertThat(record.objectKeys).startsWith("[").endsWith("]");
        assertThat(record.readObjectKeys()).isEqualTo(keys);
    }

    @Test
    void anEmptySnapshotWritesAnEmptyArray() {
        DocumentPurgeRecord record = new DocumentPurgeRecord();

        record.writeObjectKeys(List.of());

        assertThat(record.objectKeys).isEqualTo("[]");
        assertThat(record.readObjectKeys()).isEmpty();
    }

    @Test
    void aNullOrBlankSnapshotReadsAsEmpty() {
        DocumentPurgeRecord record = new DocumentPurgeRecord();

        assertThat(record.readObjectKeys()).isEmpty();
        record.objectKeys = "  ";
        assertThat(record.readObjectKeys()).isEmpty();
    }

    @Test
    void anUnparseableSnapshotSurfacesAsLedgerExceptionNamingThePurge() {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        record.purgeId = UUID.randomUUID();
        record.objectKeys = "{ broken json";

        assertThatThrownBy(record::readObjectKeys)
                .isInstanceOf(LedgerException.class)
                .hasMessageContaining(record.purgeId.toString());
    }

    @Test
    void newRecordsStartPendingAtZeroAttempts() {
        DocumentPurgeRecord record = new DocumentPurgeRecord();

        assertThat(record.status).isEqualTo(DocumentPurgeRecord.STATUS_PENDING);
        assertThat(record.attempts).isZero();
        assertThat(record.relayedAt).isNull();
        assertThat(record.lastError).isNull();
        assertThat(DocumentPurgeRecord.MAX_ATTEMPTS).isEqualTo(10);
    }
}

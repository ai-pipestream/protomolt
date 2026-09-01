package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentEventRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRowKind;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import ai.pipestream.proto.repo.container.ledger.LedgerDatabase;
import ai.pipestream.proto.repo.container.ledger.Tx;
import ai.pipestream.proto.repo.v1.DocumentEvent;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.NodeAddress;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The document-events outbox (V4) against real testcontainers PostgreSQL 17:
 * enqueue-in-caller-tx (the transactional-outbox atomicity claim), the
 * SKIP-LOCKED claim scan's order, the conditional PUBLISHED transition, and
 * the attempts-to-FAILED ladder. Kafka never enters the picture here - the
 * relay has its own IT.
 */
@Testcontainers(disabledWithoutDocker = true)
class DocumentEventOutboxIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    static LedgerDatabase database;
    static Tx tx;
    static JdbcEventOutbox outbox;

    @BeforeAll
    static void boot() {
        database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        outbox = new JdbcEventOutbox(tx);
    }

    @AfterAll
    static void stop() {
        database.close();
    }

    @Test
    void enqueueRidesTheCallersTransaction() {
        DocumentRecord row = row("doc-outbox-commit");
        DocumentEventRecord record = DocumentEventFactory.saved(row, Instant.now());
        tx.inTransaction(em -> {
            outbox.enqueue(em, record);
        });

        DocumentEventRecord stored = outbox.findById(record.eventId).orElseThrow();
        assertThat(stored.status).isEqualTo(DocumentEventRecord.STATUS_PENDING);
        assertThat(stored.eventType).isEqualTo(DocumentEventRecord.TYPE_SAVED);
        assertThat(stored.kafkaKey).isEqualTo("doc-outbox-commit");
        assertThat(stored.attempts).isZero();
        assertThat(stored.createdAt).isNotNull();
        assertThat(stored.publishedAt).isNull();

        // The payload is the serialized DocumentEvent protobuf, event_id echoed.
        DocumentEvent event = parse(stored);
        assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
        assertThat(event.hasSaved()).isTrue();
        assertThat(event.getSaved().getAddress().getDocId()).isEqualTo("doc-outbox-commit");
        assertThat(event.getSaved().getChecksum()).isEqualTo(row.checksum);
        assertThat(event.getSaved().getDocVersion()).isEqualTo(3);
    }

    @Test
    void rollbackLeavesNoRow() {
        UUID eventId = UUID.randomUUID();
        DocumentRecord row = row("doc-outbox-rollback");
        // Cast disambiguates the Tx.inTransaction Consumer overload.
        assertThatThrownBy(() -> tx.inTransaction(
                (java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
                    DocumentEventRecord record = DocumentEventFactory.saved(row, Instant.now());
                    record.eventId = eventId;
                    outbox.enqueue(em, record);
                    // The mutation fails AFTER the outbox write: the event
                    // must roll back with it - that is the whole point of
                    // the outbox.
                    throw new IllegalStateException("simulated commit failure");
                })).isInstanceOf(IllegalStateException.class);

        assertThat(outbox.findById(eventId)).isEmpty();
    }

    @Test
    void claimBatchIsOldestPendingFirst() {
        String docId = "doc-outbox-order-" + UUID.randomUUID();
        DocumentEventRecord oldest = saved(docId, Instant.now().minusSeconds(60));
        DocumentEventRecord middle = saved(docId, Instant.now().minusSeconds(30));
        DocumentEventRecord newest = saved(docId, Instant.now());
        // Insert out of order; the claim scan must sort by created_at.
        tx.inTransaction(em -> {
            outbox.enqueue(em, middle);
            outbox.enqueue(em, newest);
            outbox.enqueue(em, oldest);
        });

        List<DocumentEventRecord> claimed = outbox.claimBatch(100).stream()
                .filter(r -> r.kafkaKey.equals(docId))
                .toList();
        assertThat(claimed).extracting(r -> r.eventId)
                .containsExactly(oldest.eventId, middle.eventId, newest.eventId);
    }

    @Test
    void markPublishedIsConditionalOnPending() {
        DocumentEventRecord record = saved("doc-outbox-publish", Instant.now());
        tx.inTransaction(em -> {
            outbox.enqueue(em, record);
        });

        Instant ack = Instant.now();
        assertThat(outbox.markPublished(record.eventId, ack)).isTrue();
        // A competing relay settling the same record is a no-op.
        assertThat(outbox.markPublished(record.eventId, ack)).isFalse();

        DocumentEventRecord stored = outbox.findById(record.eventId).orElseThrow();
        assertThat(stored.status).isEqualTo(DocumentEventRecord.STATUS_PUBLISHED);
        assertThat(stored.publishedAt).isNotNull();
        // PUBLISHED rows are retained, not deleted (same as the purge queue).
        assertThat(outbox.countByStatus().get(DocumentEventRecord.STATUS_PUBLISHED))
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    void failuresLandFailedAtTheAttemptsCeiling() {
        DocumentEventRecord record = saved("doc-outbox-dlq", Instant.now());
        tx.inTransaction(em -> {
            outbox.enqueue(em, record);
        });

        for (int i = 1; i < DocumentEventRecord.MAX_ATTEMPTS; i++) {
            var updated = outbox.markFailed(record, "broker down (attempt " + i + ")");
            assertThat(updated).isPresent();
            assertThat(updated.get().attempts).isEqualTo(i);
            assertThat(updated.get().status).isEqualTo(DocumentEventRecord.STATUS_PENDING);
        }
        var last = outbox.markFailed(record, "broker down (final)");
        assertThat(last).isPresent();
        assertThat(last.get().attempts).isEqualTo(DocumentEventRecord.MAX_ATTEMPTS);
        assertThat(last.get().status).isEqualTo(DocumentEventRecord.STATUS_FAILED);
        assertThat(last.get().lastError).isEqualTo("broker down (final)");

        // FAILED is the DLQ: the claim scan never picks it up again.
        assertThat(outbox.claimBatch(100)).noneMatch(r -> r.eventId.equals(record.eventId));
        // And a late markPublished cannot resurrect it.
        assertThat(outbox.markPublished(record.eventId, Instant.now())).isFalse();
    }

    /** A fresh DocumentSaved outbox row for {@code docId}. */
    private static DocumentEventRecord saved(String docId, Instant when) {
        return DocumentEventFactory.saved(row(docId), when);
    }

    private static DocumentEvent parse(DocumentEventRecord record) {
        try {
            return DocumentEvent.parseFrom(record.payload);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError("outbox payload did not parse", e);
        }
    }

    /** A minimal ledger row fixture (unsaved; the event factory only reads it). */
    private static DocumentRecord row(String docId) {
        DocumentRecord row = new DocumentRecord();
        row.nodeId = UUID.randomUUID();
        row.docId = docId;
        row.graphAddressId = "ds-1";
        row.accountId = "acct-outbox";
        row.graphId = "intake:acct-outbox";
        row.rowKind = DocumentRowKind.INTAKE;
        row.datasourceId = "ds-1";
        row.checksum = "sha256:" + docId;
        row.driveName = "intake";
        row.objectKey = "documents/acct-outbox/" + row.nodeId;
        row.sizeBytes = 42L;
        row.writeManifest(DocumentManifest.newBuilder()
                .setAddress(NodeAddress.newBuilder()
                        .setDocId(docId)
                        .setGraphAddressId("ds-1")
                        .setAccountId("acct-outbox")
                        .setGraphId("intake:acct-outbox"))
                .setDocVersion(3)
                .build());
        return row;
    }
}

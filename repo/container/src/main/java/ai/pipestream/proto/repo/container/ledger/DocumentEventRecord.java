package ai.pipestream.proto.repo.container.ledger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the transactional outbox (V4, {@code document_events_outbox}):
 * a serialized {@code DocumentEvent} protobuf awaiting publication to the
 * document-events Kafka topic.
 * <p>
 * Writers (save, hard delete, purge request, purge completion) insert one
 * record IN THE SAME TRANSACTION as the ledger mutation the event describes,
 * so the event stream can never drift from the ledger. The relay
 * ({@code EventRelay}) claims PENDING records
 * ({@code SELECT ... FOR UPDATE SKIP LOCKED}), publishes each to Kafka keyed
 * by {@link #kafkaKey} (the doc_id, so one document's events are
 * partition-ordered), and marks the record PUBLISHED after the broker ack.
 * <p>
 * Status machine: {@code PENDING} → {@code PUBLISHED} (acked; the row is
 * retained, not deleted, same as the purge queue retains PURGED rows) or
 * {@code FAILED} ({@link #MAX_ATTEMPTS} attempts exhausted; the FAILED record
 * IS the dead-letter queue for now - the relay deliberately never re-enqueues
 * it; recovery is operator territory).
 * <p>
 * Delivery is at-least-once: publish precedes the PUBLISHED transition, so a
 * relay crash between the two republishes on restart. Consumers dedupe on the
 * event id, which the {@code DocumentEvent.event_id} field carries.
 * <p>
 * The whole table is written only when Kafka is configured
 * ({@code DOCUMENT_PLATFORM_KAFKA_BOOTSTRAP_SERVERS}); unset, the commit
 * points skip the outbox entirely.
 */
@Entity
@Table(name = "document_events_outbox", check = {
        @jakarta.persistence.CheckConstraint(name = "chk_document_events_outbox_status",
                constraint = "status IN ('PENDING', 'PUBLISHED', 'FAILED')"),
        @jakarta.persistence.CheckConstraint(name = "chk_document_events_outbox_type",
                constraint = "event_type IN ('DocumentSaved', 'DocumentDeleted',"
                        + " 'PurgeRequested', 'DocumentPurged')")})
public class DocumentEventRecord {

    /** Queued, awaiting (or between) relay attempts. */
    public static final String STATUS_PENDING = "PENDING";
    /** Published and acked by the broker. Terminal; the row is retained. */
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    /** Relay failed {@link #MAX_ATTEMPTS} times. Terminal - the DLQ. */
    public static final String STATUS_FAILED = "FAILED";

    /** Attempts ceiling: a relay failure at or past it lands the record in FAILED. */
    public static final int MAX_ATTEMPTS = 10;

    /** Event type names, mirroring the DocumentEvent oneof arms. */
    public static final String TYPE_SAVED = "DocumentSaved";
    /** Event type name for the hard-delete event. */
    public static final String TYPE_DELETED = "DocumentDeleted";
    /** Event type name for the purge-request (tombstone) event. */
    public static final String TYPE_PURGE_REQUESTED = "PurgeRequested";
    /** Event type name for the purge-completion event. */
    public static final String TYPE_PURGED = "DocumentPurged";

    /** Default constructor required by the JPA/Hibernate persistence provider. */
    public DocumentEventRecord() {
    }

    /**
     * Surrogate event id, minted by the app per event; also carried on the
     * protobuf as {@code DocumentEvent.event_id} (the consumer dedupe key).
     */
    @Id
    @Column(name = "event_id", nullable = false)
    public UUID eventId;

    /** The DocumentEvent oneof arm name (see the TYPE_* constants). */
    @Column(name = "event_type", nullable = false)
    public String eventType;

    /** The serialized DocumentEvent protobuf. */
    @Column(name = "payload", nullable = false)
    public byte[] payload;

    /** The Kafka record key: the document's doc_id. */
    @Column(name = "kafka_key", nullable = false)
    public String kafkaKey;

    /** Relay attempts so far; at {@link #MAX_ATTEMPTS} the record goes FAILED. */
    @Column(name = "attempts", nullable = false)
    public int attempts;

    /** Queue state: PENDING (default), PUBLISHED or FAILED. */
    @Column(name = "status", nullable = false)
    public String status = STATUS_PENDING;

    /** When the event's transaction committed (the relay's drain order). */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    /** When the broker acked the record; null until PUBLISHED. */
    @Column(name = "published_at")
    public Instant publishedAt;

    /** The last relay failure's detail (truncated), for the DLQ record. */
    @Column(name = "last_error")
    public String lastError;
}

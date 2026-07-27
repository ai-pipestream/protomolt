-- Transactional outbox for Kafka document events.
--
-- Every lifecycle commit point (save, hard delete, purge request, purge
-- completion) inserts one row here IN THE SAME TRANSACTION as the ledger
-- mutation, so an event can never drift from the state change it describes.
-- The payload column carries the serialized DocumentEvent protobuf (see
-- repo/proto document_events.proto); event_type names the oneof arm inside
-- it. kafka_key is the doc_id: the relay publishes with it as the record key,
-- so one document's events are partition-ordered on the single
-- document-events topic.
--
-- status: PENDING (awaiting/between relay attempts) → PUBLISHED (acked by the
-- broker; retained, not deleted, same as the purge queue retains PURGED rows)
-- or FAILED (attempts exhausted; the FAILED record IS the dead-letter queue
-- for now - operator territory, the relay deliberately does not re-enqueue
-- it).
--
-- Delivery is at-least-once: the relay publishes first and marks PUBLISHED
-- after, so a crash mid-flight republishes on restart. Consumers dedupe on
-- the event id (the DocumentEvent.event_id field carries this row's event_id).

CREATE TABLE document_events_outbox (
    -- Surrogate event id, minted by the app (java.util.UUID) per event.
    event_id     UUID PRIMARY KEY,
    -- The DocumentEvent oneof arm: DocumentSaved, DocumentDeleted,
    -- PurgeRequested or DocumentPurged.
    event_type   TEXT NOT NULL,
    -- The serialized DocumentEvent protobuf.
    payload      BYTEA NOT NULL,
    -- The Kafka record key: the document's doc_id.
    kafka_key    TEXT NOT NULL,
    -- Relay attempts so far; attempts >= 10 flips status to FAILED.
    attempts     INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    -- When the event's transaction committed (the relay's drain order).
    created_at   TIMESTAMPTZ NOT NULL,
    -- When the broker acked the record; null until PUBLISHED.
    published_at TIMESTAMPTZ,
    -- The last relay failure's detail (truncated), for the DLQ record.
    last_error   TEXT,

    CONSTRAINT chk_document_events_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    CONSTRAINT chk_document_events_outbox_type CHECK (event_type IN
        ('DocumentSaved', 'DocumentDeleted', 'PurgeRequested', 'DocumentPurged'))
);

-- The claim scan: oldest PENDING first.
CREATE INDEX idx_document_events_outbox_status_created
    ON document_events_outbox (status, created_at);

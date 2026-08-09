-- Kafka purge queue (DOCUMENT_PLATFORM_PURGE_QUEUE=kafka): relay tracking.
--
-- The document_purges row stays the ledger of record when the queue is
-- Kafka-backed; the topic only distributes claims. relayed_at marks which
-- PENDING rows have already been published to the purge topic: the relay
-- selects PENDING rows with relayed_at IS NULL (FOR UPDATE SKIP LOCKED),
-- publishes each, and stamps relayed_at after the broker ack. Publish
-- precedes the stamp, so a crash mid-flight republishes on the next claim -
-- at-least-once delivery, tolerated because every terminal transition is
-- conditional on PENDING.
--
-- Null forever on the JDBC queue (the default), which claims rows directly.

ALTER TABLE document_purges ADD COLUMN relayed_at TIMESTAMPTZ;

-- The relay scan: oldest unrelayed PENDING first.
CREATE INDEX idx_document_purges_unrelayed ON document_purges (requested_at, purge_id)
    WHERE status = 'PENDING' AND relayed_at IS NULL;

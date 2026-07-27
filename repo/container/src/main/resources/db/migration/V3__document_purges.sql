-- Two-phase delete: the purge work queue.
--
-- Phase A (the gRPC delete call) tombstones the documents row to
-- PENDING_PURGE and inserts one row here IN THE SAME TRANSACTION, snapshotting
-- every object key to delete (manifest PRESENT part keys + the intake row's
-- raw blob key) so Phase B never recomputes them. Phase B (the background
-- purger) claims PENDING rows (SELECT ... FOR UPDATE SKIP LOCKED), re-checks
-- the document row's staleness guard, batch-deletes the snapshot keys, and
-- removes the document row.
--
-- status: PENDING (queued/retryable) → PURGED (objects + row gone),
-- VOID (the row was re-staged after the purge was requested — the purge is
-- cancelled, objects and row are left alone) or FAILED (attempts exhausted;
-- the FAILED record IS the dead-letter queue for now — operator territory,
-- the sweeper deliberately does not re-enqueue it).

CREATE TABLE document_purges (
    -- Surrogate record id, minted by the app (java.util.UUID) per purge
    -- request. Surrogate rather than node_id because rebirth makes identity
    -- non-unique over time: a deleted-and-re-ingested document mints the SAME
    -- deterministic node id, and each purge request gets its own row.
    purge_id         UUID PRIMARY KEY,
    -- The deterministic id of the documents row to purge. NOT unique: a
    -- revived-then-redeleted identity has one record per purge request.
    node_id          UUID NOT NULL,
    -- The four-segment storage identity, denormalized so a FAILED record
    -- still names what it tried to destroy after the documents row is gone.
    doc_id           TEXT NOT NULL,
    graph_address_id TEXT NOT NULL,
    account_id       TEXT NOT NULL,
    graph_id         TEXT NOT NULL,
    -- Drive whose bucket holds the objects (bucket resolved via the drives
    -- row at drain time).
    drive_name       TEXT NOT NULL,
    -- The snapshot: JSON array of every object key to delete, captured at
    -- tombstone time. Phase B never recomputes keys — a body re-staged after
    -- the snapshot belongs to the revive, and the staleness guard voids the
    -- purge instead of deleting the new body's keys.
    object_keys      JSONB NOT NULL,
    -- When the delete was requested; the staleness guard compares the
    -- documents row's updated_at against this.
    requested_at     TIMESTAMPTZ NOT NULL,
    -- Drain attempts so far; attempts >= 10 flips status to FAILED.
    attempts         INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    -- The last drain failure's detail (truncated), for the DLQ record.
    last_error       TEXT,

    CONSTRAINT chk_document_purges_status CHECK (status IN ('PENDING', 'PURGED', 'FAILED', 'VOID'))
);

-- The claim scan: oldest PENDING first.
CREATE INDEX idx_document_purges_status_requested ON document_purges (status, requested_at);

-- All purge requests of one identity (rebirth history, sweeper anti-join).
CREATE INDEX idx_document_purges_node ON document_purges (node_id);

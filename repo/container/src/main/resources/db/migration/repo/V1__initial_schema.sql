-- Claim-check document ledger — initial schema.
--
-- Fresh start (no migration history ported): the ledger stores one row per
-- stored document state plus the drives those states live under. Dropped
-- from the old model: the indexing ledger, the settlement roster, the
-- nodes/filesystem tree, and the monolithic-object fallback columns.

-- ============================================================================
-- drives: account-scoped object-storage roots (bucket + prefix) that
-- document part objects live under. (account_id, name) is unique because
-- document rows reference their drive by bare name.
-- ============================================================================
CREATE TABLE drives (
    drive_id        UUID PRIMARY KEY,
    account_id      TEXT NOT NULL,
    name            TEXT NOT NULL,
    drive_type      TEXT NOT NULL,
    provider        TEXT NOT NULL DEFAULT 's3',
    bucket          TEXT NOT NULL,
    prefix          TEXT NOT NULL DEFAULT '',
    region          TEXT,
    credentials_ref TEXT,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_drives_account_name UNIQUE (account_id, name),
    CONSTRAINT chk_drives_type CHECK (drive_type IN ('INTAKE', 'PIPELINE', 'CUSTOM'))
);

CREATE INDEX idx_drives_account ON drives (account_id);

-- ============================================================================
-- documents: one claim-check row per stored document state. The bytes live
-- in object storage as addressable part objects; this row is the map.
--
-- Storage identity: node_id is a caller-minted deterministic UUIDv5 over
-- hash(doc_id | graph_address_id | account_id | graph_id); the same four
-- segments form uq_documents_identity. The graph segment namespaces pipeline
-- hops so two graphs' same-named nodes never resolve to one row.
-- ============================================================================
CREATE TABLE documents (
    node_id                       UUID PRIMARY KEY,
    doc_id                        TEXT NOT NULL,
    graph_address_id              TEXT NOT NULL,
    graph_id                      TEXT NOT NULL,
    row_kind                      TEXT NOT NULL,
    cluster_id                    TEXT,
    account_id                    TEXT NOT NULL,
    datasource_id                 TEXT NOT NULL,
    connector_id                  TEXT,
    checksum                      TEXT NOT NULL,
    drive_name                    TEXT NOT NULL,
    object_key                    VARCHAR(1024) NOT NULL,
    version_id                    TEXT,
    etag                          TEXT NOT NULL,
    size_bytes                    BIGINT NOT NULL,
    content_type                  TEXT,
    filename                      TEXT,
    security                      JSONB,
    part_manifest                 JSONB,
    delete_source_blobs_on_settle BOOLEAN NOT NULL DEFAULT FALSE,
    source_blob_delete_reason     TEXT,
    status                        TEXT NOT NULL DEFAULT 'AVAILABLE',
    crawl_id                      TEXT,
    reprocess_count               INTEGER NOT NULL DEFAULT 0,
    last_reprocessed_at           TIMESTAMPTZ,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The four-segment storage identity.
    CONSTRAINT uq_documents_identity UNIQUE (doc_id, graph_address_id, account_id, graph_id),

    -- graph_id is required and non-blank on every row; blank-graph rows are
    -- unrepresentable. INTAKE rows carry the account's intake graph
    -- ('intake:<accountId>') and never a cluster; PIPELINE rows carry a real
    -- graph id. Mirrors the wire contract exactly — no inference from blank
    -- fields anywhere.
    CONSTRAINT chk_documents_row_kind CHECK (
        graph_id <> '' AND (
            (row_kind = 'INTAKE'   AND graph_id LIKE 'intake:%' AND cluster_id IS NULL)
         OR (row_kind = 'PIPELINE' AND graph_id NOT LIKE 'intake:%')
        )
    ),

    CONSTRAINT chk_documents_status CHECK (status IN ('AVAILABLE', 'PENDING_PURGE', 'PURGE_FAILED'))
);

-- Account-scoped document enumeration (the common admin/console read shape).
CREATE INDEX idx_documents_account_doc ON documents (account_id, doc_id);

-- All rows stored under one drive (drive maintenance, per-drive purge).
CREATE INDEX idx_documents_drive ON documents (drive_name);

-- All documents from one crawl run. Partial: most rows in non-crawl paths
-- carry no crawl context, so they don't belong in this index.
CREATE INDEX idx_documents_crawl ON documents (crawl_id) WHERE crawl_id IS NOT NULL;

-- Rows by producing connector. Partial for the same reason.
CREATE INDEX idx_documents_connector ON documents (connector_id) WHERE connector_id IS NOT NULL;

-- The purge work queue: only non-AVAILABLE rows are ever scanned, and they
-- are the small minority — keep the index partial so it stays tiny.
CREATE INDEX idx_documents_status ON documents (status) WHERE status <> 'AVAILABLE';

-- The intake replay frontier: all intake rows of one (account, datasource).
CREATE INDEX idx_documents_intake_frontier ON documents (account_id, datasource_id)
    WHERE row_kind = 'INTAKE';

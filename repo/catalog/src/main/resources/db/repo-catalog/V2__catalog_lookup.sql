-- Lookup fields of each document's current revision (requirements §2, §8).
-- The binary catalog_entry stays the record of the row; these columns and the
-- metadata table are its filterable and sortable derivation, written in the
-- commit transaction, cleared by a purge, and rebuilt from the stored parts on
-- demand. Lookup strings are bounded at admission (title 2000 bytes, metadata
-- keys 200 bytes, no NUL) so index rows always fit.
ALTER TABLE catalog_documents
    ADD COLUMN committed_nanos         bigint,
    ADD COLUMN lookup_title            text,
    ADD COLUMN lookup_document_type    text,
    ADD COLUMN lookup_source_uri       text,
    ADD COLUMN lookup_source_mime_type text,
    ADD COLUMN lookup_language         text,
    ADD COLUMN lookup_author           text,
    ADD COLUMN lookup_category         text,
    ADD COLUMN lookup_datasource_id    text,
    ADD COLUMN lookup_connector_id     text,
    ADD COLUMN owner_datasource_id     text,
    ADD COLUMN owner_module_id         text;

-- The exact commit time of the current revision as epoch nanoseconds, the value
-- the entry carries; updated_at keeps its microsecond copy.
UPDATE catalog_documents SET committed_nanos =
    (EXTRACT(EPOCH FROM updated_at) * 1000000000)::bigint;
ALTER TABLE catalog_documents ALTER COLUMN committed_nanos SET NOT NULL;

-- One index per sort direction: the tie-break on the call number is always
-- ascending, so a backward scan of one index cannot serve the other order.
CREATE INDEX catalog_documents_committed ON catalog_documents
    (account_id, committed_nanos, graph_id COLLATE "C", graph_address_id COLLATE "C", doc_id COLLATE "C");
CREATE INDEX catalog_documents_committed_desc ON catalog_documents
    (account_id, committed_nanos DESC, graph_id COLLATE "C", graph_address_id COLLATE "C", doc_id COLLATE "C");
CREATE INDEX catalog_documents_title ON catalog_documents
    (account_id, (COALESCE(lookup_title, '')) COLLATE "C", graph_id COLLATE "C", graph_address_id COLLATE "C", doc_id COLLATE "C");
CREATE INDEX catalog_documents_title_desc ON catalog_documents
    (account_id, (COALESCE(lookup_title, '')) COLLATE "C" DESC, graph_id COLLATE "C", graph_address_id COLLATE "C", doc_id COLLATE "C");

-- SearchMetadata.metadata of the current revision, one row per key. Replaced
-- with the lookup; cleared by a tombstone or a purge. A filter probes it by
-- document and key, which the primary key serves; values are not indexed.
CREATE TABLE catalog_document_metadata (
    document_id bigint NOT NULL REFERENCES catalog_documents,
    key         text NOT NULL CHECK (length(btrim(key)) > 0),
    value       text NOT NULL,
    PRIMARY KEY (document_id, key)
);

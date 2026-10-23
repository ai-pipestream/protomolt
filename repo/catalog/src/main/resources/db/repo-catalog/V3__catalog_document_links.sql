-- Document links (requirements §17).
--
-- The admitted structured_data type of each revision, the protobuf full name in
-- its type URL: NULL for a tombstone, for a revision without structured_data,
-- and for one whose structured_data was stored opaque. A link pinned to a
-- revision checks the target type here, reading no part bytes. It is derived
-- from the revision's CORE part, so a catalog rebuild may rewrite it; every
-- other column of a revision stays immutable.
ALTER TABLE catalog_revisions ADD COLUMN structured_data_type text
    CHECK (structured_data_type IS NULL OR length(btrim(structured_data_type)) > 0);

DROP TRIGGER catalog_revisions_immutable ON catalog_revisions;
CREATE FUNCTION protect_catalog_revisions() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'catalog_revisions rows are immutable'; END IF;
    IF (NEW.document_id, NEW.revision, NEW.previous_revision, NEW.operation_id, NEW.intent_sha256, NEW.tombstone,
            NEW.validation_status, NEW.committed_at, NEW.manifest, NEW.manifest_sha256)
            IS DISTINCT FROM (OLD.document_id, OLD.revision, OLD.previous_revision, OLD.operation_id, OLD.intent_sha256,
            OLD.tombstone, OLD.validation_status, OLD.committed_at, OLD.manifest, OLD.manifest_sha256)
            OR (NEW.tombstone AND NEW.structured_data_type IS NOT NULL) THEN
        RAISE EXCEPTION 'catalog_revisions rows are immutable; only a rebuild rewrites structured_data_type';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER catalog_revisions_immutable BEFORE UPDATE OR DELETE ON catalog_revisions
    FOR EACH ROW EXECUTE FUNCTION protect_catalog_revisions();

-- The static links of each document's current revision, one row per link
-- value: replaced on each accession that writes or sources CORE, cleared by a
-- tombstone or a purge, and rebuilt from the stored CORE part with its retained
-- schema. The target columns are a call number, never a foreign key, so a
-- target's tombstone or purge writes nothing here; dangling is computed by
-- joining catalog_documents at read time. Dynamic links write no row.
CREATE TABLE catalog_document_links (
    document_id             bigint  NOT NULL REFERENCES catalog_documents,
    field_path              text    NOT NULL CHECK (length(field_path) > 0),
    ordinal                 integer NOT NULL CHECK (ordinal >= 0),
    relation                text    NOT NULL CHECK (length(relation) > 0),
    target_account_id       text    NOT NULL,
    target_graph_id         text    NOT NULL,
    target_graph_address_id text    NOT NULL,
    target_doc_id           text    NOT NULL,
    pinned_revision         bigint  CHECK (pinned_revision IS NULL OR pinned_revision > 0),
    PRIMARY KEY (document_id, field_path, ordinal)
);

-- Backlinks: every edge into one call number, optionally of one relation.
CREATE INDEX catalog_document_links_target ON catalog_document_links
    (target_account_id, target_graph_id, target_graph_address_id, target_doc_id, relation);

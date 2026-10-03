ALTER TABLE documents ADD COLUMN pending_purge_id UUID;
ALTER TABLE document_purges
    ADD COLUMN completion_mode VARCHAR(16) NOT NULL DEFAULT 'ASYNC',
    ADD COLUMN generation_id UUID,
    ADD COLUMN content_checksum TEXT;
ALTER TABLE document_purges ADD CONSTRAINT chk_purge_completion_mode
    CHECK (completion_mode IN ('ASYNC', 'SYNCHRONOUS'));

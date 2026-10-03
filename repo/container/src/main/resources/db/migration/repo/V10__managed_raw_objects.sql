-- No legacy key is adopted implicitly. Existing documents retain their current
-- references until ingestion/migration explicitly registers managed objects.
CREATE TABLE raw_objects (
    raw_id UUID PRIMARY KEY,
    account_id TEXT NOT NULL CHECK (btrim(account_id) <> ''),
    backend_identity TEXT NOT NULL CHECK (btrim(backend_identity) <> ''),
    drive_id UUID NOT NULL,
    drive_name TEXT NOT NULL CHECK (btrim(drive_name) <> ''),
    bucket TEXT NOT NULL CHECK (btrim(bucket) <> ''),
    object_key TEXT NOT NULL CHECK (btrim(object_key) <> ''),
    expected_size BIGINT NOT NULL CHECK (expected_size >= 0),
    content_type TEXT NOT NULL CHECK (btrim(content_type) <> ''),
    state VARCHAR(16) NOT NULL CHECK (state IN ('STAGING', 'VERIFIED', 'LIVE', 'DELETING', 'DELETED')),
    lease_token UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    sha256 VARCHAR(64) CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    provider_version TEXT,
    etag TEXT,
    cleanup_token UUID,
    cleanup_attempts BIGINT NOT NULL DEFAULT 0 CHECK (cleanup_attempts >= 0),
    cleanup_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_raw_location UNIQUE (backend_identity, bucket, object_key),
    CONSTRAINT chk_raw_verified CHECK (state NOT IN ('VERIFIED', 'LIVE') OR sha256 IS NOT NULL),
    CONSTRAINT chk_raw_cleanup CHECK (state NOT IN ('DELETING', 'DELETED') OR cleanup_token IS NOT NULL)
);

CREATE TABLE document_raw_refs (
    node_id UUID NOT NULL REFERENCES documents(node_id) ON DELETE CASCADE,
    raw_id UUID NOT NULL REFERENCES raw_objects(raw_id),
    PRIMARY KEY (node_id, raw_id)
);
CREATE INDEX idx_document_raw_refs_raw ON document_raw_refs(raw_id);
CREATE INDEX idx_raw_objects_cleanup ON raw_objects(updated_at, raw_id);

-- Coordinates identify the originally selected backend, not whatever a mutable
-- drive happens to resolve to later. Verified byte identity is immutable too.
CREATE FUNCTION protect_raw_object_identity() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.raw_id, NEW.account_id, NEW.backend_identity, NEW.drive_id, NEW.drive_name,
           NEW.bucket, NEW.object_key, NEW.expected_size, NEW.content_type, NEW.lease_token, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.raw_id, OLD.account_id, OLD.backend_identity, OLD.drive_id, OLD.drive_name,
           OLD.bucket, OLD.object_key, OLD.expected_size, OLD.content_type, OLD.lease_token, OLD.created_at) THEN
        RAISE EXCEPTION 'managed raw object identity is immutable';
    END IF;
    IF OLD.sha256 IS NOT NULL AND
       ROW(NEW.sha256, NEW.provider_version, NEW.etag) IS DISTINCT FROM
       ROW(OLD.sha256, OLD.provider_version, OLD.etag) THEN
        RAISE EXCEPTION 'verified raw byte identity is immutable';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'STAGING' AND NEW.state IN ('VERIFIED', 'DELETING')) OR
        (OLD.state = 'VERIFIED' AND NEW.state IN ('LIVE', 'DELETING')) OR
        (OLD.state = 'LIVE' AND NEW.state = 'DELETING') OR
        (OLD.state = 'DELETING' AND NEW.state = 'DELETED') OR
        (OLD.state = 'DELETED' AND NEW.state = 'DELETING')
    ) THEN
        RAISE EXCEPTION 'invalid managed raw object state transition';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_raw_object_identity BEFORE UPDATE ON raw_objects
    FOR EACH ROW EXECUTE FUNCTION protect_raw_object_identity();

-- Bare binding reservations and legacy objects are not implicitly uploads.
CREATE TABLE archive_object_uploads (
    object_id UUID PRIMARY KEY REFERENCES archive_object_bindings(object_id),
    expected_size BIGINT NOT NULL CHECK (expected_size >= 0),
    content_type TEXT NOT NULL CHECK (btrim(content_type) <> ''),
    lease_token UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('STAGING','VERIFIED')),
    sha256 VARCHAR(64) CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    provider_version TEXT,
    etag TEXT,
    CHECK ((state = 'STAGING' AND sha256 IS NULL AND provider_version IS NULL AND etag IS NULL)
        OR (state = 'VERIFIED' AND sha256 IS NOT NULL))
);
CREATE INDEX archive_upload_expiry ON archive_object_uploads(lease_until, object_id);

CREATE FUNCTION protect_archive_upload_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Archive upload admission cannot be deleted';
    END IF;
    IF ROW(NEW.object_id,NEW.expected_size,NEW.content_type,NEW.lease_token)
       IS DISTINCT FROM ROW(OLD.object_id,OLD.expected_size,OLD.content_type,OLD.lease_token) THEN
        RAISE EXCEPTION 'Archive upload identity is immutable';
    END IF;
    IF OLD.state = 'VERIFIED' AND ROW(NEW.state,NEW.sha256,NEW.provider_version,NEW.etag)
       IS DISTINCT FROM ROW(OLD.state,OLD.sha256,OLD.provider_version,OLD.etag) THEN
        RAISE EXCEPTION 'Verified archive byte identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_upload_identity BEFORE UPDATE OR DELETE ON archive_object_uploads
FOR EACH ROW EXECUTE FUNCTION protect_archive_upload_identity();

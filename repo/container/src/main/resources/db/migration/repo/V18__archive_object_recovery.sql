ALTER TABLE archive_object_uploads
    ADD COLUMN cleanup_token uuid,
    ADD COLUMN cleanup_attempts bigint NOT NULL DEFAULT 0 CHECK (cleanup_attempts >= 0),
    ADD COLUMN cleanup_error varchar(128),
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT clock_timestamp();
ALTER TABLE archive_object_uploads DROP CONSTRAINT archive_upload_state;
ALTER TABLE archive_object_uploads DROP CONSTRAINT archive_upload_verified_bytes;
ALTER TABLE archive_object_uploads ADD CONSTRAINT archive_upload_state
    CHECK (state IN ('STAGING','VERIFIED','LIVE','DELETING','DELETED'));
ALTER TABLE archive_object_uploads ADD CONSTRAINT archive_upload_verified_bytes CHECK (
    (state='STAGING' AND sha256 IS NULL AND provider_version IS NULL AND etag IS NULL)
    OR (state IN ('VERIFIED','LIVE') AND sha256 IS NOT NULL)
    OR state IN ('DELETING','DELETED'));
ALTER TABLE archive_object_uploads ADD CONSTRAINT archive_cleanup_claim CHECK (
    (state IN ('DELETING','DELETED') AND cleanup_token IS NOT NULL AND cleanup_attempts > 0)
    OR (state IN ('STAGING','VERIFIED','LIVE') AND cleanup_token IS NULL AND cleanup_attempts=0 AND cleanup_error IS NULL));
CREATE INDEX archive_cleanup_age ON archive_object_uploads(updated_at,object_id);

CREATE OR REPLACE FUNCTION protect_archive_upload_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'Archive upload admission cannot be deleted'; END IF;
    IF ROW(NEW.object_id,NEW.expected_size,NEW.content_type,NEW.lease_token)
       IS DISTINCT FROM ROW(OLD.object_id,OLD.expected_size,OLD.content_type,OLD.lease_token) THEN
        RAISE EXCEPTION 'Archive upload identity is immutable';
    END IF;
    IF NOT (OLD.state='STAGING' AND NEW.state='VERIFIED') AND
       ROW(NEW.sha256,NEW.provider_version,NEW.etag) IS DISTINCT FROM ROW(OLD.sha256,OLD.provider_version,OLD.etag) THEN
        RAISE EXCEPTION 'Verified archive byte identity is immutable';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state='STAGING' AND NEW.state IN ('VERIFIED','DELETING')) OR
        (OLD.state='VERIFIED' AND NEW.state IN ('LIVE','DELETING')) OR
        (OLD.state='LIVE' AND NEW.state='DELETING') OR
        (OLD.state='DELETING' AND NEW.state='DELETED') OR
        (OLD.state='DELETED' AND NEW.state='DELETING')) THEN
        RAISE EXCEPTION 'Archive upload state transition is invalid; verified identity is immutable';
    END IF;
    IF NEW.state IN ('DELETING','DELETED') AND EXISTS (
        SELECT 1 FROM archive_version_object_refs WHERE object_id=NEW.object_id) THEN
        RAISE EXCEPTION 'Referenced archive objects cannot be reclaimed';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

-- Publication and reclamation serialize on the same upload row. A late or
-- direct reference insertion must not resurrect an object being reclaimed.
CREATE FUNCTION require_live_archive_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE upload_state text; owner_entry uuid;
BEGIN
    SELECT u.state,b.entry_uuid INTO upload_state,owner_entry
    FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id)
    WHERE u.object_id=NEW.object_id FOR UPDATE OF u;
    IF upload_state IS DISTINCT FROM 'LIVE' OR owner_entry IS DISTINCT FROM NEW.entry_uuid THEN
        RAISE EXCEPTION 'Archive reference requires a live object owned by the entry';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_reference_live BEFORE INSERT OR UPDATE ON archive_version_object_refs
FOR EACH ROW EXECUTE FUNCTION require_live_archive_reference();

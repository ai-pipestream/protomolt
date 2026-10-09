ALTER TABLE archive_object_uploads DROP CONSTRAINT archive_object_uploads_state_check;
ALTER TABLE archive_object_uploads DROP CONSTRAINT archive_object_uploads_check;
ALTER TABLE archive_object_uploads ADD CONSTRAINT archive_upload_state
    CHECK (state IN ('STAGING','VERIFIED','LIVE'));
ALTER TABLE archive_object_uploads ADD CONSTRAINT archive_upload_verified_bytes
    CHECK ((state = 'STAGING' AND sha256 IS NULL AND provider_version IS NULL AND etag IS NULL)
        OR (state IN ('VERIFIED','LIVE') AND sha256 IS NOT NULL));

CREATE OR REPLACE FUNCTION protect_archive_upload_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Archive upload admission cannot be deleted';
    END IF;
    IF ROW(NEW.object_id,NEW.expected_size,NEW.content_type,NEW.lease_token)
       IS DISTINCT FROM ROW(OLD.object_id,OLD.expected_size,OLD.content_type,OLD.lease_token) THEN
        RAISE EXCEPTION 'Archive upload identity is immutable';
    END IF;
    IF OLD.state IN ('VERIFIED','LIVE') AND ROW(NEW.sha256,NEW.provider_version,NEW.etag)
       IS DISTINCT FROM ROW(OLD.sha256,OLD.provider_version,OLD.etag) THEN
        RAISE EXCEPTION 'Verified archive byte identity is immutable';
    END IF;
    IF NEW.state <> OLD.state AND NOT (
        (OLD.state = 'STAGING' AND NEW.state = 'VERIFIED') OR
        (OLD.state = 'VERIFIED' AND NEW.state = 'LIVE')) THEN
        RAISE EXCEPTION 'Archive upload state transition is invalid; verified identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TABLE archive_version_object_refs (
    entry_uuid UUID NOT NULL,
    version BIGINT NOT NULL,
    object_id UUID NOT NULL REFERENCES archive_object_bindings(object_id),
    PRIMARY KEY (entry_uuid,version,object_id),
    FOREIGN KEY (entry_uuid,version) REFERENCES archive_versions(entry_uuid,version) ON DELETE CASCADE
);
CREATE INDEX archive_version_object_refs_object ON archive_version_object_refs(object_id);

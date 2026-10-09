-- Stop/drain V30 readers before rollout. Legacy pins retain UNKNOWN ownership.
LOCK TABLE archive_read_pins IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE repository_reader_incarnations (
    incarnation UUID PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE','FENCED','UNKNOWN'))
);
INSERT INTO repository_reader_incarnations
    SELECT DISTINCT reader_incarnation,'UNKNOWN' FROM archive_read_pins;
ALTER TABLE archive_read_pins ADD CONSTRAINT archive_reader_incarnation_fk
    FOREIGN KEY(reader_incarnation) REFERENCES repository_reader_incarnations(incarnation);

CREATE FUNCTION guard_repository_reader_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Reader incarnation tombstones are permanent'; END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.state <> 'ACTIVE' THEN RAISE EXCEPTION 'New reader incarnation must be ACTIVE'; END IF;
        RETURN NEW;
    END IF;
    IF NEW.incarnation IS DISTINCT FROM OLD.incarnation OR
        NOT (NEW.state=OLD.state OR (OLD.state='ACTIVE' AND NEW.state='FENCED')) THEN
        RAISE EXCEPTION 'Reader incarnation identity and fence are permanent';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_reader_incarnation_guard BEFORE INSERT OR UPDATE OR DELETE
    ON repository_reader_incarnations FOR EACH ROW EXECUTE FUNCTION guard_repository_reader_incarnation();

CREATE FUNCTION require_active_repository_reader(id UUID) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE current_state TEXT;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT state INTO current_state FROM repository_reader_incarnations WHERE incarnation=id FOR SHARE;
    IF NOT FOUND OR current_state <> 'ACTIVE' THEN
        RAISE EXCEPTION 'Reader incarnation is not ACTIVE';
    END IF;
END;
$$;

CREATE FUNCTION fence_repository_reader(id UUID) RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE current_state TEXT;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT state INTO current_state FROM repository_reader_incarnations WHERE incarnation=id FOR UPDATE;
    IF NOT FOUND OR current_state='UNKNOWN' THEN
        RAISE EXCEPTION 'Reader incarnation is unknown';
    END IF;
    IF current_state='ACTIVE' THEN
        UPDATE repository_reader_incarnations SET state='FENCED' WHERE incarnation=id;
    END IF;
    RETURN true;
END;
$$;

-- One client statement per short admission/release transaction. Existing triggers
-- remain authoritative for direct writes. No provider I/O or expiry is introduced.
CREATE OR REPLACE FUNCTION acquire_archive_read_pin(p_pin UUID, p_reader UUID, p_entry UUID, p_version BIGINT, p_object UUID)
RETURNS TABLE(object_id UUID, entry_uuid UUID, account_id TEXT, archive TEXT, backend_generation TEXT,
    bucket TEXT, object_key TEXT, storage_realm TEXT, expected_size BIGINT, sha256 TEXT, provider_version TEXT)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE closed BOOLEAN;
BEGIN
    PERFORM require_repository_read_committed();
    IF p_pin IS NULL OR p_reader IS NULL OR p_entry IS NULL OR p_object IS NULL OR p_version IS NULL OR p_version<=0 THEN
        RAISE EXCEPTION 'Archive read admission requires complete identity and a positive version';
    END IF;
    PERFORM require_active_repository_reader(p_reader);
    PERFORM 1 FROM archive_object_uploads u WHERE u.object_id=p_object FOR SHARE;
    IF NOT FOUND THEN RETURN; END IF;
    SELECT r.retiring INTO STRICT closed FROM repository_object_retention r WHERE r.object_id=p_object FOR SHARE;
    IF closed THEN RETURN; END IF;
    SELECT b.object_id,b.entry_uuid,b.account_id,b.archive,b.backend_generation,
        b.bucket,b.object_key,b.storage_realm,u.expected_size,u.sha256,u.provider_version
        INTO object_id,entry_uuid,account_id,archive,backend_generation,
            bucket,object_key,storage_realm,expected_size,sha256,provider_version
        FROM archive_object_bindings b
        JOIN archive_object_uploads u ON u.object_id=b.object_id AND u.state='LIVE'
        JOIN archive_version_object_refs r ON r.object_id=b.object_id AND r.entry_uuid=b.entry_uuid
        WHERE r.entry_uuid=p_entry AND r.version=p_version AND b.object_id=p_object;
    IF NOT FOUND THEN RETURN; END IF;
    INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
        VALUES(p_pin,p_reader,p_object,p_entry,p_version);
    RETURN NEXT;
END;
$$;


CREATE OR REPLACE FUNCTION guard_archive_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; closed BOOLEAN;
BEGIN
    IF TG_OP='INSERT' THEN PERFORM require_active_repository_reader(NEW.reader_incarnation); END IF;
    IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Archive read pin identity is immutable'; END IF;
    id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
    PERFORM share_archive_retention_owner(id);
    SELECT retiring INTO STRICT closed FROM repository_object_retention WHERE object_id=id FOR SHARE;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    IF closed OR NOT EXISTS(SELECT 1 FROM archive_object_uploads WHERE object_id=id AND state='LIVE')
        OR NOT EXISTS(SELECT 1 FROM archive_version_object_refs
            WHERE object_id=id AND entry_uuid=NEW.entry_uuid AND version=NEW.version) THEN
        RAISE EXCEPTION 'Archive read pin requires an open retained version';
    END IF;
    RETURN NEW;
END;
$$;

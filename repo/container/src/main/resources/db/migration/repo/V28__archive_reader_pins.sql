-- Drain older readers/recovery processes before rollout; they do not acquire pins.
LOCK TABLE archive_object_uploads, archive_version_object_refs, archive_mutation_targets,
    repository_object_retention, repository_object_references IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE archive_read_pins (
    pin_id UUID PRIMARY KEY,
    reader_incarnation UUID NOT NULL,
    object_id UUID NOT NULL REFERENCES archive_object_uploads(object_id),
    entry_uuid UUID NOT NULL,
    version BIGINT NOT NULL CHECK(version>0),
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- Deliberately no FK to a version: logical deletion must not release active reads.
CREATE INDEX archive_read_pin_object ON archive_read_pins(object_id);
CREATE INDEX archive_read_pin_incarnation ON archive_read_pins(reader_incarnation);
ALTER TABLE repository_object_references DROP CONSTRAINT repository_object_references_owner_kind_check;
ALTER TABLE repository_object_references ADD CONSTRAINT repository_object_references_owner_kind_check
    CHECK(owner_kind IN ('ARCHIVE_VERSION','DOCUMENT_HISTORY','DOCUMENT_CURRENT','ARCHIVE_READER'));

CREATE OR REPLACE FUNCTION repository_native_reference_exists(id UUID, kind TEXT, owner UUID, revision BIGINT)
RETURNS BOOLEAN LANGUAGE sql VOLATILE AS $$
    SELECT CASE kind
    WHEN 'ARCHIVE_VERSION' THEN EXISTS(
        SELECT 1 FROM archive_version_object_refs r WHERE r.object_id=id AND r.entry_uuid=owner AND r.version=revision)
    WHEN 'DOCUMENT_HISTORY' THEN EXISTS(
        SELECT 1 FROM repository_physical_locations p JOIN document_part_publication_history h ON h.attempt_id=p.source_id
        WHERE p.object_id=id AND p.source_kind='DOCUMENT_PART' AND h.attempt_id=owner AND h.publication_revision=revision)
    WHEN 'DOCUMENT_CURRENT' THEN EXISTS(
        SELECT 1 FROM repository_physical_locations p JOIN document_part_publications c ON c.attempt_id=p.source_id
        JOIN document_part_publication_history h ON h.attempt_id=c.attempt_id
        WHERE p.object_id=id AND p.source_kind='DOCUMENT_PART' AND c.node_id=owner AND h.publication_revision=revision)
    WHEN 'ARCHIVE_READER' THEN EXISTS(
        SELECT 1 FROM archive_read_pins p WHERE p.object_id=id AND p.pin_id=owner AND p.version=revision)
    ELSE false END
$$;

CREATE FUNCTION guard_archive_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; closed BOOLEAN;
BEGIN
    IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Archive read pin identity is immutable'; END IF;
    id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
    PERFORM lock_repository_retention_owner(id);
    SELECT retiring INTO STRICT closed FROM repository_object_retention WHERE object_id=id FOR UPDATE;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    IF closed OR NOT EXISTS(SELECT 1 FROM archive_object_uploads WHERE object_id=id AND state='LIVE')
        OR NOT EXISTS(SELECT 1 FROM archive_version_object_refs
            WHERE object_id=id AND entry_uuid=NEW.entry_uuid AND version=NEW.version) THEN
        RAISE EXCEPTION 'Archive read pin requires an open retained version';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_read_pin_guard BEFORE INSERT OR UPDATE OR DELETE ON archive_read_pins
    FOR EACH ROW EXECUTE FUNCTION guard_archive_read_pin();
CREATE FUNCTION mirror_archive_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        INSERT INTO repository_object_references VALUES(NEW.object_id,'ARCHIVE_READER',NEW.pin_id,NEW.version);
        RETURN NEW;
    END IF;
    DELETE FROM repository_object_references WHERE object_id=OLD.object_id AND owner_kind='ARCHIVE_READER'
        AND owner_id=OLD.pin_id AND owner_revision=OLD.version;
    RETURN OLD;
END;
$$;
CREATE TRIGGER archive_read_pin_mirror AFTER INSERT OR DELETE ON archive_read_pins
    FOR EACH ROW EXECUTE FUNCTION mirror_archive_read_pin();

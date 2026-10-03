-- Existing durable owners only. No reader, schema or raw retention API is enabled.
-- Drain old writer/recovery processes before rollout, including in-flight I/O.
LOCK TABLE repository_physical_locations, archive_object_uploads, archive_version_object_refs,
    archive_mutation_targets, document_part_attempts, document_part_attempt_objects,
    document_part_publication_history, document_part_publications, document_part_attempt_cleanup
    IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE repository_object_retention (
    object_id UUID PRIMARY KEY REFERENCES repository_physical_locations(object_id),
    reclaiming BOOLEAN NOT NULL DEFAULT false
);
CREATE TABLE repository_object_references (
    object_id UUID NOT NULL REFERENCES repository_object_retention(object_id),
    owner_kind TEXT NOT NULL CHECK(owner_kind IN ('ARCHIVE_VERSION','DOCUMENT_HISTORY','DOCUMENT_CURRENT')),
    owner_id UUID NOT NULL,
    owner_revision BIGINT NOT NULL CHECK(owner_revision>0),
    PRIMARY KEY(object_id,owner_kind,owner_id,owner_revision)
);
CREATE INDEX repository_reference_owner ON repository_object_references(owner_kind,owner_id,owner_revision);

-- Source-owner locks precede retention locks on every acquisition and cleanup path.
CREATE FUNCTION lock_repository_retention_owner(id UUID) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE location repository_physical_locations%ROWTYPE;
BEGIN
    SELECT * INTO STRICT location FROM repository_physical_locations WHERE object_id=id;
    IF location.source_kind='ARCHIVE' THEN
        PERFORM 1 FROM archive_object_uploads WHERE object_id=id FOR UPDATE;
    ELSE
        PERFORM 1 FROM document_part_attempts WHERE attempt_id=location.source_id FOR UPDATE;
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION 'Repository retention requires a durable source owner'; END IF;
END;
$$;
CREATE FUNCTION repository_native_reference_exists(id UUID, kind TEXT, owner UUID, revision BIGINT)
-- Mirror triggers inspect native changes made in the same transaction.
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
    ELSE false END
$$;
CREATE FUNCTION guard_repository_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; fenced BOOLEAN;
BEGIN
    IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Repository reference identity is immutable'; END IF;
    id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
    PERFORM lock_repository_retention_owner(id);
    SELECT reclaiming INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF fenced THEN RAISE EXCEPTION 'Repository object is permanently fenced for reclamation'; END IF;
        IF NOT repository_native_reference_exists(id,NEW.owner_kind,NEW.owner_id,NEW.owner_revision) THEN
            RAISE EXCEPTION 'Repository reference requires its exact durable native owner';
        END IF;
        RETURN NEW;
    END IF;
    IF repository_native_reference_exists(id,OLD.owner_kind,OLD.owner_id,OLD.owner_revision) THEN
        RAISE EXCEPTION 'Repository reference cannot release a retained native owner';
    END IF;
    RETURN OLD;
END;
$$;
CREATE TRIGGER repository_reference_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_object_references
    FOR EACH ROW EXECUTE FUNCTION guard_repository_reference();
CREATE FUNCTION guard_repository_retention_state() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR NEW.object_id IS DISTINCT FROM OLD.object_id OR (OLD.reclaiming AND NOT NEW.reclaiming) THEN
        RAISE EXCEPTION 'Repository reclamation fence is permanent';
    END IF;
    IF NEW.reclaiming AND EXISTS(SELECT 1 FROM repository_object_references WHERE object_id=NEW.object_id) THEN
        RAISE EXCEPTION 'Retained repository objects cannot be reclaimed';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_retention_state_guard BEFORE UPDATE OR DELETE ON repository_object_retention
    FOR EACH ROW EXECUTE FUNCTION guard_repository_retention_state();
CREATE FUNCTION fence_repository_object(id UUID) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM lock_repository_retention_owner(id);
    PERFORM 1 FROM repository_object_retention WHERE object_id=id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Repository object retention row is missing'; END IF;
    UPDATE repository_object_retention SET reclaiming=true WHERE object_id=id AND NOT reclaiming;
END;
$$;

CREATE FUNCTION initialize_repository_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO repository_object_retention(object_id) VALUES(NEW.object_id);
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_retention_initialize AFTER INSERT ON repository_physical_locations
    FOR EACH ROW EXECUTE FUNCTION initialize_repository_retention();
INSERT INTO repository_object_retention(object_id,reclaiming)
SELECT p.object_id,CASE p.source_kind WHEN 'ARCHIVE' THEN
    EXISTS(SELECT 1 FROM archive_object_uploads u WHERE u.object_id=p.object_id AND u.state IN ('DELETING','DELETED'))
    OR EXISTS(SELECT 1 FROM archive_mutation_targets t WHERE t.object_id=p.object_id)
    ELSE EXISTS(SELECT 1 FROM document_part_attempt_cleanup c WHERE c.attempt_id=p.source_id) END
FROM repository_physical_locations p;

CREATE FUNCTION lock_archive_retention_source() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND ROW(NEW.entry_uuid,NEW.version,NEW.object_id)
        IS DISTINCT FROM ROW(OLD.entry_uuid,OLD.version,OLD.object_id) THEN
        RAISE EXCEPTION 'Archive reference identity is immutable; replace it atomically';
    END IF;
    PERFORM 1 FROM archive_object_uploads WHERE object_id=OLD.object_id FOR UPDATE;
    RETURN OLD;
END;
$$;
CREATE TRIGGER aa_archive_reference_owner_lock BEFORE DELETE OR UPDATE ON archive_version_object_refs
    FOR EACH ROW EXECUTE FUNCTION lock_archive_retention_source();
CREATE FUNCTION mirror_archive_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        INSERT INTO repository_object_references VALUES(NEW.object_id,'ARCHIVE_VERSION',NEW.entry_uuid,NEW.version);
        RETURN NEW;
    END IF;
    DELETE FROM repository_object_references WHERE object_id=OLD.object_id AND owner_kind='ARCHIVE_VERSION'
        AND owner_id=OLD.entry_uuid AND owner_revision=OLD.version;
    RETURN OLD;
END;
$$;
CREATE TRIGGER archive_reference_mirror AFTER INSERT OR DELETE ON archive_version_object_refs
    FOR EACH ROW EXECUTE FUNCTION mirror_archive_retention();

CREATE FUNCTION mirror_document_history_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
    SELECT physical_object_id,'DOCUMENT_HISTORY',NEW.attempt_id,NEW.publication_revision
    FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id ORDER BY physical_object_id;
    RETURN NEW;
END;
$$;
CREATE TRIGGER document_history_reference_mirror AFTER INSERT ON document_part_publication_history
    FOR EACH ROW EXECUTE FUNCTION mirror_document_history_retention();
CREATE FUNCTION lock_document_current_retention_sources() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        PERFORM 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    ELSIF TG_OP='DELETE' THEN
        PERFORM 1 FROM document_part_attempts WHERE attempt_id=OLD.attempt_id FOR UPDATE;
    ELSE
        PERFORM 1 FROM document_part_attempts WHERE attempt_id IN (OLD.attempt_id,NEW.attempt_id) ORDER BY attempt_id FOR UPDATE;
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;
CREATE TRIGGER aa_document_current_owner_lock BEFORE INSERT OR UPDATE OR DELETE ON document_part_publications
    FOR EACH ROW EXECUTE FUNCTION lock_document_current_retention_sources();
CREATE FUNCTION mirror_document_current_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE revision BIGINT;
BEGIN
    IF TG_OP='UPDATE' AND ROW(NEW.node_id,NEW.attempt_id) IS NOT DISTINCT FROM ROW(OLD.node_id,OLD.attempt_id) THEN RETURN NEW; END IF;
    IF TG_OP<>'INSERT' THEN
        PERFORM 1 FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
            WHERE o.attempt_id=OLD.attempt_id ORDER BY r.object_id FOR UPDATE OF r;
        DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_CURRENT' AND owner_id=OLD.node_id;
    END IF;
    IF TG_OP<>'DELETE' THEN
        SELECT publication_revision INTO STRICT revision FROM document_part_publication_history WHERE attempt_id=NEW.attempt_id;
        INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
        SELECT physical_object_id,'DOCUMENT_CURRENT',NEW.node_id,revision
        FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id ORDER BY physical_object_id;
    END IF;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;
CREATE TRIGGER document_current_reference_mirror AFTER INSERT OR UPDATE OR DELETE ON document_part_publications
    FOR EACH ROW EXECUTE FUNCTION mirror_document_current_retention();

CREATE FUNCTION fence_archive_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IN ('DELETING','DELETED') THEN
        PERFORM fence_repository_object(NEW.object_id);
    END IF;
    RETURN NEW;
END;
$$;
-- Separate functions avoid looking for a state field on mutation target rows.
CREATE FUNCTION fence_archive_mutation_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM fence_repository_object(NEW.object_id);
    RETURN NEW;
END;
$$;
CREATE TRIGGER zz_archive_cleanup_retention AFTER UPDATE OF state ON archive_object_uploads
    FOR EACH ROW EXECUTE FUNCTION fence_archive_retention();
CREATE TRIGGER zz_archive_mutation_retention AFTER INSERT ON archive_mutation_targets
    FOR EACH ROW EXECUTE FUNCTION fence_archive_mutation_retention();
CREATE FUNCTION fence_document_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    PERFORM 1 FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
        WHERE o.attempt_id=NEW.attempt_id ORDER BY r.object_id FOR UPDATE OF r;
    UPDATE repository_object_retention r SET reclaiming=true FROM document_part_attempt_objects o
        WHERE o.physical_object_id=r.object_id AND o.attempt_id=NEW.attempt_id AND NOT r.reclaiming;
    RETURN NEW;
END;
$$;
CREATE TRIGGER zz_document_cleanup_retention AFTER INSERT ON document_part_attempt_cleanup
    FOR EACH ROW EXECUTE FUNCTION fence_document_retention();

INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
SELECT object_id,'ARCHIVE_VERSION',entry_uuid,version FROM archive_version_object_refs ORDER BY object_id,entry_uuid,version;
INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
SELECT o.physical_object_id,'DOCUMENT_HISTORY',h.attempt_id,h.publication_revision
FROM document_part_publication_history h JOIN document_part_attempt_objects o USING(attempt_id) ORDER BY o.physical_object_id;
INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
SELECT o.physical_object_id,'DOCUMENT_CURRENT',c.node_id,h.publication_revision
FROM document_part_publications c JOIN document_part_publication_history h USING(attempt_id)
JOIN document_part_attempt_objects o USING(attempt_id) ORDER BY o.physical_object_id;

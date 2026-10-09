-- Drain prior writers, readers and recovery processes, including provider I/O.
-- Logical retirement closes acquisition; physical reclamation requires all owners gone.
LOCK TABLE archive_object_uploads, archive_version_object_refs, archive_mutation_targets,
    document_part_attempts, document_part_attempt_objects, document_part_attempt_cleanup,
    repository_object_retention, repository_object_references IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE repository_object_retention ADD COLUMN retiring BOOLEAN NOT NULL DEFAULT false;
-- A one-time reclassification under migration locks, not a runtime reset capability.
DROP TRIGGER repository_retention_state_guard ON repository_object_retention;
UPDATE repository_object_retention SET retiring=reclaiming;
UPDATE repository_object_retention r SET reclaiming=false
FROM archive_object_uploads u
WHERE r.object_id=u.object_id AND r.reclaiming AND u.state='LIVE'
    AND u.cleanup_token IS NULL AND u.cleanup_attempts=0 AND u.cleanup_error IS NULL
    AND EXISTS(SELECT 1 FROM archive_mutation_targets t WHERE t.object_id=u.object_id);
ALTER TABLE repository_object_retention ADD CONSTRAINT reclamation_requires_retirement CHECK(NOT reclaiming OR retiring);

CREATE OR REPLACE FUNCTION guard_repository_retention_state() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR NEW.object_id IS DISTINCT FROM OLD.object_id
        OR (OLD.reclaiming AND NOT NEW.reclaiming) OR (OLD.retiring AND NOT NEW.retiring) THEN
        RAISE EXCEPTION 'Repository retirement/reclamation fence is permanent';
    END IF;
    IF NEW.reclaiming AND EXISTS(SELECT 1 FROM repository_object_references WHERE object_id=NEW.object_id) THEN
        RAISE EXCEPTION 'Retained repository objects cannot be reclaimed';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_retention_state_guard BEFORE UPDATE OR DELETE ON repository_object_retention
    FOR EACH ROW EXECUTE FUNCTION guard_repository_retention_state();

CREATE OR REPLACE FUNCTION guard_repository_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; fenced BOOLEAN;
BEGIN
    IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Repository reference identity is immutable'; END IF;
    id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
    PERFORM lock_repository_retention_owner(id);
    SELECT retiring INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF fenced THEN RAISE EXCEPTION 'Repository object is permanently fenced for retirement or reclamation'; END IF;
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
CREATE OR REPLACE FUNCTION fence_repository_object(id UUID) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM lock_repository_retention_owner(id);
    PERFORM 1 FROM repository_object_retention WHERE object_id=id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Repository object retention row is missing'; END IF;
    UPDATE repository_object_retention SET retiring=true,reclaiming=true WHERE object_id=id AND NOT reclaiming;
END;
$$;
CREATE OR REPLACE FUNCTION fence_archive_mutation_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM lock_repository_retention_owner(NEW.object_id);
    PERFORM 1 FROM repository_object_retention WHERE object_id=NEW.object_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Repository object retention row is missing'; END IF;
    UPDATE repository_object_retention SET retiring=true WHERE object_id=NEW.object_id AND NOT retiring;
    RETURN NEW;
END;
$$;
CREATE OR REPLACE FUNCTION fence_document_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    PERFORM 1 FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
        WHERE o.attempt_id=NEW.attempt_id ORDER BY r.object_id FOR UPDATE OF r;
    UPDATE repository_object_retention r SET retiring=true,reclaiming=true FROM document_part_attempt_objects o
        WHERE o.physical_object_id=r.object_id AND o.attempt_id=NEW.attempt_id AND NOT r.reclaiming;
    RETURN NEW;
END;
$$;

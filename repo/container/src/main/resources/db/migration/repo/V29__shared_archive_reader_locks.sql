-- Reader admission/release changes only the reader's own pin and reference rows.
-- Keep source -> retention order; cleanup and logical mutations stay exclusive.
-- Drain prior reader processes before rollout: mixed lock modes can upgrade/deadlock.
LOCK TABLE archive_object_uploads, archive_read_pins, repository_object_retention,
    repository_object_references IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION require_repository_read_committed() RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Repository retention requires READ COMMITTED isolation';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION lock_repository_retention_owner(id UUID) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE location repository_physical_locations%ROWTYPE;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT * INTO STRICT location FROM repository_physical_locations WHERE object_id=id;
    IF location.source_kind='ARCHIVE' THEN
        PERFORM 1 FROM archive_object_uploads WHERE object_id=id FOR UPDATE;
    ELSE
        PERFORM 1 FROM document_part_attempts WHERE attempt_id=location.source_id FOR UPDATE;
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION 'Repository retention requires a durable source owner'; END IF;
END;
$$;
CREATE OR REPLACE FUNCTION guard_repository_retention_state() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
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
CREATE OR REPLACE FUNCTION fence_document_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    PERFORM 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    PERFORM 1 FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
        WHERE o.attempt_id=NEW.attempt_id ORDER BY r.object_id FOR UPDATE OF r;
    UPDATE repository_object_retention r SET retiring=true,reclaiming=true FROM document_part_attempt_objects o
        WHERE o.physical_object_id=r.object_id AND o.attempt_id=NEW.attempt_id AND NOT r.reclaiming;
    RETURN NEW;
END;
$$;

CREATE FUNCTION share_archive_retention_owner(id UUID) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    PERFORM 1 FROM archive_object_uploads WHERE object_id=id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Archive reader requires a durable source owner'; END IF;
END;
$$;
CREATE OR REPLACE FUNCTION guard_archive_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; closed BOOLEAN;
BEGIN
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
CREATE OR REPLACE FUNCTION guard_repository_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; kind TEXT; fenced BOOLEAN;
BEGIN
    IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Repository reference identity is immutable'; END IF;
    id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
    kind := CASE WHEN TG_OP='DELETE' THEN OLD.owner_kind ELSE NEW.owner_kind END;
    IF kind='ARCHIVE_READER' THEN
        PERFORM share_archive_retention_owner(id);
        SELECT retiring INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR SHARE;
    ELSE
        PERFORM lock_repository_retention_owner(id);
        SELECT retiring INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR UPDATE;
    END IF;
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

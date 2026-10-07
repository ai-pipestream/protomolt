-- Preserve legacy absence. Rendition lifecycle updates may change a manifest,
-- but must not rewrite the descriptive metadata captured for its version.
CREATE FUNCTION preserve_archive_version_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (
        OLD.manifest->'metadataSnapshot' IS DISTINCT FROM NEW.manifest->'metadataSnapshot'
        OR (OLD.manifest ? 'metadataSnapshot' AND (
        OLD.entry_uuid IS DISTINCT FROM NEW.entry_uuid
        OR OLD.version IS DISTINCT FROM NEW.version
        OR OLD.manifest->'address' IS DISTINCT FROM NEW.manifest->'address'
        OR OLD.manifest->'version' IS DISTINCT FROM NEW.manifest->'version'))) THEN
        RAISE EXCEPTION 'Archive version identity and metadata snapshot are immutable'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.manifest ? 'metadataSnapshot' AND (
        jsonb_typeof(NEW.manifest->'metadataSnapshot') IS DISTINCT FROM 'object'
        OR jsonb_typeof(NEW.manifest->'metadataSnapshot'->'address') IS DISTINCT FROM 'object'
        OR coalesce(btrim(NEW.manifest->'metadataSnapshot'->'address'->>'accountId'), '') = ''
        OR coalesce(btrim(NEW.manifest->'metadataSnapshot'->'address'->>'archive'), '') = ''
        OR coalesce(btrim(NEW.manifest->'metadataSnapshot'->'address'->>'entryId'), '') = ''
        OR NEW.manifest->'metadataSnapshot'->'address' IS DISTINCT FROM NEW.manifest->'address'
        OR NEW.manifest->'metadataSnapshot'->>'currentVersion' IS DISTINCT FROM NEW.version::text
        OR NEW.manifest->>'version' IS DISTINCT FROM NEW.version::text
        OR NEW.manifest->'metadataSnapshot'->>'entryUuid' IS DISTINCT FROM NEW.entry_uuid::text
        OR NEW.version <= 0
    ) THEN
        RAISE EXCEPTION 'Archive metadata snapshot must identify its containing version'
            USING ERRCODE = '23514';
    END IF;
    -- Resolve identity once at creation. Lifecycle changes do not need another
    -- parent lookup: the snapshot and its containing identity are frozen above.
    IF TG_OP = 'INSERT' AND NEW.manifest ? 'metadataSnapshot' AND NOT EXISTS (
        SELECT 1 FROM archive_entries e WHERE e.entry_uuid = NEW.entry_uuid
          AND e.account_id = NEW.manifest->'address'->>'accountId'
          AND e.archive = NEW.manifest->'address'->>'archive'
          AND e.entry_id = NEW.manifest->'address'->>'entryId'
    ) THEN
        RAISE EXCEPTION 'Archive metadata snapshot address must match its parent entry'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER archive_version_metadata_immutable BEFORE INSERT OR UPDATE ON archive_versions
FOR EACH ROW EXECUTE FUNCTION preserve_archive_version_metadata();

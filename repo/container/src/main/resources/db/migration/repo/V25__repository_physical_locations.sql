-- Location registration only. No shared retention or cleanup authority is enabled.
-- Drain older writer processes before rollout; table locks cannot stop provider I/O.
LOCK TABLE archive_object_bindings, document_part_attempt_objects, raw_objects,
    documents, document_purges, archive_versions, archive_version_object_refs,
    archive_object_uploads, document_part_attempts, document_part_publication_history,
    document_part_key_reservations, managed_backend_profiles IN SHARE ROW EXCLUSIVE MODE;

-- Unknown legacy coordinates are globally key-scoped. The same row serializes
-- quarantine against known-location registration, including concurrent inserts.
CREATE TABLE repository_object_keys (
    key_digest BYTEA PRIMARY KEY,
    object_key TEXT NOT NULL CHECK (btrim(object_key) <> ''),
    quarantined BOOLEAN NOT NULL DEFAULT false,
    CHECK (key_digest=sha256(convert_to(object_key,'UTF8')))
);
CREATE TABLE repository_physical_locations (
    object_id UUID PRIMARY KEY,
    backend_generation VARCHAR(128) NOT NULL,
    storage_realm VARCHAR(128) NOT NULL,
    storage_namespace TEXT NOT NULL CHECK (btrim(storage_namespace) <> ''),
    object_key TEXT NOT NULL CHECK (btrim(object_key) <> ''),
    namespace_digest BYTEA NOT NULL,
    key_digest BYTEA NOT NULL REFERENCES repository_object_keys(key_digest),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('ARCHIVE','DOCUMENT_PART')),
    source_id UUID NOT NULL,
    source_ordinal INTEGER NOT NULL CHECK (source_ordinal>=0),
    FOREIGN KEY (backend_generation,storage_realm)
        REFERENCES managed_backend_profiles(generation,storage_realm),
    UNIQUE (source_kind,source_id,source_ordinal),
    UNIQUE (storage_realm,namespace_digest,key_digest),
    CHECK (namespace_digest=sha256(convert_to(storage_namespace,'UTF8'))),
    CHECK (key_digest=sha256(convert_to(object_key,'UTF8'))),
    CHECK (source_kind<>'ARCHIVE' OR (source_id=object_id AND source_ordinal=0))
);
CREATE INDEX repository_physical_key ON repository_physical_locations(key_digest);

CREATE FUNCTION lock_repository_object_key(key_value TEXT, exclusive_lock BOOLEAN) RETURNS BYTEA LANGUAGE plpgsql AS $$
DECLARE digest_value BYTEA; stored_key TEXT;
BEGIN
    IF key_value IS NULL OR btrim(key_value)='' THEN
        RAISE EXCEPTION 'Repository object key must not be blank';
    END IF;
    digest_value := sha256(convert_to(key_value,'UTF8'));
    INSERT INTO repository_object_keys(key_digest,object_key) VALUES(digest_value,key_value)
        ON CONFLICT(key_digest) DO NOTHING;
    IF exclusive_lock THEN
        SELECT object_key INTO STRICT stored_key FROM repository_object_keys
            WHERE key_digest=digest_value FOR UPDATE;
    ELSE
        -- Known locations in distinct namespaces can share this guard. Only
        -- quarantine requires exclusive access; coordinate uniqueness is below.
        SELECT object_key INTO STRICT stored_key FROM repository_object_keys
            WHERE key_digest=digest_value FOR SHARE;
    END IF;
    IF stored_key<>key_value THEN RAISE EXCEPTION 'Repository object key digest collision'; END IF;
    RETURN digest_value;
END;
$$;
CREATE FUNCTION quarantine_repository_object_key(key_value TEXT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE digest_value BYTEA;
BEGIN
    digest_value := lock_repository_object_key(key_value,true);
    IF EXISTS(SELECT 1 FROM repository_physical_locations WHERE key_digest=digest_value) THEN
        RAISE EXCEPTION 'Unknown repository location conflicts with a managed key';
    END IF;
    UPDATE repository_object_keys SET quarantined=true WHERE key_digest=digest_value AND NOT quarantined;
END;
$$;
CREATE FUNCTION guard_repository_location() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Repository physical locations are immutable'; END IF;
    NEW.key_digest := lock_repository_object_key(NEW.object_key,false);
    IF (SELECT quarantined FROM repository_object_keys WHERE key_digest=NEW.key_digest) THEN
        RAISE EXCEPTION 'Repository object key has unknown legacy storage identity';
    END IF;
    NEW.namespace_digest := sha256(convert_to(NEW.storage_namespace,'UTF8'));
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_location_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_physical_locations
    FOR EACH ROW EXECUTE FUNCTION guard_repository_location();
CREATE FUNCTION guard_repository_object_key() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(NEW.key_digest,NEW.object_key) IS DISTINCT FROM ROW(OLD.key_digest,OLD.object_key)
        OR (OLD.quarantined AND NOT NEW.quarantined) THEN
        RAISE EXCEPTION 'Repository key reservations cannot be removed or redirected';
    END IF;
    IF NEW.quarantined AND EXISTS(SELECT 1 FROM repository_physical_locations WHERE key_digest=NEW.key_digest) THEN
        RAISE EXCEPTION 'Unknown repository location conflicts with a managed key';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_object_key_guard BEFORE UPDATE OR DELETE ON repository_object_keys
    FOR EACH ROW EXECUTE FUNCTION guard_repository_object_key();

ALTER TABLE document_part_attempt_objects ADD COLUMN physical_object_id UUID NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE document_part_attempt_objects ADD CONSTRAINT uq_document_physical_object_id UNIQUE(physical_object_id);
CREATE FUNCTION register_archive_physical_location() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO repository_physical_locations(object_id,backend_generation,storage_realm,storage_namespace,
        object_key,source_kind,source_id,source_ordinal)
    VALUES(NEW.object_id,NEW.backend_generation,NEW.storage_realm,NEW.bucket,NEW.object_key,'ARCHIVE',NEW.object_id,0);
    RETURN NEW;
END;
$$;
CREATE FUNCTION register_document_physical_location() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE generation TEXT;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF NEW.physical_object_id IS DISTINCT FROM OLD.physical_object_id THEN
            RAISE EXCEPTION 'Document physical object identity is immutable';
        END IF;
        RETURN NEW;
    END IF;
    SELECT backend_generation INTO STRICT generation FROM document_part_attempts WHERE attempt_id=NEW.attempt_id;
    INSERT INTO repository_physical_locations(object_id,backend_generation,storage_realm,storage_namespace,
        object_key,source_kind,source_id,source_ordinal)
    VALUES(NEW.physical_object_id,generation,NEW.storage_realm,NEW.storage_namespace,NEW.object_key,
        'DOCUMENT_PART',NEW.attempt_id,NEW.ordinal);
    RETURN NEW;
END;
$$;
-- Run after existing BEFORE guards have populated and checked source identities.
CREATE TRIGGER archive_physical_location AFTER INSERT ON archive_object_bindings
    FOR EACH ROW EXECUTE FUNCTION register_archive_physical_location();
CREATE TRIGGER document_physical_location AFTER INSERT OR UPDATE OF physical_object_id ON document_part_attempt_objects
    FOR EACH ROW EXECUTE FUNCTION register_document_physical_location();

INSERT INTO repository_physical_locations(object_id,backend_generation,storage_realm,storage_namespace,
    object_key,source_kind,source_id,source_ordinal)
SELECT object_id,backend_generation,storage_realm,bucket,object_key,'ARCHIVE',object_id,0
FROM archive_object_bindings ORDER BY object_key COLLATE "C";
INSERT INTO repository_physical_locations(object_id,backend_generation,storage_realm,storage_namespace,
    object_key,source_kind,source_id,source_ordinal)
SELECT o.physical_object_id,a.backend_generation,o.storage_realm,o.storage_namespace,o.object_key,
    'DOCUMENT_PART',o.attempt_id,o.ordinal
FROM document_part_attempt_objects o JOIN document_part_attempts a USING(attempt_id)
ORDER BY o.object_key COLLATE "C";
ALTER TABLE archive_object_bindings ADD FOREIGN KEY(object_id) REFERENCES repository_physical_locations(object_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE document_part_attempt_objects ADD FOREIGN KEY(physical_object_id) REFERENCES repository_physical_locations(object_id) DEFERRABLE INITIALLY DEFERRED;

-- Raw V10 identity is not a foreign key to the later backend profiles. Even a
-- matching string is not migration evidence. Keep all raw keys quarantined until
-- an explicit, verified adoption procedure can replace this conservative rule.
CREATE FUNCTION quarantine_raw_physical_location() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM quarantine_repository_object_key(NEW.object_key);
    RETURN NEW;
END;
$$;
CREATE TRIGGER raw_physical_location AFTER INSERT ON raw_objects
    FOR EACH ROW EXECUTE FUNCTION quarantine_raw_physical_location();

CREATE FUNCTION quarantine_document_location_keys(owner_node UUID, keys JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE key_value TEXT;
BEGIN
    FOR key_value IN SELECT value FROM (SELECT DISTINCT value FROM jsonb_array_elements_text(keys)) distinct_keys ORDER BY sha256(convert_to(value,'UTF8')),value COLLATE "C" LOOP
        IF NOT EXISTS(SELECT 1 FROM document_part_attempt_objects o JOIN document_part_attempts a USING(attempt_id)
            WHERE a.node_id=owner_node AND o.key_digest=sha256(convert_to(key_value,'UTF8')) AND o.object_key=key_value
                AND EXISTS(SELECT 1 FROM document_part_publication_history h WHERE h.node_id=owner_node AND h.attempt_id=a.attempt_id)) THEN
            PERFORM quarantine_repository_object_key(key_value);
        END IF;
    END LOOP;
END;
$$;
CREATE FUNCTION quarantine_document_physical_locations() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE keys JSONB;
BEGIN
    IF TG_TABLE_NAME='document_purges' THEN
        keys := NEW.object_keys;
    ELSE
        SELECT COALESCE(jsonb_agg(key_value),'[]'::jsonb) INTO keys FROM (
            SELECT NEW.object_key AS key_value WHERE NEW.object_key IS NOT NULL AND btrim(NEW.object_key)<>''
            UNION SELECT item->>'objectKey' FROM jsonb_array_elements(COALESCE(NEW.part_manifest->'parts','[]'::jsonb)) item
                WHERE item ? 'objectKey' AND btrim(item->>'objectKey')<>''
        ) all_keys;
    END IF;
    PERFORM quarantine_document_location_keys(NEW.node_id,keys);
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER zz_document_location_quarantine AFTER INSERT OR UPDATE ON documents DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION quarantine_document_physical_locations();
CREATE CONSTRAINT TRIGGER document_purge_location_quarantine AFTER INSERT OR UPDATE ON document_purges DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION quarantine_document_physical_locations();

CREATE FUNCTION quarantine_archive_location_keys(owner_entry UUID, owner_version BIGINT, manifest JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE rendition JSONB; key_value TEXT;
BEGIN
    FOR rendition IN SELECT item FROM jsonb_array_elements(COALESCE(manifest->'renditions','[]'::jsonb)) item
        WHERE item ? 'objectKey' AND btrim(item->>'objectKey')<>''
        ORDER BY sha256(convert_to(item->>'objectKey','UTF8')), (item->>'objectKey') COLLATE "C" LOOP
        key_value := rendition->>'objectKey';
        IF NOT EXISTS(SELECT 1 FROM archive_object_bindings b JOIN archive_object_uploads u USING(object_id)
            WHERE b.entry_uuid=owner_entry AND b.object_key=key_value
                AND b.object_id::text=rendition->>'storageObjectId'
                AND u.sha256 IS NOT NULL AND u.sha256=rendition->>'sha256'
                AND u.expected_size::text=COALESCE(rendition->>'sizeBytes','0')
                AND (rendition->>'state'='RENDITION_STATE_DELETED'
                    OR (rendition->>'state'='RENDITION_STATE_PRESENT' AND EXISTS(
                        SELECT 1 FROM archive_version_object_refs r WHERE r.object_id=b.object_id
                            AND r.entry_uuid=owner_entry AND r.version=owner_version)))) THEN
            PERFORM quarantine_repository_object_key(key_value);
        END IF;
    END LOOP;
END;
$$;
CREATE FUNCTION quarantine_archive_physical_locations() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM quarantine_archive_location_keys(NEW.entry_uuid,NEW.version,NEW.manifest);
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER archive_location_quarantine AFTER INSERT OR UPDATE ON archive_versions DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION quarantine_archive_physical_locations();

DO $$ DECLARE row_value RECORD;
BEGIN
    FOR row_value IN SELECT object_key FROM raw_objects ORDER BY object_key COLLATE "C" LOOP
        PERFORM quarantine_repository_object_key(row_value.object_key);
    END LOOP;
    FOR row_value IN SELECT object_key FROM document_part_key_reservations WHERE attempt_id IS NULL ORDER BY object_key COLLATE "C" LOOP
        PERFORM quarantine_repository_object_key(row_value.object_key);
    END LOOP;
    FOR row_value IN SELECT node_id,object_key,part_manifest FROM documents ORDER BY node_id LOOP
        PERFORM quarantine_document_location_keys(row_value.node_id,
            COALESCE((SELECT jsonb_agg(item->>'objectKey') FROM jsonb_array_elements(COALESCE(row_value.part_manifest->'parts','[]'::jsonb)) item
                WHERE item ? 'objectKey' AND btrim(item->>'objectKey')<>''),'[]'::jsonb)
            || CASE WHEN row_value.object_key IS NULL OR btrim(row_value.object_key)='' THEN '[]'::jsonb ELSE jsonb_build_array(row_value.object_key) END);
    END LOOP;
    FOR row_value IN SELECT node_id,object_keys FROM document_purges ORDER BY node_id,purge_id LOOP
        PERFORM quarantine_document_location_keys(row_value.node_id,row_value.object_keys);
    END LOOP;
    FOR row_value IN SELECT entry_uuid,version,manifest FROM archive_versions ORDER BY entry_uuid,version LOOP
        PERFORM quarantine_archive_location_keys(row_value.entry_uuid,row_value.version,row_value.manifest);
    END LOOP;
END $$;

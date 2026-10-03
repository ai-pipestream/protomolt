-- Drain pre-V23 writer processes before enabling managed writes. SQL locks close
-- the migration scan/trigger gap, but cannot cancel old provider I/O already in flight.
LOCK TABLE documents, document_purges, document_part_attempt_objects IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE document_part_key_reservations (
 key_digest BYTEA PRIMARY KEY,
 object_key TEXT NOT NULL CHECK (object_key ~ '(^|/)documents(/|$)'),
 attempt_id UUID REFERENCES document_part_attempts(attempt_id),
 reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 CHECK (key_digest=sha256(convert_to(object_key,'UTF8')))
);
COMMENT ON COLUMN document_part_key_reservations.attempt_id IS
 'NULL reserves a legacy key permanently; non-null belongs exclusively to that managed attempt. Global key scope is conservative because legacy references lack immutable realms.';
CREATE FUNCTION protect_document_key_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Document key reservations are immutable'; END;
$$;
CREATE TRIGGER document_key_reservation_immutable BEFORE UPDATE OR DELETE ON document_part_key_reservations
 FOR EACH ROW EXECUTE FUNCTION protect_document_key_reservation();

CREATE FUNCTION reserve_document_key(key_value TEXT, owner_attempt UUID, allow_managed BOOLEAN) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE existing document_part_key_reservations%ROWTYPE; digest_value BYTEA;
BEGIN
 -- Manifests and purge snapshots can also mention raw blobs; those have their own lifecycle.
 IF key_value IS NULL OR key_value !~ '(^|/)documents(/|$)' THEN RETURN; END IF;
 digest_value := sha256(convert_to(key_value,'UTF8'));
 INSERT INTO document_part_key_reservations(key_digest,object_key,attempt_id)
 VALUES(digest_value,key_value,owner_attempt) ON CONFLICT(key_digest) DO NOTHING;
 SELECT * INTO STRICT existing FROM document_part_key_reservations WHERE key_digest=digest_value;
 IF existing.object_key <> key_value THEN RAISE EXCEPTION 'Document key digest collision'; END IF;
 IF owner_attempt IS NOT NULL THEN
  IF existing.attempt_id IS NULL THEN RAISE EXCEPTION 'Document key is reserved for legacy storage'; END IF;
  IF existing.attempt_id <> owner_attempt THEN RAISE EXCEPTION 'Document key belongs to another managed attempt'; END IF;
 ELSIF existing.attempt_id IS NOT NULL AND NOT allow_managed THEN
  RAISE EXCEPTION 'Document key is reserved for a managed attempt';
 END IF;
END;
$$;
CREATE FUNCTION reserve_document_keys(keys JSONB, owner_attempt UUID, allow_managed BOOLEAN) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE key_value TEXT;
BEGIN
 FOR key_value IN SELECT value FROM (SELECT DISTINCT value FROM jsonb_array_elements_text(keys)) key_set
   ORDER BY sha256(convert_to(value,'UTF8')),value COLLATE "C" LOOP
  PERFORM reserve_document_key(key_value,owner_attempt,allow_managed);
 END LOOP;
END;
$$;
-- All batches use the same digest/key ordering, independent of Java or locale.

-- Preserve already admitted owners first. Duplicate key text under distinct
-- attempts or a digest collision aborts migration rather than choosing an owner.
DO $$ DECLARE obj RECORD; ref RECORD;
BEGIN
 FOR obj IN SELECT object_key,attempt_id FROM document_part_attempt_objects ORDER BY object_key COLLATE "C" LOOP
  PERFORM reserve_document_key(obj.object_key,obj.attempt_id,false);
 END LOOP;
 FOR ref IN SELECT d.node_id,entry->>'objectKey' AS key_value,
    EXISTS(SELECT 1 FROM document_part_publications p WHERE p.node_id=d.node_id) AS bound
    FROM documents d CROSS JOIN LATERAL jsonb_array_elements(d.part_manifest->'parts') entry
    ORDER BY (entry->>'objectKey') COLLATE "C" LOOP
  PERFORM reserve_document_key(ref.key_value,NULL,ref.bound);
 END LOOP;
 FOR ref IN SELECT p.node_id,key_value,
    EXISTS(SELECT 1 FROM document_part_publication_history h WHERE h.node_id=p.node_id) AS bound
    FROM document_purges p CROSS JOIN LATERAL jsonb_array_elements_text(p.object_keys) key_value
    ORDER BY key_value COLLATE "C" LOOP
  PERFORM reserve_document_key(ref.key_value,NULL,ref.bound);
 END LOOP;
END $$;

CREATE FUNCTION reserve_document_manifest_keys() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE keys JSONB;
BEGIN
 SELECT COALESCE(jsonb_agg(entry->>'objectKey'),'[]'::jsonb) INTO keys
 FROM jsonb_array_elements(NEW.part_manifest->'parts') entry WHERE entry ? 'objectKey';
 -- Managed keys still require V22's final same-transaction publication pin.
 PERFORM reserve_document_keys(keys,NULL,true);
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_manifest_key_reservation BEFORE INSERT OR UPDATE OF part_manifest ON documents
 FOR EACH ROW EXECUTE FUNCTION reserve_document_manifest_keys();
CREATE FUNCTION reserve_document_purge_keys() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Existing managed keys are allowed in queue snapshots but legacy purge cannot delete them.
 PERFORM reserve_document_keys(NEW.object_keys,NULL,true);
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_purge_key_reservation BEFORE INSERT OR UPDATE OF object_keys ON document_purges
 FOR EACH ROW EXECUTE FUNCTION reserve_document_purge_keys();
CREATE FUNCTION reserve_document_attempt_key() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM reserve_document_key(NEW.object_key,NEW.attempt_id,false);
 RETURN NEW;
END;
$$;
CREATE TRIGGER zz_document_attempt_key_reservation BEFORE INSERT ON document_part_attempt_objects
 FOR EACH ROW EXECUTE FUNCTION reserve_document_attempt_key();

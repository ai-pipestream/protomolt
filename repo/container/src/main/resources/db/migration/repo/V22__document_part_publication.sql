-- Immutable publication history and the current document's pin. Existing rows
-- remain explicitly unbound; no storage coordinates are inferred during migration.
CREATE INDEX document_part_key_lookup ON document_part_attempt_objects(key_digest);
CREATE FUNCTION document_publication_body(row_data documents) RETURNS JSONB
LANGUAGE sql IMMUTABLE AS $$
 SELECT jsonb_build_object('node_id',row_data.node_id,'account_id',row_data.account_id,
  'doc_id',row_data.doc_id,'graph_id',row_data.graph_id,'graph_address_id',row_data.graph_address_id,
  'drive_name',row_data.drive_name,'object_key',row_data.object_key,'version_id',row_data.version_id,
  'etag',row_data.etag,'size_bytes',row_data.size_bytes,'checksum',row_data.checksum,
  'part_manifest',row_data.part_manifest)
$$;
CREATE TABLE document_part_publication_history (
 attempt_id UUID PRIMARY KEY REFERENCES document_part_attempts(attempt_id),
 node_id UUID NOT NULL,
 publication_revision BIGINT NOT NULL,
 body JSONB NOT NULL,
 published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(node_id,attempt_id)
);
CREATE TABLE document_part_publications (
 node_id UUID PRIMARY KEY REFERENCES documents(node_id) ON DELETE CASCADE,
 attempt_id UUID NOT NULL UNIQUE,
 FOREIGN KEY(node_id,attempt_id) REFERENCES document_part_publication_history(node_id,attempt_id)
);
CREATE FUNCTION protect_document_publication_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a document_part_attempts%ROWTYPE; d documents%ROWTYPE; planned JSONB; presented JSONB;
 total_size NUMERIC; root_hash TEXT; core document_part_attempt_objects%ROWTYPE;
BEGIN
 IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'Document publication history is immutable'; END IF;
 SELECT * INTO STRICT a FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
 SELECT * INTO STRICT d FROM documents WHERE node_id=NEW.node_id;
 IF a.state <> 'VERIFIED' OR a.lease_until <= clock_timestamp()
    OR a.node_id <> d.node_id OR a.account_id <> d.account_id
    OR NEW.publication_revision <> d.mutation_revision OR NEW.body IS DISTINCT FROM document_publication_body(d)
    OR d.status <> 'AVAILABLE' OR d.pending_purge_id IS NOT NULL THEN
  RAISE EXCEPTION 'Document publication requires its live verified attempt and exact row';
 END IF;
 IF EXISTS (SELECT 1 FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id AND NOT verified) THEN
  RAISE EXCEPTION 'Document publication contains unverified objects';
 END IF;
 IF jsonb_typeof(d.part_manifest->'parts') IS DISTINCT FROM 'array'
    OR COALESCE((d.part_manifest->>'docVersion')::bigint,0) <= 0
    OR d.part_manifest->'address'->>'accountId' IS DISTINCT FROM d.account_id
    OR d.part_manifest->'address'->>'docId' IS DISTINCT FROM d.doc_id
    OR d.part_manifest->'address'->>'graphId' IS DISTINCT FROM d.graph_id
    OR d.part_manifest->'address'->>'graphAddressId' IS DISTINCT FROM d.graph_address_id THEN
  RAISE EXCEPTION 'Document publication manifest identity is invalid';
 END IF;
 IF EXISTS (SELECT 1 FROM jsonb_array_elements(d.part_manifest->'parts') e WHERE
    COALESCE(e->>'part','') NOT IN ('DOCUMENT_PART_CORE','DOCUMENT_PART_BLOBS','DOCUMENT_PART_CHUNKS','DOCUMENT_PART_PARSED')
    OR COALESCE(e->>'state','') NOT IN ('PART_STATE_PRESENT','PART_STATE_EMPTY','PART_STATE_DELETED')
    OR COALESCE((e->>'sizeBytes')::bigint,0) < 0
    OR (e->>'part'<>'DOCUMENT_PART_CHUNKS' AND COALESCE(e->>'subKey','')<>'')
    OR (e->>'state'='PART_STATE_EMPTY' AND (COALESCE(e->>'objectKey','')<>'' OR COALESCE(e->>'sha256','')<>''
        OR COALESCE((e->>'sizeBytes')::bigint,0)<>0 OR COALESCE(e->>'deletedReason','')<>''))
    OR (e->>'state'='PART_STATE_DELETED' AND (e->>'part'='DOCUMENT_PART_CORE' OR btrim(COALESCE(e->>'deletedReason',''))=''))
    OR (e->>'state'='PART_STATE_PRESENT' AND COALESCE(e->>'deletedReason','')<>''))
    OR EXISTS (SELECT 1 FROM jsonb_array_elements(d.part_manifest->'parts') e
       GROUP BY e->>'part',COALESCE(e->>'subKey','') HAVING count(*)>1) THEN
  RAISE EXCEPTION 'Document publication manifest slots are invalid';
 END IF;
 SELECT sum(expected_size), encode(sha256(convert_to(string_agg(
   part::text || '|' || sub_key || '|' || expected_sha256 || E'\n','' ORDER BY ordinal),'UTF8')),'hex')
 INTO total_size,root_hash FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id;
 SELECT * INTO STRICT core FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id AND part=1;
 IF total_size IS DISTINCT FROM d.size_bytes::numeric OR root_hash IS DISTINCT FROM d.checksum
    OR core.provider_version IS DISTINCT FROM d.version_id OR COALESCE(core.etag,'') IS DISTINCT FROM d.etag THEN
  RAISE EXCEPTION 'Document publication aggregate or provider identity differs from plan';
 END IF;
 SELECT jsonb_agg(jsonb_build_array(part,sub_key,object_key,expected_size,expected_sha256) ORDER BY ordinal)
  INTO planned FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id;
 SELECT jsonb_agg(jsonb_build_array(
  CASE value->>'part' WHEN 'DOCUMENT_PART_CORE' THEN 1 WHEN 'DOCUMENT_PART_BLOBS' THEN 2
   WHEN 'DOCUMENT_PART_CHUNKS' THEN 3 WHEN 'DOCUMENT_PART_PARSED' THEN 4 ELSE 0 END,
  COALESCE(value->>'subKey',''),COALESCE(value->>'objectKey',''),COALESCE((value->>'sizeBytes')::bigint,0),
  COALESCE(value->>'sha256','')) ORDER BY ordinality)
 INTO presented FROM jsonb_array_elements(d.part_manifest->'parts') WITH ORDINALITY
 WHERE value->>'state'='PART_STATE_PRESENT';
 IF planned IS DISTINCT FROM presented THEN RAISE EXCEPTION 'Document publication differs from verified objects'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_publication_history_guard BEFORE INSERT OR UPDATE OR DELETE ON document_part_publication_history
 FOR EACH ROW EXECUTE FUNCTION protect_document_publication_history();

CREATE FUNCTION protect_document_publication_switch() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h document_part_publication_history%ROWTYPE; a document_part_attempts%ROWTYPE; revision BIGINT;
BEGIN
 IF TG_OP='UPDATE' AND NEW.node_id <> OLD.node_id THEN RAISE EXCEPTION 'Document publication node is immutable'; END IF;
 SELECT * INTO STRICT h FROM document_part_publication_history WHERE attempt_id=NEW.attempt_id;
 SELECT * INTO STRICT a FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
 SELECT mutation_revision INTO STRICT revision FROM documents WHERE node_id=NEW.node_id;
 IF a.state <> 'VERIFIED' OR a.lease_until <= clock_timestamp() OR h.publication_revision <> revision THEN
  RAISE EXCEPTION 'Document publication switch requires a new live publication';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_publication_switch BEFORE INSERT OR UPDATE ON document_part_publications
 FOR EACH ROW EXECUTE FUNCTION protect_document_publication_switch();

CREATE FUNCTION require_document_publication_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; d documents%ROWTYPE; h document_part_publication_history%ROWTYPE;
BEGIN
 id := CASE WHEN TG_OP='DELETE' THEN OLD.node_id ELSE NEW.node_id END;
 SELECT * INTO d FROM documents WHERE node_id=id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT history.* INTO h FROM document_part_publications current_pin
  JOIN document_part_publication_history history USING(attempt_id) WHERE current_pin.node_id=id;
 IF NOT FOUND THEN
  IF TG_TABLE_NAME <> 'documents' THEN RAISE EXCEPTION 'Surviving document cannot lose its publication'; END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(d.part_manifest->'parts') entry
     JOIN document_part_attempt_objects admitted
      ON admitted.key_digest=sha256(convert_to(entry->>'objectKey','UTF8'))
      AND admitted.object_key=entry->>'objectKey') THEN
   RAISE EXCEPTION 'Admitted document parts require an atomic publication';
  END IF;
  RETURN NULL; -- Existing legacy rows are explicitly unbound.
 END IF;
 IF h.body IS DISTINCT FROM document_publication_body(d) THEN
  RAISE EXCEPTION 'Bound document body changed without a new publication';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_publication_row_consistency AFTER INSERT OR UPDATE ON documents
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_publication_consistency();
CREATE CONSTRAINT TRIGGER document_publication_pin_consistency AFTER INSERT OR UPDATE OR DELETE ON document_part_publications
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_publication_consistency();
CREATE FUNCTION require_document_publication_pin() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM document_part_publications WHERE node_id=NEW.node_id AND attempt_id=NEW.attempt_id) THEN
  RAISE EXCEPTION 'New document publication must commit its pin';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_publication_history_pin AFTER INSERT ON document_part_publication_history
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_publication_pin();

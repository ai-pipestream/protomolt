-- Additive projection only: V22 publication and V26/V29 retention stay authoritative.
-- Drain old writers/recovery before rollout; no provider I/O is protected by SQL locks.
LOCK TABLE documents, document_part_attempts, document_part_attempt_objects,
 document_part_publication_history, document_part_publications,
 repository_physical_locations IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE document_revision_publications (
 revision_id UUID PRIMARY KEY,
 node_id UUID NOT NULL,
 publication_revision BIGINT NOT NULL CHECK(publication_revision>0),
 body JSONB NOT NULL,
 published_at TIMESTAMPTZ NOT NULL,
 legacy_attempt_id UUID UNIQUE REFERENCES document_part_publication_history(attempt_id),
 projection_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 projection_sealed BOOLEAN NOT NULL DEFAULT false,
 UNIQUE(node_id,revision_id)
);
CREATE TABLE document_revision_parts (
 revision_id UUID NOT NULL REFERENCES document_revision_publications(revision_id),
 revision_ordinal INTEGER NOT NULL CHECK(revision_ordinal>=0),
 part INTEGER NOT NULL CHECK(part BETWEEN 1 AND 4),
 sub_key TEXT NOT NULL,
 object_id UUID NOT NULL REFERENCES repository_physical_locations(object_id),
 PRIMARY KEY(revision_id,revision_ordinal)
);
CREATE INDEX document_revision_parts_object ON document_revision_parts(object_id);
CREATE TABLE document_revision_current (
 node_id UUID PRIMARY KEY REFERENCES documents(node_id) ON DELETE CASCADE,
 revision_id UUID NOT NULL,
 FOREIGN KEY(node_id,revision_id) REFERENCES document_revision_publications(node_id,revision_id)
);

-- Full manifest position and dense PRESENT ordinal are different coordinates.
-- Missing or inconsistent source evidence is omitted here and rejected by the
-- complete-count and equality proof below; it is never silently adopted.
CREATE FUNCTION document_revision_legacy_parts(id UUID)
RETURNS TABLE(revision_ordinal INTEGER,part INTEGER,sub_key TEXT,object_id UUID)
LANGUAGE sql STABLE AS $$
 WITH entries AS MATERIALIZED (
  SELECT (e.ordinality-1)::integer position,
   (row_number() OVER (ORDER BY e.ordinality)-1)::integer dense_ordinal,
   CASE e.value->>'part' WHEN 'DOCUMENT_PART_CORE' THEN 1 WHEN 'DOCUMENT_PART_BLOBS' THEN 2
    WHEN 'DOCUMENT_PART_CHUNKS' THEN 3 WHEN 'DOCUMENT_PART_PARSED' THEN 4 ELSE 0 END part,
   COALESCE(e.value->>'subKey','') sub_key, e.value
  FROM document_part_publication_history h,
   LATERAL jsonb_array_elements(h.body->'part_manifest'->'parts') WITH ORDINALITY e
  WHERE h.attempt_id=id AND e.value->>'state'='PART_STATE_PRESENT'
 )
 SELECT e.position,o.part,o.sub_key,o.physical_object_id
 FROM entries e JOIN document_part_attempt_objects o ON o.attempt_id=id AND o.ordinal=e.dense_ordinal
 JOIN document_part_attempts a ON a.attempt_id=o.attempt_id
 JOIN repository_physical_locations l ON l.object_id=o.physical_object_id
 WHERE o.verified AND e.part=o.part AND e.sub_key=o.sub_key
  AND e.value->>'objectKey'=o.object_key AND COALESCE((e.value->>'sizeBytes')::bigint,0)=o.expected_size
  AND e.value->>'sha256'=o.expected_sha256
  AND l.source_kind='DOCUMENT_PART' AND l.source_id=id AND l.source_ordinal=o.ordinal
  AND l.backend_generation=a.backend_generation AND l.storage_realm=a.storage_realm
  AND l.storage_realm=o.storage_realm AND l.storage_namespace=o.storage_namespace
  AND l.storage_namespace=a.storage_namespace AND l.object_key=o.object_key
$$;

CREATE FUNCTION guard_document_revision_projection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h document_part_publication_history%ROWTYPE;
BEGIN
 IF TG_OP='UPDATE' AND NOT OLD.projection_sealed AND NEW.projection_sealed
  AND OLD.projection_xid=pg_current_xact_id()
  AND (to_jsonb(NEW)-'projection_sealed')=(to_jsonb(OLD)-'projection_sealed') THEN RETURN NEW; END IF;
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Document revision projection is immutable'; END IF;
 SELECT * INTO h FROM document_part_publication_history WHERE attempt_id=NEW.legacy_attempt_id;
 IF NOT FOUND OR NEW.revision_id<>h.attempt_id OR NEW.node_id<>h.node_id
  OR NEW.publication_revision<>h.publication_revision OR NEW.body IS DISTINCT FROM h.body
  OR NEW.published_at<>h.published_at THEN
  RAISE EXCEPTION 'Document revision projection requires exact legacy history';
 END IF;
 NEW.projection_xid := pg_current_xact_id();
 NEW.projection_sealed := false;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_projection_guard BEFORE INSERT OR UPDATE OR DELETE ON document_revision_publications
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_projection();

CREATE FUNCTION guard_document_revision_part() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Document revision parts are immutable'; END IF;
 IF NOT EXISTS(SELECT 1 FROM document_revision_publications WHERE revision_id=NEW.revision_id
  AND projection_xid=pg_current_xact_id() AND NOT projection_sealed) THEN
  RAISE EXCEPTION 'Document revision parts require their creation transaction';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_part_guard BEFORE INSERT OR UPDATE OR DELETE ON document_revision_parts
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_part();

CREATE FUNCTION populate_document_revision_parts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO document_revision_parts SELECT NEW.revision_id,p.* FROM document_revision_legacy_parts(NEW.legacy_attempt_id) p;
 UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=NEW.revision_id;
 RETURN NEW;
END;
$$;
-- Must run before the completeness constraint even when a caller makes it immediate.
CREATE TRIGGER a0_document_revision_parts_populate AFTER INSERT ON document_revision_publications
 FOR EACH ROW EXECUTE FUNCTION populate_document_revision_parts();

-- One proof per revision, not one full-manifest scan per part.
CREATE FUNCTION require_document_revision_projection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a document_part_attempts%ROWTYPE; present_count BIGINT; actual_count BIGINT; expected_count BIGINT;
 total_size NUMERIC; root_hash TEXT; core document_part_attempt_objects%ROWTYPE; parts_differ BOOLEAN;
BEGIN
 SELECT * INTO STRICT a FROM document_part_attempts WHERE attempt_id=NEW.legacy_attempt_id;
 SELECT count(*) INTO present_count FROM jsonb_array_elements(NEW.body->'part_manifest'->'parts') e
  WHERE e->>'state'='PART_STATE_PRESENT';
 SELECT count(*),sum(expected_size),encode(sha256(convert_to(string_agg(
  part::text || '|' || sub_key || '|' || expected_sha256 || E'\n','' ORDER BY ordinal),'UTF8')),'hex')
 INTO actual_count,total_size,root_hash FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id;
 SELECT * INTO STRICT core FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id AND part=1;
 WITH expected AS MATERIALIZED (SELECT * FROM document_revision_legacy_parts(a.attempt_id)),
 projected AS MATERIALIZED (SELECT revision_ordinal,part,sub_key,object_id FROM document_revision_parts WHERE revision_id=NEW.revision_id)
 SELECT (SELECT count(*) FROM expected),EXISTS(
  (SELECT * FROM projected EXCEPT SELECT * FROM expected)
  UNION ALL (SELECT * FROM expected EXCEPT SELECT * FROM projected)) INTO expected_count,parts_differ;
 IF a.plan_kind<>'FULL_REVISION' OR a.state<>'VERIFIED' OR a.node_id<>NEW.node_id
  OR a.account_id IS DISTINCT FROM NEW.body->>'account_id'
  OR NEW.node_id::text IS DISTINCT FROM NEW.body->>'node_id'
  OR total_size IS DISTINCT FROM (NEW.body->>'size_bytes')::numeric
  OR root_hash IS DISTINCT FROM NEW.body->>'checksum'
  OR core.provider_version IS DISTINCT FROM NEW.body->>'version_id'
  OR COALESCE(core.etag,'') IS DISTINCT FROM NEW.body->>'etag'
  OR jsonb_typeof(NEW.body->'part_manifest'->'parts') IS DISTINCT FROM 'array'
  OR present_count=0 OR present_count<>a.planned_count OR actual_count<>present_count OR expected_count<>present_count
  OR EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=a.attempt_id) THEN
  RAISE EXCEPTION 'Document revision projection has inconsistent managed evidence';
 END IF;
 IF parts_differ THEN RAISE EXCEPTION 'Document revision projection parts differ from managed evidence'; END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_revision_projection_complete AFTER INSERT ON document_revision_publications
 DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION require_document_revision_projection();

CREATE FUNCTION mirror_document_revision_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO document_revision_publications(revision_id,node_id,publication_revision,body,published_at,legacy_attempt_id)
 VALUES(NEW.attempt_id,NEW.node_id,NEW.publication_revision,NEW.body,NEW.published_at,NEW.attempt_id);
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_history_mirror AFTER INSERT ON document_part_publication_history
 FOR EACH ROW EXECUTE FUNCTION mirror_document_revision_history();

CREATE FUNCTION guard_document_revision_current() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM document_part_publications p JOIN documents d USING(node_id) WHERE p.node_id=OLD.node_id) THEN
   RAISE EXCEPTION 'Document revision projection cannot lose its current pin';
  END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='UPDATE' AND NEW.node_id<>OLD.node_id THEN RAISE EXCEPTION 'Document revision node is immutable'; END IF;
 IF NOT EXISTS(SELECT 1 FROM document_part_publications p JOIN document_revision_publications r
  ON r.legacy_attempt_id=p.attempt_id WHERE p.node_id=NEW.node_id AND r.revision_id=NEW.revision_id) THEN
  RAISE EXCEPTION 'Document revision projection requires exact legacy current pin';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_current_guard BEFORE INSERT OR UPDATE OR DELETE ON document_revision_current
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_current();
CREATE FUNCTION mirror_document_revision_current() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN DELETE FROM document_revision_current WHERE node_id=OLD.node_id; RETURN OLD; END IF;
 INSERT INTO document_revision_current(node_id,revision_id) VALUES(NEW.node_id,NEW.attempt_id)
 ON CONFLICT(node_id) DO UPDATE SET revision_id=EXCLUDED.revision_id;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_current_mirror AFTER INSERT OR UPDATE OR DELETE ON document_part_publications
 FOR EACH ROW EXECUTE FUNCTION mirror_document_revision_current();

-- Do not accumulate a deferred event for every retained history during backfill.
SET CONSTRAINTS document_revision_projection_complete IMMEDIATE;
INSERT INTO document_revision_publications(revision_id,node_id,publication_revision,body,published_at,legacy_attempt_id)
 SELECT attempt_id,node_id,publication_revision,body,published_at,attempt_id FROM document_part_publication_history;
INSERT INTO document_revision_current SELECT node_id,attempt_id FROM document_part_publications;

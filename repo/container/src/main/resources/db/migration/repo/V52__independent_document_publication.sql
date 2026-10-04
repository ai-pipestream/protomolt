-- Drain repository writers before rollout. Native publication remains an internal
-- composition boundary: Java must prove content, policy and canonical result
-- correspondence before the transaction described by these constraints.
LOCK TABLE repository_operations, repository_operation_owners, documents,
 document_revision_publications, document_revision_parts, document_revision_current,
 document_part_publications, document_events_outbox IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE repository_operation_success (
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 command_codec VARCHAR(128) NOT NULL,
 command_version INTEGER NOT NULL,
 command_sha256 BYTEA NOT NULL CHECK(octet_length(command_sha256)=32),
 result_codec VARCHAR(128) NOT NULL CHECK(result_codec='document-publication-result'),
 result_version INTEGER NOT NULL CHECK(result_version=1),
 result_bytes BYTEA NOT NULL CHECK(octet_length(result_bytes) BETWEEN 1 AND 1048576),
 result_sha256 BYTEA NOT NULL CHECK(result_sha256=sha256(result_bytes)),
 member_count INTEGER NOT NULL CHECK(member_count BETWEEN 1 AND 64),
 creation_xid xid8 NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id),
 UNIQUE(account_id,principal,operation_id,owner_generation),
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_operations
);

CREATE TABLE document_revision_commits (
 revision_id UUID PRIMARY KEY,
 node_id UUID NOT NULL,
 publication_revision BIGINT NOT NULL CHECK(publication_revision>0),
 previous_revision UUID REFERENCES document_revision_publications(revision_id),
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 member_id VARCHAR(128) NOT NULL,
 member_ordinal INTEGER NOT NULL CHECK(member_ordinal BETWEEN 0 AND 63),
 selection_revision BIGINT NOT NULL CHECK(selection_revision>0),
 event_id UUID NOT NULL UNIQUE REFERENCES document_events_outbox DEFERRABLE INITIALLY DEFERRED,
 metadata_version INTEGER NOT NULL CHECK(metadata_version=1),
 metadata_snapshot JSONB NOT NULL CHECK(octet_length(metadata_snapshot::text)<=1048576),
 -- The first native path is opaque. Typed admission requires retained schema and
 -- validation bindings before another mode can be admitted, never a downgrade.
 admission_mode TEXT NOT NULL CHECK(admission_mode='OPAQUE'),
 structured_resolution BYTEA CHECK(octet_length(structured_resolution) BETWEEN 1 AND 32768),
 creation_xid xid8 NOT NULL,
 UNIQUE(account_id,principal,operation_id,member_id),
 UNIQUE(account_id,principal,operation_id,member_ordinal),
 UNIQUE(account_id,principal,operation_id,node_id),
 FOREIGN KEY(account_id,principal,operation_id,owner_generation)
  REFERENCES repository_operation_success(account_id,principal,operation_id,owner_generation)
  DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
  REFERENCES document_operation_selection_attempts DEFERRABLE INITIALLY DEFERRED
);
ALTER TABLE document_revision_publications ADD COLUMN native_binding UUID;
ALTER TABLE document_revision_publications ADD CONSTRAINT document_revision_origin_kind CHECK(
 (legacy_attempt_id IS NOT NULL AND native_binding IS NULL)
 OR (legacy_attempt_id IS NULL AND native_binding IS NOT NULL AND native_binding=revision_id));
ALTER TABLE document_revision_publications ADD CONSTRAINT document_revision_native_binding
 FOREIGN KEY(native_binding) REFERENCES document_revision_commits(revision_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE document_revision_publications ADD UNIQUE(revision_id,node_id,publication_revision);
ALTER TABLE document_revision_commits ADD CONSTRAINT document_commit_revision
 FOREIGN KEY(revision_id,node_id,publication_revision)
 REFERENCES document_revision_publications(revision_id,node_id,publication_revision) DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION document_revision_metadata_v1(d documents) RETURNS JSONB LANGUAGE sql IMMUTABLE AS $$
 SELECT jsonb_build_object('row_kind',d.row_kind,'cluster_id',d.cluster_id,
  'account_id',d.account_id,'datasource_id',d.datasource_id,'connector_id',d.connector_id,
  'content_type',d.content_type,'filename',d.filename,'security',d.security,
  'delete_source_blobs_on_settle',d.delete_source_blobs_on_settle,
  'source_blob_delete_reason',d.source_blob_delete_reason,'crawl_id',d.crawl_id,
  'created_at_epoch_micros',extract(epoch FROM d.created_at)*1000000,
  'updated_at_epoch_micros',extract(epoch FROM d.updated_at)*1000000)
$$;

CREATE OR REPLACE FUNCTION require_repository_operation_write_fence(
 scoped_account text, scoped_principal text, scoped_operation uuid, scoped_generation bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=scoped_account
  AND principal=scoped_principal AND operation_id=scoped_operation) THEN
  RAISE EXCEPTION 'Repository operation is terminal';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_operation_owners
  WHERE account_id=scoped_account AND principal=scoped_principal AND operation_id=scoped_operation
   AND owner_generation=scoped_generation AND write_fence_xid=pg_current_xact_id_if_assigned()
   AND lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository mutation requires a live owner write fence in this transaction';
 END IF;
 RETURN true;
END;
$$;

-- This additional guard runs before the existing owner guard and stamping trigger.
-- Recovery preserves identity/lease and never grants the ordinary write fence.
CREATE FUNCTION reject_terminal_repository_owner_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN RETURN NEW; END IF;
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Repository operation is terminal';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_repository_terminal_owner_guard BEFORE INSERT OR UPDATE ON repository_operation_owners
 FOR EACH ROW EXECUTE FUNCTION reject_terminal_repository_owner_write();

CREATE FUNCTION guard_document_revision_commit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE d documents%ROWTYPE; s document_operation_selections%ROWTYPE; selected BIGINT;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Document revision commit binding is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT d FROM documents WHERE node_id=NEW.node_id;
 SELECT * INTO STRICT s FROM document_operation_selections WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id;
 SELECT selection_revision INTO STRICT selected FROM document_operation_selection_current
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id;
 IF d.account_id<>NEW.account_id OR d.mutation_revision<>NEW.publication_revision
  OR s.node_id<>NEW.node_id OR selected<>NEW.selection_revision
  OR d.status<>'AVAILABLE' OR d.pending_purge_id IS NOT NULL THEN
  RAISE EXCEPTION 'Document revision commit differs from current document or selection';
 END IF;
 SELECT revision_id INTO NEW.previous_revision FROM document_revision_current WHERE node_id=NEW.node_id;
 IF NEW.previous_revision IS NULL AND s.sampled_revision<>0 THEN
  RAISE EXCEPTION 'Independent revision requires explicit prior publication evidence';
 END IF;
 NEW.metadata_snapshot := document_revision_metadata_v1(d);
 NEW.creation_xid := pg_current_xact_id();
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_commit_guard BEFORE INSERT OR UPDATE OR DELETE ON document_revision_commits
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_commit();

CREATE FUNCTION guard_repository_operation_success() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE command repository_operations%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Repository operation success is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT command FROM repository_operations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF ROW(NEW.command_codec,NEW.command_version,NEW.command_sha256)
  IS DISTINCT FROM ROW(command.command_codec,command.command_version,command.command_sha256)
  OR command.command_codec<>'document-publication' OR command.command_version<>1 THEN
  RAISE EXCEPTION 'Repository result differs from admitted command identity';
 END IF;
 NEW.creation_xid := pg_current_xact_id();
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_operation_success_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_operation_success
 FOR EACH ROW EXECUTE FUNCTION guard_repository_operation_success();

CREATE FUNCTION protect_linked_document_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
  IF EXISTS(SELECT 1 FROM document_revision_commits c JOIN repository_operation_success s
   USING(account_id,principal,operation_id,owner_generation)
   WHERE c.event_id=OLD.event_id AND c.creation_xid=pg_current_xact_id()) THEN
   RAISE EXCEPTION 'Publication event cannot change after terminal insertion in its transaction';
  END IF;
  RETURN NEW;
 END IF;
 IF EXISTS(SELECT 1 FROM document_revision_commits WHERE event_id=OLD.event_id) THEN
  RAISE EXCEPTION 'Document publication event is retained by its revision';
 END IF;
 RETURN OLD;
END;
$$;
CREATE TRIGGER document_event_revision_retention BEFORE UPDATE OR DELETE ON document_events_outbox
 FOR EACH ROW EXECUTE FUNCTION protect_linked_document_event();

CREATE FUNCTION protect_terminal_publication_document() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM document_revision_current p JOIN document_revision_commits c USING(revision_id)
  JOIN repository_operation_success s USING(account_id,principal,operation_id,owner_generation)
  WHERE p.node_id=OLD.node_id AND c.creation_xid=pg_current_xact_id()) THEN
  RAISE EXCEPTION 'Document cannot change after terminal insertion in its publication transaction';
 END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;
CREATE TRIGGER a0_document_terminal_publication_guard BEFORE UPDATE OR DELETE ON documents
 FOR EACH ROW EXECUTE FUNCTION protect_terminal_publication_document();

CREATE FUNCTION require_document_native_parts(id UUID) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE r document_revision_publications%ROWTYPE; c document_revision_commits%ROWTYPE;
 d documents%ROWTYPE; s document_operation_selections%ROWTYPE; selected UUID;
 total_size NUMERIC; root_hash TEXT; actual_count BIGINT; present_count BIGINT;
 core_version TEXT; core_etag TEXT; prior_version BIGINT;
BEGIN
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=id;
 SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=id;
 SELECT * INTO STRICT d FROM documents WHERE node_id=r.node_id;
 SELECT * INTO STRICT s FROM document_operation_selections WHERE account_id=c.account_id
  AND principal=c.principal AND operation_id=c.operation_id AND owner_generation=c.owner_generation AND member_id=c.member_id;
 SELECT h.attempt_id INTO STRICT selected FROM document_operation_selection_current p
  JOIN document_operation_selection_attempts h USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
  WHERE p.account_id=c.account_id AND p.principal=c.principal AND p.operation_id=c.operation_id
   AND p.owner_generation=c.owner_generation AND p.member_id=c.member_id AND p.selection_revision=c.selection_revision;
 IF r.legacy_attempt_id IS NOT NULL OR r.native_binding IS DISTINCT FROM c.revision_id
  OR r.projection_xid<>pg_current_xact_id() OR c.creation_xid<>r.projection_xid
  OR r.node_id<>c.node_id OR r.publication_revision<>c.publication_revision
  OR d.account_id<>c.account_id OR d.mutation_revision<>r.publication_revision
  OR r.body IS DISTINCT FROM document_publication_body(d)
  OR c.metadata_snapshot IS DISTINCT FROM document_revision_metadata_v1(d)
  OR d.status<>'AVAILABLE' OR d.pending_purge_id IS NOT NULL
  OR r.publication_revision<=s.sampled_revision THEN
  RAISE EXCEPTION 'Independent revision differs from its exact document and selection';
 END IF;
 prior_version := 0;
 IF c.previous_revision IS NOT NULL THEN
  SELECT (body->'part_manifest'->>'docVersion')::bigint INTO STRICT prior_version
   FROM document_revision_publications WHERE revision_id=c.previous_revision;
 END IF;
 IF COALESCE((d.part_manifest->>'docVersion')::bigint,0) IS DISTINCT FROM prior_version+1 THEN
  RAISE EXCEPTION 'Independent revision manifest version is not the next publication version';
 END IF;
 IF selected IS NOT NULL AND NOT EXISTS(SELECT 1 FROM document_part_attempts a
  WHERE a.attempt_id=selected AND a.state='VERIFIED' AND a.lease_until>clock_timestamp()
   AND a.plan_kind='NEW_CONTENT' AND a.account_id=c.account_id AND a.operation_principal=c.principal
   AND a.operation_id=c.operation_id AND a.operation_generation=c.owner_generation AND a.member_id=c.member_id
   AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=a.attempt_id)) THEN
  RAISE EXCEPTION 'Independent revision requires its live verified selected upload';
 END IF;
 IF jsonb_typeof(d.part_manifest->'parts') IS DISTINCT FROM 'array'
  OR jsonb_array_length(d.part_manifest->'parts') NOT BETWEEN 1 AND 10000
  OR COALESCE((d.part_manifest->>'docVersion')::bigint,0)<=0
  OR d.part_manifest->'address'->>'accountId' IS DISTINCT FROM d.account_id
  OR d.part_manifest->'address'->>'docId' IS DISTINCT FROM d.doc_id
  OR d.part_manifest->'address'->>'graphId' IS DISTINCT FROM d.graph_id
  OR d.part_manifest->'address'->>'graphAddressId' IS DISTINCT FROM d.graph_address_id THEN
  RAISE EXCEPTION 'Independent revision manifest identity is invalid';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(d.part_manifest->'parts') e WHERE
  COALESCE(e->>'part','') NOT IN ('DOCUMENT_PART_CORE','DOCUMENT_PART_BLOBS','DOCUMENT_PART_CHUNKS','DOCUMENT_PART_PARSED')
  OR COALESCE(e->>'state','') NOT IN ('PART_STATE_PRESENT','PART_STATE_EMPTY','PART_STATE_DELETED')
  OR COALESCE((e->>'sizeBytes')::bigint,0)<0
  OR (e->>'part'<>'DOCUMENT_PART_CHUNKS' AND COALESCE(e->>'subKey','')<>'')
  OR (e->>'part'='DOCUMENT_PART_CORE' AND e->>'state'<>'PART_STATE_PRESENT')
  OR (e->>'state'='PART_STATE_EMPTY' AND (COALESCE(e->>'objectKey','')<>'' OR COALESCE(e->>'sha256','')<>''
      OR COALESCE((e->>'sizeBytes')::bigint,0)<>0 OR COALESCE(e->>'deletedReason','')<>''))
  OR (e->>'state'='PART_STATE_DELETED' AND btrim(COALESCE(e->>'deletedReason',''))='')
  OR (e->>'state'='PART_STATE_PRESENT' AND COALESCE(e->>'deletedReason','')<>''))
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(d.part_manifest->'parts') e
    GROUP BY e->>'part',COALESCE(e->>'subKey','') HAVING count(*)>1)
  OR (SELECT count(*) FROM jsonb_array_elements(d.part_manifest->'parts') e WHERE e->>'part'='DOCUMENT_PART_CORE')<>1 THEN
  RAISE EXCEPTION 'Independent revision manifest slots are invalid';
 END IF;
 SELECT count(*) INTO present_count FROM jsonb_array_elements(d.part_manifest->'parts') e WHERE e->>'state'='PART_STATE_PRESENT';
 SELECT count(*) INTO actual_count FROM document_revision_parts WHERE revision_id=id;
 IF actual_count<>present_count THEN RAISE EXCEPTION 'Independent revision part set is incomplete'; END IF;
 -- Every projected full-list position must prove one exact physical source. Counts
 -- plus this anti-join reject omissions, duplicates, extra and reordered slots.
 IF EXISTS(SELECT 1 FROM document_revision_parts p WHERE p.revision_id=id AND NOT EXISTS(
  SELECT 1 FROM repository_physical_locations l
  JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
  JOIN document_part_attempts a ON a.attempt_id=o.attempt_id
  JOIN repository_object_retention hold ON hold.object_id=l.object_id AND NOT hold.retiring AND NOT hold.reclaiming
  WHERE l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART' AND o.verified AND a.state='VERIFIED'
   AND a.account_id=c.account_id AND a.backend_generation=l.backend_generation
   AND a.storage_realm=l.storage_realm AND a.storage_namespace=l.storage_namespace
   AND o.storage_realm=l.storage_realm AND o.storage_namespace=l.storage_namespace AND o.object_key=l.object_key
   AND o.part=p.part AND o.sub_key=p.sub_key AND btrim(o.content_type)<>''
   AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=a.attempt_id)
   AND (a.attempt_id=selected OR EXISTS(SELECT 1 FROM repository_object_references ref
     WHERE ref.object_id=l.object_id AND ref.owner_kind IN ('DOCUMENT_CURRENT','DOCUMENT_HISTORY')))
   AND (a.attempt_id IS DISTINCT FROM selected OR o.revision_ordinal=p.revision_ordinal)
   AND d.part_manifest->'parts'->p.revision_ordinal->>'state'='PART_STATE_PRESENT'
   AND d.part_manifest->'parts'->p.revision_ordinal->>'part'=CASE p.part
    WHEN 1 THEN 'DOCUMENT_PART_CORE' WHEN 2 THEN 'DOCUMENT_PART_BLOBS' WHEN 3 THEN 'DOCUMENT_PART_CHUNKS' WHEN 4 THEN 'DOCUMENT_PART_PARSED' END
   AND COALESCE(d.part_manifest->'parts'->p.revision_ordinal->>'subKey','')=p.sub_key
   AND d.part_manifest->'parts'->p.revision_ordinal->>'objectKey'=o.object_key
   AND COALESCE((d.part_manifest->'parts'->p.revision_ordinal->>'sizeBytes')::bigint,0)=o.expected_size
   AND d.part_manifest->'parts'->p.revision_ordinal->>'sha256'=o.expected_sha256
 )) THEN RAISE EXCEPTION 'Independent revision parts differ from verified retained objects'; END IF;
 IF (SELECT count(*) FROM document_revision_parts p JOIN repository_physical_locations l USING(object_id)
  WHERE p.revision_id=id AND l.source_id=selected)<>s.upload_count THEN
  RAISE EXCEPTION 'Independent revision omits selected upload objects';
 END IF;
 SELECT sum(o.expected_size),encode(sha256(convert_to(string_agg(
  p.part::text||'|'||p.sub_key||'|'||o.expected_sha256||E'\n','' ORDER BY p.revision_ordinal),'UTF8')),'hex')
 INTO total_size,root_hash FROM document_revision_parts p
 JOIN repository_physical_locations l USING(object_id)
 JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal WHERE p.revision_id=id;
 SELECT o.provider_version,o.etag INTO STRICT core_version,core_etag FROM document_revision_parts p
 JOIN repository_physical_locations l USING(object_id)
 JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal WHERE p.revision_id=id AND p.part=1;
 IF total_size IS DISTINCT FROM d.size_bytes::numeric OR root_hash IS DISTINCT FROM d.checksum
  OR core_version IS DISTINCT FROM d.version_id OR COALESCE(core_etag,'') IS DISTINCT FROM d.etag THEN
  RAISE EXCEPTION 'Independent revision aggregate or CORE provider identity differs';
 END IF;
 RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION guard_document_revision_projection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h document_part_publication_history%ROWTYPE; c document_revision_commits%ROWTYPE; d documents%ROWTYPE;
BEGIN
 IF TG_OP='UPDATE' AND NOT OLD.projection_sealed AND NEW.projection_sealed
  AND OLD.projection_xid=pg_current_xact_id()
  AND (to_jsonb(NEW)-'projection_sealed')=(to_jsonb(OLD)-'projection_sealed') THEN
  IF NEW.native_binding IS NOT NULL THEN
   SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.revision_id;
   PERFORM require_repository_operation_write_fence(c.account_id,c.principal,c.operation_id,c.owner_generation);
   PERFORM require_document_native_parts(NEW.revision_id);
  END IF;
  RETURN NEW;
 END IF;
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Document revision projection is immutable'; END IF;
 IF NEW.native_binding IS NOT NULL THEN
  SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.native_binding;
  PERFORM require_repository_operation_write_fence(c.account_id,c.principal,c.operation_id,c.owner_generation);
  SELECT * INTO STRICT d FROM documents WHERE node_id=c.node_id;
  IF NEW.legacy_attempt_id IS NOT NULL OR NEW.revision_id<>c.revision_id OR NEW.node_id<>c.node_id
   OR NEW.publication_revision<>c.publication_revision OR c.creation_xid<>pg_current_xact_id()
   OR NEW.body IS DISTINCT FROM document_publication_body(d) THEN
   RAISE EXCEPTION 'Independent revision requires its exact operation binding';
  END IF;
  NEW.published_at := clock_timestamp();
 ELSE
  SELECT * INTO h FROM document_part_publication_history WHERE attempt_id=NEW.legacy_attempt_id;
  IF NOT FOUND OR NEW.revision_id<>h.attempt_id OR NEW.node_id<>h.node_id
   OR NEW.publication_revision<>h.publication_revision OR NEW.body IS DISTINCT FROM h.body
   OR NEW.published_at<>h.published_at THEN
   RAISE EXCEPTION 'Document revision projection requires exact legacy history';
  END IF;
 END IF;
 NEW.projection_xid := pg_current_xact_id(); NEW.projection_sealed := false;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION populate_document_revision_parts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.native_binding IS NOT NULL THEN RETURN NEW; END IF;
 INSERT INTO document_revision_parts SELECT NEW.revision_id,p.* FROM document_revision_legacy_parts(NEW.legacy_attempt_id) p;
 UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=NEW.revision_id;
 RETURN NEW;
END;
$$;

-- Keep the legacy proof verbatim on legacy inserts; independent completion is
-- proved by the deferred operation outcome and bidirectional revision bindings.
DROP TRIGGER document_revision_projection_complete ON document_revision_publications;
CREATE CONSTRAINT TRIGGER document_revision_projection_complete AFTER INSERT ON document_revision_publications
 DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW WHEN (NEW.legacy_attempt_id IS NOT NULL)
 EXECUTE FUNCTION require_document_revision_projection();

CREATE FUNCTION require_repository_operation_success_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE count_members BIGINT; first_ordinal INTEGER; last_ordinal INTEGER; member document_revision_commits%ROWTYPE;
 total_parts BIGINT;
BEGIN
 PERFORM require_repository_read_committed();
 IF NEW.creation_xid<>pg_current_xact_id() OR NOT EXISTS(SELECT 1 FROM repository_operation_owners o
  WHERE o.account_id=NEW.account_id AND o.principal=NEW.principal AND o.operation_id=NEW.operation_id
   AND o.owner_generation=NEW.owner_generation AND o.write_fence_xid=pg_current_xact_id() AND o.lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository finalization requires its original live committing owner';
 END IF;
 SELECT count(*),min(member_ordinal),max(member_ordinal) INTO count_members,first_ordinal,last_ordinal
 FROM document_revision_commits WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF count_members<>NEW.member_count OR first_ordinal<>0 OR last_ordinal<>NEW.member_count-1 THEN
  RAISE EXCEPTION 'Repository result member set is incomplete';
 END IF;
 SELECT sum(jsonb_array_length(r.body->'part_manifest'->'parts')) INTO total_parts
 FROM document_revision_commits c JOIN document_revision_publications r USING(revision_id)
 WHERE c.account_id=NEW.account_id AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id;
 IF total_parts IS NULL OR total_parts>10000 THEN RAISE EXCEPTION 'Repository result exceeds total publication part bound'; END IF;
 FOR member IN SELECT * FROM document_revision_commits WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id ORDER BY member_ordinal LOOP
  IF member.owner_generation<>NEW.owner_generation OR member.creation_xid<>NEW.creation_xid
   OR NOT EXISTS(SELECT 1 FROM document_revision_publications r JOIN document_revision_current p USING(revision_id)
    WHERE r.revision_id=member.revision_id AND r.native_binding=member.revision_id
     AND r.projection_sealed AND r.projection_xid=NEW.creation_xid AND p.node_id=member.node_id)
   OR NOT EXISTS(SELECT 1 FROM document_events_outbox e JOIN documents d ON d.node_id=member.node_id
    WHERE e.event_id=member.event_id AND e.insertion_xid=NEW.creation_xid
     AND e.event_type='DocumentSaved' AND e.kafka_key=d.doc_id AND e.status IN ('RECORDED','PENDING')) THEN
   RAISE EXCEPTION 'Repository result requires every sealed current revision and its new event';
  END IF;
  PERFORM require_document_native_parts(member.revision_id);
 END LOOP;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_operation_success_complete AFTER INSERT ON repository_operation_success
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_operation_success_complete();

CREATE OR REPLACE FUNCTION guard_document_revision_current() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r document_revision_publications%ROWTYPE; c document_revision_commits%ROWTYPE;
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM documents d WHERE d.node_id=OLD.node_id)
   AND (EXISTS(SELECT 1 FROM document_part_publications WHERE node_id=OLD.node_id)
    OR EXISTS(SELECT 1 FROM document_revision_publications WHERE revision_id=OLD.revision_id AND native_binding IS NOT NULL)) THEN
   RAISE EXCEPTION 'Document revision projection cannot lose its current pin';
  END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='UPDATE' AND NEW.node_id<>OLD.node_id THEN RAISE EXCEPTION 'Document revision node is immutable'; END IF;
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=NEW.revision_id;
 IF r.native_binding IS NOT NULL THEN
  SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=r.revision_id;
  PERFORM require_repository_operation_write_fence(c.account_id,c.principal,c.operation_id,c.owner_generation);
  IF r.node_id<>NEW.node_id OR NOT r.projection_sealed OR r.projection_xid<>pg_current_xact_id() THEN
   RAISE EXCEPTION 'Independent current pin requires its new sealed revision';
  END IF;
 ELSE
  IF TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM document_revision_publications
   WHERE revision_id=OLD.revision_id AND native_binding IS NOT NULL) THEN
   RAISE EXCEPTION 'Legacy publication cannot replace an independent current revision';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM document_part_publications p WHERE p.node_id=NEW.node_id AND p.attempt_id=r.legacy_attempt_id) THEN
   RAISE EXCEPTION 'Document revision projection requires exact legacy current pin';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION mirror_document_revision_current() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  -- Native publication switches first, then drops the obsolete legacy pin. Never
  -- remove the new pointer when that old row's DELETE mirror runs.
  DELETE FROM document_revision_current WHERE node_id=OLD.node_id AND revision_id=OLD.attempt_id;
  RETURN OLD;
 END IF;
 INSERT INTO document_revision_current(node_id,revision_id) VALUES(NEW.node_id,NEW.attempt_id)
 ON CONFLICT(node_id) DO UPDATE SET revision_id=EXCLUDED.revision_id;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION require_document_publication_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; d documents%ROWTYPE; h document_part_publication_history%ROWTYPE; r document_revision_publications%ROWTYPE;
BEGIN
 id := CASE WHEN TG_OP='DELETE' THEN OLD.node_id ELSE NEW.node_id END;
 SELECT * INTO d FROM documents WHERE node_id=id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT revision.* INTO r FROM document_revision_current p JOIN document_revision_publications revision USING(revision_id)
  WHERE p.node_id=id;
 IF FOUND AND r.native_binding IS NOT NULL THEN
  IF NOT r.projection_sealed OR r.body IS DISTINCT FROM document_publication_body(d)
   OR EXISTS(SELECT 1 FROM document_part_publications WHERE node_id=id)
   OR NOT EXISTS(SELECT 1 FROM document_revision_commits c JOIN repository_operation_success s
     USING(account_id,principal,operation_id,owner_generation) WHERE c.revision_id=r.revision_id) THEN
   RAISE EXCEPTION 'Independent document requires its exact committed revision without a legacy pin';
  END IF;
  RETURN NULL;
 END IF;
 SELECT history.* INTO h FROM document_part_publications current_pin
  JOIN document_part_publication_history history USING(attempt_id) WHERE current_pin.node_id=id;
 IF NOT FOUND THEN
  IF TG_TABLE_NAME<>'documents' THEN RAISE EXCEPTION 'Surviving document cannot lose its publication'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(d.part_manifest->'parts') entry
   JOIN document_part_attempt_objects admitted ON admitted.key_digest=sha256(convert_to(entry->>'objectKey','UTF8'))
    AND admitted.object_key=entry->>'objectKey') THEN
   RAISE EXCEPTION 'Admitted document parts require an atomic publication';
  END IF;
  RETURN NULL;
 END IF;
 IF h.body IS DISTINCT FROM document_publication_body(d) THEN
  RAISE EXCEPTION 'Bound document body changed without a new publication';
 END IF;
 RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION quarantine_document_location_keys(owner_node UUID, keys JSONB) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE key_value TEXT;
BEGIN
 FOR key_value IN SELECT value FROM (SELECT DISTINCT value FROM jsonb_array_elements_text(keys)) distinct_keys
  ORDER BY sha256(convert_to(value,'UTF8')),value COLLATE "C" LOOP
  IF NOT EXISTS(SELECT 1 FROM document_part_attempt_objects o JOIN document_part_attempts a USING(attempt_id)
   WHERE a.node_id=owner_node AND o.key_digest=sha256(convert_to(key_value,'UTF8')) AND o.object_key=key_value
    AND EXISTS(SELECT 1 FROM document_part_publication_history h WHERE h.node_id=owner_node AND h.attempt_id=a.attempt_id))
   AND NOT EXISTS(SELECT 1 FROM repository_physical_locations l JOIN document_revision_parts p USING(object_id)
    JOIN document_revision_publications r USING(revision_id)
    JOIN document_revision_commits c USING(revision_id)
    JOIN repository_operation_success s USING(account_id,principal,operation_id,owner_generation)
    WHERE l.key_digest=sha256(convert_to(key_value,'UTF8')) AND l.object_key=key_value
     AND l.source_kind='DOCUMENT_PART' AND r.node_id=owner_node AND r.projection_sealed AND r.native_binding=r.revision_id) THEN
   PERFORM quarantine_repository_object_key(key_value);
  END IF;
 END LOOP;
END;
$$;

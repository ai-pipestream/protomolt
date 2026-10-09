-- Storage prerequisite, not an admission verdict. The trusted publisher must
-- decode canonical bundles, reproduce complete candidate evidence, bind policy
-- and retain the exact asset union before typed publication can be enabled.
-- Existing revisions remain unchanged; no evidence is invented by migration.
LOCK TABLE document_revision_commits, document_revision_publications,
 document_revision_parts IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE document_revision_schema_evidence (
 revision_id UUID NOT NULL REFERENCES document_revision_commits(revision_id),
 revision_ordinal INTEGER NOT NULL,
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 root_locator_sha256 BYTEA NOT NULL CHECK(octet_length(root_locator_sha256)=32),
 fragment_sha256 BYTEA NOT NULL CHECK(octet_length(fragment_sha256)=32),
 fragment_size BIGINT NOT NULL CHECK(fragment_size>0),
 evidence_codec TEXT NOT NULL CHECK(evidence_codec='document-root-schema-evidence'),
 evidence_version INTEGER NOT NULL CHECK(evidence_version=1),
 evidence_bytes BYTEA NOT NULL CHECK(octet_length(evidence_bytes) BETWEEN 1 AND 4194304),
 evidence_sha256 BYTEA NOT NULL CHECK(evidence_sha256=sha256(evidence_bytes)),
 evidence_size INTEGER GENERATED ALWAYS AS (octet_length(evidence_bytes)) STORED,
 PRIMARY KEY(revision_id,revision_ordinal,root_locator_sha256),
 FOREIGN KEY(revision_id,revision_ordinal)
  REFERENCES document_revision_parts(revision_id,revision_ordinal)
);
-- Aggregate budget checks inspect lengths, never decode or detoast every bundle.
CREATE INDEX document_revision_schema_evidence_budget
 ON document_revision_schema_evidence(account_id,principal,operation_id,revision_id)
 INCLUDE(evidence_size);

CREATE FUNCTION guard_document_revision_schema_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c document_revision_commits%ROWTYPE;
 r document_revision_publications%ROWTYPE;
 revision_rows BIGINT; operation_rows BIGINT; revision_bytes BIGINT; operation_bytes BIGINT;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Revision schema evidence is immutable'; END IF;
 -- The proven owner write locks serialize this operation, including aggregates
 -- across different member revisions. Do not acquire an owner lock out of order.
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.revision_id;
 IF c.account_id<>NEW.account_id OR c.principal<>NEW.principal OR c.operation_id<>NEW.operation_id
  OR c.owner_generation<>NEW.owner_generation OR c.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema evidence differs from native commit owner or transaction';
 END IF;
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=NEW.revision_id FOR UPDATE;
 IF r.native_binding IS DISTINCT FROM NEW.revision_id OR r.projection_sealed
  OR r.projection_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema evidence requires the current unsealed native projection';
 END IF;
 IF NOT EXISTS(
  SELECT 1 FROM document_revision_parts p
  JOIN repository_physical_locations l ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
  JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
   AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
  JOIN document_part_attempts a ON a.attempt_id=o.attempt_id
  WHERE p.revision_id=NEW.revision_id AND p.revision_ordinal=NEW.revision_ordinal
   AND p.part IN (1,4) AND p.sub_key='' AND o.part=p.part AND o.sub_key=p.sub_key
   AND a.account_id=NEW.account_id AND a.state='VERIFIED' AND o.verified
   AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
   AND a.storage_namespace=l.storage_namespace AND o.storage_realm=l.storage_realm
   AND o.storage_namespace=l.storage_namespace AND o.object_key=l.object_key
   AND decode(o.expected_sha256,'hex')=NEW.fragment_sha256 AND o.expected_size=NEW.fragment_size
 ) THEN RAISE EXCEPTION 'Revision schema evidence differs from a supported verified fragment'; END IF;
 -- Full provider identity, retention and manifest agreement are also enforced
 -- by native projection sealing. This guard adds evidence's raw-byte identity.
 SELECT count(*),COALESCE(sum(evidence_size),0),
  count(*) FILTER(WHERE revision_id=NEW.revision_id),
  COALESCE(sum(evidence_size) FILTER(WHERE revision_id=NEW.revision_id),0)
 INTO operation_rows,operation_bytes,revision_rows,revision_bytes
 FROM document_revision_schema_evidence
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF revision_rows>=1024 OR revision_bytes+octet_length(NEW.evidence_bytes)>16777216 THEN
  RAISE EXCEPTION 'Revision schema evidence exceeds revision budget';
 END IF;
 IF operation_rows>=4096 OR operation_bytes+octet_length(NEW.evidence_bytes)>67108864 THEN
  RAISE EXCEPTION 'Revision schema evidence exceeds operation budget';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_schema_evidence_guard
 BEFORE INSERT OR UPDATE OR DELETE ON document_revision_schema_evidence
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_schema_evidence();

-- Root evidence has its own count/byte budget; it is not a schema catalog asset.
-- SQL binds stored evidence to retained fragments. Canonical decoding, complete
-- root discovery and exact manifest equality remain trusted handler obligations.
LOCK TABLE document_assessment_owners IN SHARE ROW EXCLUSIVE MODE;
ALTER TABLE document_assessment_owners ADD COLUMN expected_roots INTEGER NOT NULL DEFAULT 0
 CHECK(expected_roots BETWEEN 0 AND 4096);
CREATE TABLE document_assessment_roots (
 assessment_id UUID NOT NULL,
 member_id VARCHAR(128) NOT NULL,
 revision_ordinal INTEGER NOT NULL,
 root_locator_sha256 BYTEA NOT NULL CHECK(octet_length(root_locator_sha256)=32),
 fragment_sha256 BYTEA NOT NULL CHECK(octet_length(fragment_sha256)=32),
 fragment_size BIGINT NOT NULL CHECK(fragment_size>=0),
 evidence_codec TEXT NOT NULL CHECK(evidence_codec='document-root-schema-evidence'),
 evidence_version INTEGER NOT NULL CHECK(evidence_version=1),
 evidence_bytes BYTEA NOT NULL CHECK(octet_length(evidence_bytes) BETWEEN 1 AND 4194304),
 evidence_sha256 BYTEA NOT NULL CONSTRAINT document_assessment_root_evidence_checksum CHECK(evidence_sha256=sha256(evidence_bytes)),
 evidence_size INTEGER GENERATED ALWAYS AS (octet_length(evidence_bytes)) STORED,
 PRIMARY KEY(assessment_id,member_id,revision_ordinal,root_locator_sha256),
 FOREIGN KEY(assessment_id,member_id,revision_ordinal)
  REFERENCES document_assessment_slots(assessment_id,member_id,revision_ordinal)
);
CREATE INDEX document_assessment_root_budget ON document_assessment_roots(assessment_id) INCLUDE(evidence_size);

CREATE FUNCTION guard_document_assessment_root() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner RECORD; id UUID;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Assessment root evidence is immutable'; END IF;
 id:=CASE WHEN TG_OP='DELETE' THEN OLD.assessment_id ELSE NEW.assessment_id END;
 SELECT account_id,principal,operation_id,owner_generation,creation_xid,sealed,release_xid
 INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=id FOR UPDATE;
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(owner.account_id,owner.principal,owner.operation_id);
  IF owner.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment root release requires fenced owner recovery';
  END IF;
  RETURN OLD;
 END IF;
 PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 IF owner.creation_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() OR NOT owner.sealed OR owner.release_xid IS NOT NULL THEN
  RAISE EXCEPTION 'Assessment root requires its physical seal in the creation transaction';
 END IF;
 -- Sealing already holds the complete source/retention lock set. Do not take a
 -- new source lock here or repeat aggregate budget scans for each inserted root.
 IF NOT EXISTS(
  SELECT 1 FROM document_assessment_slots s
  JOIN repository_physical_locations l ON l.object_id=s.object_id AND l.source_kind='DOCUMENT_PART'
  JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
   AND o.physical_object_id=l.object_id AND o.verified
  JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.account_id=owner.account_id AND a.state='VERIFIED'
  WHERE s.assessment_id=id AND s.member_id=NEW.member_id AND s.revision_ordinal=NEW.revision_ordinal
   AND decode(o.expected_sha256,'hex')=NEW.fragment_sha256 AND o.expected_size=NEW.fragment_size
 ) THEN RAISE EXCEPTION 'Assessment root differs from its retained verified fragment'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_root_guard BEFORE INSERT OR UPDATE OR DELETE ON document_assessment_roots
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_root();

CREATE FUNCTION require_complete_document_assessment_roots() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected INTEGER; actual BIGINT; bytes BIGINT;
BEGIN
 SELECT expected_roots INTO expected FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT count(*),COALESCE(sum(evidence_size::bigint),0) INTO actual,bytes
 FROM document_assessment_roots WHERE assessment_id=NEW.assessment_id;
 IF actual<>expected OR bytes>67108864 THEN
  RAISE EXCEPTION 'Assessment root set is incomplete or exceeds byte budget';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_assessment_roots_complete AFTER INSERT OR UPDATE ON document_assessment_owners
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_complete_document_assessment_roots();

CREATE OR REPLACE FUNCTION release_expired_document_assessment(p_account TEXT,p_principal TEXT,p_operation UUID,p_assessment UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE owner document_assessment_owners%ROWTYPE;
BEGIN
 PERFORM fence_repository_operation_recovery(p_account,p_principal,p_operation);
 SELECT * INTO owner FROM document_assessment_owners WHERE assessment_id=p_assessment FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 IF ROW(owner.account_id,owner.principal,owner.operation_id) IS DISTINCT FROM ROW(p_account,p_principal,p_operation) THEN
  RAISE EXCEPTION 'Assessment recovery scope differs from owner';
 END IF;
 UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=p_assessment;
 PERFORM 1 FROM repository_schema_artifacts a JOIN document_assessment_artifacts r USING(account_id,artifact_sha256)
  WHERE r.assessment_id=p_assessment ORDER BY a.artifact_sha256 FOR KEY SHARE OF a;
 DELETE FROM document_assessment_roots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_artifacts WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_slots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_objects WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_owners WHERE assessment_id=p_assessment;
 RETURN true;
END;
$$;

-- Internal handler provenance. Older owners deliberately remain without a
-- snapshot; they must not be adopted by the future reconciliation handler.
CREATE TABLE document_assessment_slot_snapshots (
 assessment_id UUID PRIMARY KEY REFERENCES document_assessment_owners,
 snapshot_codec TEXT NOT NULL CHECK(snapshot_codec='document-assessment-slots'),
 snapshot_version INTEGER NOT NULL CHECK(snapshot_version=1),
 snapshot_bytes BYTEA NOT NULL CHECK(octet_length(snapshot_bytes) BETWEEN 1 AND 4194304),
 snapshot_sha256 BYTEA NOT NULL CONSTRAINT document_assessment_slot_snapshot_checksum
  CHECK(snapshot_sha256=sha256(snapshot_bytes))
);

CREATE FUNCTION guard_document_assessment_slot_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner RECORD; id UUID;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Assessment slot snapshot is immutable'; END IF;
 id:=CASE WHEN TG_OP='DELETE' THEN OLD.assessment_id ELSE NEW.assessment_id END;
 SELECT account_id,principal,operation_id,owner_generation,creation_xid,sealed,release_xid
 INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=id FOR UPDATE;
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(owner.account_id,owner.principal,owner.operation_id);
  IF owner.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment slot snapshot release requires fenced owner recovery';
  END IF;
  RETURN OLD;
 END IF;
 PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 IF owner.creation_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() OR NOT owner.sealed OR owner.release_xid IS NOT NULL THEN
  RAISE EXCEPTION 'Assessment slot snapshot requires its physical seal in the creation transaction';
 END IF;
 -- Canonical encoding and equality with the complete slot set are handler
 -- obligations. A checksum is not independently observed validation evidence.
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_slot_snapshot_guard BEFORE INSERT OR UPDATE OR DELETE ON document_assessment_slot_snapshots
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_slot_snapshot();

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
 DELETE FROM document_assessment_slot_snapshots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_roots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_artifacts WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_slots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_objects WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_owners WHERE assessment_id=p_assessment;
 RETURN true;
END;
$$;

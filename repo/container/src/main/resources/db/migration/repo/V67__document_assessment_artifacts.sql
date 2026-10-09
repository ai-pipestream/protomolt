-- Normalized byte ownership only. Canonical manifest-to-artifact equality and
-- descriptor/root validation remain handler obligations. No replay API enabled.
LOCK TABLE document_assessment_owners IN SHARE ROW EXCLUSIVE MODE;
ALTER TABLE document_assessment_owners ADD COLUMN expected_artifacts INTEGER NOT NULL DEFAULT 0
 CHECK(expected_artifacts BETWEEN 0 AND 64);
ALTER TABLE document_assessment_owners ADD UNIQUE(assessment_id,account_id);
CREATE TABLE document_assessment_artifacts (
 assessment_id UUID NOT NULL,
 account_id VARCHAR(200) NOT NULL,
 artifact_sha256 BYTEA NOT NULL,
 PRIMARY KEY(assessment_id,artifact_sha256),
 FOREIGN KEY(assessment_id,account_id) REFERENCES document_assessment_owners(assessment_id,account_id),
 FOREIGN KEY(account_id,artifact_sha256) REFERENCES repository_schema_artifacts(account_id,artifact_sha256)
);
CREATE INDEX document_assessment_artifact_retention ON document_assessment_artifacts(account_id,artifact_sha256);

CREATE FUNCTION guard_document_assessment_artifact() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner RECORD; id UUID;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Assessment artifact identity is immutable'; END IF;
 id:=CASE WHEN TG_OP='DELETE' THEN OLD.assessment_id ELSE NEW.assessment_id END;
 SELECT account_id,principal,operation_id,owner_generation,creation_xid,sealed,release_xid,expected_artifacts
 INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=id FOR UPDATE;
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(owner.account_id,owner.principal,owner.operation_id);
  IF owner.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment artifact release requires fenced owner recovery';
  END IF;
  RETURN OLD;
 END IF;
 PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 IF owner.creation_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() OR NOT owner.sealed
  OR owner.release_xid IS NOT NULL OR NEW.account_id IS DISTINCT FROM owner.account_id THEN
  RAISE EXCEPTION 'Assessment artifact requires its scoped physical seal in the creation transaction';
 END IF;
 IF (SELECT count(*) FROM document_assessment_artifacts WHERE assessment_id=id)>=owner.expected_artifacts THEN
  RAISE EXCEPTION 'Assessment artifact exceeds declared count';
 END IF;
 -- Physical sources/retention are already locked by sealing. Catalog rows come
 -- next, then claims. Callers insert the complete bounded set in digest order.
 PERFORM 1 FROM repository_schema_artifacts
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment schema artifact is missing'; END IF;
 PERFORM 1 FROM repository_schema_artifact_claims
  WHERE account_id=owner.account_id AND principal=owner.principal AND operation_id=owner.operation_id
   AND owner_generation=owner.owner_generation AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment requires an exact current-generation artifact claim'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_artifact_guard BEFORE INSERT OR UPDATE OR DELETE ON document_assessment_artifacts
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_artifact();

CREATE FUNCTION require_complete_document_assessment_artifacts() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected INTEGER; actual BIGINT; bytes BIGINT;
BEGIN
 SELECT expected_artifacts INTO expected FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT count(*),COALESCE(sum(a.size_bytes::bigint),0) INTO actual,bytes
 FROM document_assessment_artifacts r JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
 WHERE r.assessment_id=NEW.assessment_id;
 IF actual<>expected OR bytes>67108864 THEN
  RAISE EXCEPTION 'Assessment artifact set is incomplete or exceeds byte budget';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_assessment_artifacts_complete AFTER INSERT OR UPDATE ON document_assessment_owners
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_complete_document_assessment_artifacts();

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
 -- V66's transition guard takes the complete physical lock set before artifacts.
 UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=p_assessment;
 PERFORM 1 FROM repository_schema_artifacts a JOIN document_assessment_artifacts r USING(account_id,artifact_sha256)
  WHERE r.assessment_id=p_assessment ORDER BY a.artifact_sha256 FOR KEY SHARE OF a;
 DELETE FROM document_assessment_artifacts WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_slots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_objects WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_owners WHERE assessment_id=p_assessment;
 RETURN true;
END;
$$;

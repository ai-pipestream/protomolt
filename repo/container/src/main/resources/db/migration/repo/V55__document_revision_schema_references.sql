-- Storage prerequisite only. No production writer or typed admission is enabled.
-- Root/path/candidate completeness must be proved before activation. No backfill:
-- existing opaque or legacy revisions do not acquire invented schema evidence.
LOCK TABLE document_revision_commits, document_revision_publications,
 repository_schema_artifacts, repository_schema_artifact_claims IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE document_revision_schema_artifacts (
 revision_id UUID NOT NULL REFERENCES document_revision_publications(revision_id),
 account_id VARCHAR(200) NOT NULL,
 artifact_sha256 BYTEA NOT NULL CHECK(octet_length(artifact_sha256)=32),
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 PRIMARY KEY(revision_id,artifact_sha256),
 FOREIGN KEY(revision_id) REFERENCES document_revision_commits(revision_id),
 FOREIGN KEY(account_id,artifact_sha256)
  REFERENCES repository_schema_artifacts(account_id,artifact_sha256)
);
CREATE INDEX document_revision_schema_artifact_retention
 ON document_revision_schema_artifacts(account_id,artifact_sha256);

CREATE FUNCTION guard_document_revision_schema_artifact() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c document_revision_commits%ROWTYPE;
 r document_revision_publications%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Revision schema references are immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.revision_id;
 IF c.account_id<>NEW.account_id OR c.principal<>NEW.principal OR c.operation_id<>NEW.operation_id
  OR c.owner_generation<>NEW.owner_generation OR c.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema reference differs from native commit owner or transaction';
 END IF;
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=NEW.revision_id FOR UPDATE;
 IF r.native_binding IS DISTINCT FROM NEW.revision_id OR r.projection_sealed
  OR r.projection_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema reference requires the current unsealed native projection';
 END IF;
 -- The committing caller prelocks the complete artifact set in digest order,
 -- after the existing publication locks. The catalog FK survives claim release.
 PERFORM 1 FROM repository_schema_artifacts
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Revision schema artifact is missing'; END IF;
 PERFORM 1 FROM repository_schema_artifact_claims
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND owner_generation=NEW.owner_generation AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Revision schema artifact requires a current owner claim'; END IF;
 IF (SELECT count(*) FROM document_revision_schema_artifacts WHERE revision_id=NEW.revision_id)>=64 THEN
  RAISE EXCEPTION 'Revision schema artifact count exceeds bound';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_schema_artifact_guard
 BEFORE INSERT OR UPDATE OR DELETE ON document_revision_schema_artifacts
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_schema_artifact();

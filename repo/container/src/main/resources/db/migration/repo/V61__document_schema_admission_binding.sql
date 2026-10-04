-- Drain repository writers before rollout. This binds trusted Java admission
-- decisions to exact SQL writes; it does not implement protobuf validation in SQL.
LOCK TABLE documents, document_revision_commits, document_revision_publications IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE document_revision_schema_admissions (
 revision_id UUID PRIMARY KEY,
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 member_id VARCHAR(128) NOT NULL,
 node_id UUID NOT NULL,
 selection_revision BIGINT NOT NULL CHECK(selection_revision>0),
 previous_mutation_revision BIGINT NOT NULL CHECK(previous_mutation_revision>=0),
 command_sha256 BYTEA NOT NULL CHECK(octet_length(command_sha256)=32),
 policy_revision BIGINT NOT NULL CHECK(policy_revision>0),
 policy_sha256 BYTEA NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('TYPED','OPAQUE')),
 body JSONB NOT NULL CHECK(octet_length(body::text)<=16777216),
 metadata JSONB NOT NULL CHECK(octet_length(metadata::text)<=1048576),
 manifest JSONB NOT NULL CHECK(octet_length(manifest::text)<=16777216),
 snapshot_bytes BIGINT GENERATED ALWAYS AS
  (octet_length(body::text)::bigint+octet_length(metadata::text)+octet_length(manifest::text)) STORED,
 creation_xid xid8 NOT NULL,
 UNIQUE(account_id,principal,operation_id,member_id),
 UNIQUE(account_id,principal,operation_id,node_id),
 FOREIGN KEY(account_id,policy_sha256) REFERENCES document_schema_policies,
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_operations,
 FOREIGN KEY(revision_id) REFERENCES document_revision_commits DEFERRABLE INITIALLY DEFERRED,
 CHECK((jsonb_typeof(manifest)='object' AND manifest->>'version'='1'
  AND jsonb_typeof(manifest->'parts')='array' AND jsonb_typeof(manifest->'artifacts')='array'
  AND jsonb_typeof(manifest->'assets')='array' AND jsonb_typeof(manifest->'roots')='array') IS TRUE)
);

CREATE INDEX document_schema_admission_node_xid ON document_revision_schema_admissions(node_id,creation_xid);

CREATE FUNCTION guard_document_schema_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE policy document_schema_policy_current%ROWTYPE; command repository_operations%ROWTYPE;
 selection document_operation_selections%ROWTYPE; selected BIGINT; previous BIGINT; total BIGINT; members BIGINT;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Document schema admission is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 -- Supported callers already hold this lock before any document/physical lock.
 PERFORM lock_document_schema_policy_account(NEW.account_id,false);
 SELECT * INTO STRICT policy FROM document_schema_policy_current WHERE account_id=NEW.account_id FOR SHARE;
 IF ROW(policy.policy_revision,policy.policy_sha256) IS DISTINCT FROM ROW(NEW.policy_revision,NEW.policy_sha256) THEN
  RAISE EXCEPTION 'Document schema admission policy changed';
 END IF;
 SELECT * INTO STRICT command FROM repository_operations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF command.command_codec<>'document-publication' OR command.command_version<>1
  OR command.command_sha256 IS DISTINCT FROM NEW.command_sha256 THEN
  RAISE EXCEPTION 'Document schema admission command differs';
 END IF;
 SELECT * INTO STRICT selection FROM document_operation_selections WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id;
 SELECT selection_revision INTO STRICT selected FROM document_operation_selection_current WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id;
 SELECT mutation_revision INTO previous FROM documents WHERE node_id=NEW.node_id FOR UPDATE;
 IF selection.node_id<>NEW.node_id OR selected<>NEW.selection_revision
  OR COALESCE(previous,0)<>selection.sampled_revision
  OR EXISTS(SELECT 1 FROM document_revision_commits WHERE revision_id=NEW.revision_id) THEN
  RAISE EXCEPTION 'Document schema admission must precede its selected document mutation';
 END IF;
 IF NEW.body->>'account_id' IS DISTINCT FROM NEW.account_id OR NEW.body->>'node_id' IS DISTINCT FROM NEW.node_id::text
  OR NEW.metadata->>'account_id' IS DISTINCT FROM NEW.account_id THEN
  RAISE EXCEPTION 'Document schema admission snapshots differ from its identity';
 END IF;
 IF jsonb_array_length(NEW.manifest->'parts') NOT BETWEEN 1 AND 10000
  OR jsonb_array_length(NEW.manifest->'artifacts')>64 OR jsonb_array_length(NEW.manifest->'assets')>64
  OR jsonb_array_length(NEW.manifest->'roots')>1024
  OR (NEW.decision='TYPED' AND (jsonb_array_length(NEW.manifest->'roots')=0
     OR jsonb_array_length(NEW.manifest->'artifacts')=0 OR jsonb_array_length(NEW.manifest->'assets')=0))
  OR (NEW.decision='OPAQUE' AND (jsonb_array_length(NEW.manifest->'roots')<>0
     OR jsonb_array_length(NEW.manifest->'artifacts')<>0 OR jsonb_array_length(NEW.manifest->'assets')<>0)) THEN
  RAISE EXCEPTION 'Document schema admission decision has an invalid evidence set';
 END IF;
 SELECT count(*),COALESCE(sum(snapshot_bytes),0)
 INTO members,total FROM document_revision_schema_admissions WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF members>=64 OR total+octet_length(NEW.body::text)::bigint+octet_length(NEW.metadata::text)+octet_length(NEW.manifest::text)>67108864 THEN
  RAISE EXCEPTION 'Document schema admission batch exceeds snapshot bound';
 END IF;
 NEW.previous_mutation_revision := selection.sampled_revision;
 NEW.creation_xid := pg_current_xact_id();
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_schema_admission_guard BEFORE INSERT OR UPDATE OR DELETE ON document_revision_schema_admissions
 FOR EACH ROW EXECUTE FUNCTION guard_document_schema_admission();

ALTER TABLE document_revision_commits DROP CONSTRAINT document_revision_commits_admission_mode_check;
ALTER TABLE document_revision_commits ADD CONSTRAINT document_revision_commits_admission_mode_check CHECK(admission_mode IN ('OPAQUE','TYPED'));

CREATE FUNCTION require_document_schema_admission_commit(c document_revision_commits) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE a document_revision_schema_admissions%ROWTYPE;
BEGIN
 SELECT * INTO a FROM document_revision_schema_admissions WHERE revision_id=c.revision_id;
 IF NOT FOUND THEN
  IF c.admission_mode<>'OPAQUE' THEN RAISE EXCEPTION 'Typed revision requires an admission binding'; END IF;
  PERFORM require_document_schema_policy_absent(c.account_id);
  RETURN true;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM document_schema_policy_current p WHERE p.account_id=a.account_id
  AND p.policy_revision=a.policy_revision AND p.policy_sha256=a.policy_sha256) THEN
  RAISE EXCEPTION 'Document schema admission policy changed after binding';
 END IF;
 IF a.creation_xid<>pg_current_xact_id() OR c.creation_xid<>a.creation_xid
  OR ROW(a.account_id,a.principal,a.operation_id,a.owner_generation,a.member_id,a.node_id,a.selection_revision,a.decision)
   IS DISTINCT FROM ROW(c.account_id,c.principal,c.operation_id,c.owner_generation,c.member_id,c.node_id,c.selection_revision,c.admission_mode)
  OR a.metadata IS DISTINCT FROM c.metadata_snapshot THEN
  RAISE EXCEPTION 'Document revision differs from its admission binding';
 END IF;
 RETURN true;
END;
$$;
CREATE FUNCTION guard_document_commit_schema_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_document_schema_admission_commit(NEW);
 RETURN NEW;
END;
$$;
-- Run after document_revision_commit_guard stamps metadata and creation_xid.
CREATE TRIGGER z_document_commit_schema_admission BEFORE INSERT ON document_revision_commits
 FOR EACH ROW EXECUTE FUNCTION guard_document_commit_schema_admission();

CREATE OR REPLACE FUNCTION guard_document_body_schema_policy() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous BIGINT := 0;
BEGIN
 IF TG_OP='UPDATE' THEN
  previous := OLD.mutation_revision;
  IF NEW.account_id IS DISTINCT FROM OLD.account_id THEN
   RAISE EXCEPTION 'Document account identity is immutable; publish a new addressed document';
  END IF;
  IF document_publication_body(NEW) IS NOT DISTINCT FROM document_publication_body(OLD)
   AND NOT EXISTS(SELECT 1 FROM document_revision_schema_admissions a WHERE a.node_id=NEW.node_id
    AND a.creation_xid=pg_current_xact_id()) THEN RETURN NEW; END IF;
 END IF;
 PERFORM lock_document_schema_policy_account(NEW.account_id,false);
 IF EXISTS(SELECT 1 FROM document_schema_policy_current WHERE account_id=NEW.account_id)
  AND NOT EXISTS(SELECT 1 FROM document_revision_schema_admissions a WHERE a.account_id=NEW.account_id AND a.node_id=NEW.node_id
   AND a.creation_xid=pg_current_xact_id() AND a.previous_mutation_revision=previous AND a.body=document_publication_body(NEW) AND a.metadata=document_revision_metadata_v1(NEW)) THEN
  RAISE EXCEPTION 'Publication requires an explicit schema policy binding';
 END IF;
 RETURN NEW;
END;
$$;
CREATE OR REPLACE FUNCTION guard_document_publication_schema_policy() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c document_revision_commits%ROWTYPE; a document_revision_schema_admissions%ROWTYPE; scoped_account TEXT;
BEGIN
 SELECT account_id INTO STRICT scoped_account FROM documents WHERE node_id=NEW.node_id;
 SELECT * INTO a FROM document_revision_schema_admissions WHERE revision_id=NEW.revision_id;
 IF NOT FOUND THEN PERFORM require_document_schema_policy_absent(scoped_account); RETURN NEW; END IF;
 SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.native_binding;
 PERFORM require_document_schema_admission_commit(c);
 IF NEW.native_binding IS DISTINCT FROM a.revision_id OR NEW.node_id<>a.node_id OR a.account_id<>scoped_account
  OR NEW.body IS DISTINCT FROM a.body THEN RAISE EXCEPTION 'Document projection differs from its admission binding'; END IF;
 RETURN NEW;
END;
$$;

CREATE FUNCTION require_document_schema_admission_complete(id UUID, terminal BOOLEAN) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE a document_revision_schema_admissions%ROWTYPE; c document_revision_commits%ROWTYPE; r document_revision_publications%ROWTYPE;
BEGIN
 SELECT * INTO STRICT a FROM document_revision_schema_admissions WHERE revision_id=id;
 SELECT * INTO c FROM document_revision_commits WHERE revision_id=id;
 IF NOT FOUND THEN RAISE EXCEPTION 'Document schema admission requires its native revision'; END IF;
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=id;
 PERFORM require_document_schema_admission_commit(c);
 IF r.projection_xid<>a.creation_xid OR r.native_binding IS DISTINCT FROM id OR r.body IS DISTINCT FROM a.body
  OR document_schema_retention_manifest_v1(id) IS DISTINCT FROM a.manifest THEN
  RAISE EXCEPTION 'Document schema admission retained sets differ';
 END IF;
 IF terminal AND (NOT r.projection_sealed OR NOT EXISTS(SELECT 1 FROM repository_operation_success s
  WHERE s.account_id=a.account_id AND s.principal=a.principal AND s.operation_id=a.operation_id
   AND s.owner_generation=a.owner_generation AND s.creation_xid=a.creation_xid AND s.command_sha256=a.command_sha256)) THEN
  RAISE EXCEPTION 'Document schema admission requires its sealed revision and terminal result';
 END IF;
 RETURN true;
END;
$$;
CREATE FUNCTION guard_document_schema_admission_seal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.projection_sealed AND NOT OLD.projection_sealed
  AND EXISTS(SELECT 1 FROM document_revision_schema_admissions WHERE revision_id=NEW.revision_id) THEN
  PERFORM require_document_schema_admission_complete(NEW.revision_id,false);
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_schema_admission_seal BEFORE UPDATE ON document_revision_publications
 FOR EACH ROW EXECUTE FUNCTION guard_document_schema_admission_seal();
CREATE FUNCTION require_document_schema_admission_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_document_schema_admission_complete(NEW.revision_id,true);
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_schema_admission_terminal AFTER INSERT ON document_revision_schema_admissions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_schema_admission_terminal();

-- Policy content is validated and canonically encoded by the library before
-- insertion. SQL protects integrity and monotonic activation, not protobuf rules.
-- This catalog does not activate typed publication or authorize administration.
CREATE TABLE document_schema_policies (
 account_id VARCHAR(200) NOT NULL CHECK(account_id ~ '\S'),
 policy_sha256 BYTEA NOT NULL CHECK(octet_length(policy_sha256)=32),
 policy_codec TEXT NOT NULL CHECK(policy_codec='document-schema-policy'),
 policy_version INTEGER NOT NULL CHECK(policy_version=1),
 policy_bytes BYTEA NOT NULL CHECK(octet_length(policy_bytes) BETWEEN 1 AND 524288),
 PRIMARY KEY(account_id,policy_sha256),
 CHECK(policy_sha256=sha256(policy_bytes))
);

CREATE FUNCTION reject_document_schema_policy_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'Document schema policy snapshots are immutable';
END;
$$;
CREATE TRIGGER document_schema_policy_immutable BEFORE UPDATE OR DELETE ON document_schema_policies
 FOR EACH ROW EXECUTE FUNCTION reject_document_schema_policy_mutation();

CREATE TABLE document_schema_policy_current (
 account_id VARCHAR(200) PRIMARY KEY,
 policy_revision BIGINT NOT NULL CHECK(policy_revision>0),
 policy_sha256 BYTEA NOT NULL,
 CONSTRAINT document_schema_policy_current_snapshot FOREIGN KEY(account_id,policy_sha256)
  REFERENCES document_schema_policies(account_id,policy_sha256)
);

CREATE FUNCTION guard_document_schema_policy_current() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Document schema policy pointers cannot be deleted'; END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.policy_revision<>1 THEN RAISE EXCEPTION 'Initial policy revision must be one'; END IF;
 ELSE
  IF NEW.account_id IS DISTINCT FROM OLD.account_id
   OR NEW.policy_revision IS DISTINCT FROM OLD.policy_revision+1 THEN
   RAISE EXCEPTION 'Policy update requires the next revision in the same account';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_schema_policy_current_guard BEFORE INSERT OR UPDATE OR DELETE ON document_schema_policy_current
 FOR EACH ROW EXECUTE FUNCTION guard_document_schema_policy_current();

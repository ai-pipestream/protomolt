-- Policy activation has to serialize even when no current-policy row exists.
-- The two-int advisory namespace is separate from document bigint lock keys.
-- Hash collisions may serialize accounts; all decisions still use exact IDs.
CREATE FUNCTION lock_document_schema_policy_account(scoped_account TEXT, exclusive_mode BOOLEAN)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF scoped_account IS NULL OR scoped_account !~ '\S' OR exclusive_mode IS NULL THEN
  RAISE EXCEPTION 'Schema policy fence requires an account and lock mode';
 END IF;
 IF exclusive_mode THEN
  PERFORM pg_advisory_xact_lock(1347244880,hashtext(scoped_account));
 ELSE
  PERFORM pg_advisory_xact_lock_shared(1347244880,hashtext(scoped_account));
 END IF;
END;
$$;

CREATE OR REPLACE FUNCTION guard_document_schema_policy_current() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Document schema policy pointers cannot be deleted'; END IF;
 IF TG_OP='INSERT' THEN
  -- BEFORE INSERT runs before the new pointer exists. UPDATE already holds its
  -- row lock: taking an advisory lock there would invert the writer lock order.
  PERFORM lock_document_schema_policy_account(NEW.account_id,true);
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

-- Both existing publishers are unbound. A future typed publisher must supply
-- complete policy/evidence bindings before this guard permits that path.
CREATE FUNCTION require_document_schema_policy_absent(scoped_account TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
 PERFORM lock_document_schema_policy_account(scoped_account,false);
 IF EXISTS(SELECT 1 FROM document_schema_policy_current WHERE account_id=scoped_account) THEN
  RAISE EXCEPTION 'Publication requires an explicit schema policy binding';
 END IF;
END;
$$;

CREATE FUNCTION guard_document_publication_schema_policy() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE scoped_account TEXT;
BEGIN
 SELECT account_id INTO STRICT scoped_account FROM documents WHERE node_id=NEW.node_id;
 PERFORM require_document_schema_policy_absent(scoped_account);
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_document_publication_schema_policy BEFORE INSERT ON document_revision_publications
 FOR EACH ROW EXECUTE FUNCTION guard_document_publication_schema_policy();

-- Older unmanaged rows need not create a revision projection. Protect their
-- bodies too, while leaving dedupe/status/security operations to their own gates.
CREATE FUNCTION guard_document_body_schema_policy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
  IF NEW.account_id IS DISTINCT FROM OLD.account_id THEN
   RAISE EXCEPTION 'Document account identity is immutable; publish a new addressed document';
  END IF;
  IF document_publication_body(NEW) IS NOT DISTINCT FROM document_publication_body(OLD) THEN RETURN NEW; END IF;
 END IF;
 PERFORM require_document_schema_policy_absent(NEW.account_id);
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_document_body_schema_policy BEFORE INSERT OR UPDATE ON documents
 FOR EACH ROW EXECUTE FUNCTION guard_document_body_schema_policy();

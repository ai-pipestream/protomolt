-- The unique immutable scope arbitrates first claimed versus unclaimed admission.
-- An existing unclaimed operation can never be retroactively adopted by a claim.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM repository_operations JOIN repository_execution_claims
           USING(account_id,principal,operation_id)) THEN
  RAISE EXCEPTION 'Existing operation and standalone execution claim overlap; explicit recovery migration required';
 END IF;
END $$;
CREATE TABLE repository_execution_scopes (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 claim_required boolean NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id)
);
INSERT INTO repository_execution_scopes SELECT account_id,principal,operation_id,true FROM repository_execution_claims;
INSERT INTO repository_execution_scopes SELECT account_id,principal,operation_id,false FROM repository_operations;
CREATE FUNCTION protect_repository_execution_scope() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Repository execution scope is immutable'; END;
$$;
CREATE TRIGGER repository_execution_scope_immutable BEFORE UPDATE OR DELETE ON repository_execution_scopes
 FOR EACH ROW EXECUTE FUNCTION protect_repository_execution_scope();
ALTER TABLE repository_execution_claims ADD FOREIGN KEY(account_id,principal,operation_id)
 REFERENCES repository_execution_scopes(account_id,principal,operation_id);
ALTER TABLE repository_operations ADD FOREIGN KEY(account_id,principal,operation_id)
 REFERENCES repository_execution_scopes(account_id,principal,operation_id);

CREATE FUNCTION establish_repository_execution_scope(a text,p text,o uuid,claimed boolean)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE required boolean;
BEGIN
 PERFORM require_repository_read_committed();
 INSERT INTO repository_execution_scopes VALUES(a,p,o,claimed)
 ON CONFLICT(account_id,principal,operation_id) DO NOTHING;
 -- A separate statement observes the committed winner after a unique-key wait.
 SELECT claim_required INTO STRICT required FROM repository_execution_scopes
 WHERE account_id=a AND principal=p AND operation_id=o;
 IF claimed AND NOT required THEN RAISE EXCEPTION 'Unclaimed operation cannot acquire an execution claim'; END IF;
 RETURN required;
END;
$$;
CREATE FUNCTION establish_repository_claim_scope() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM establish_repository_execution_scope(NEW.account_id,NEW.principal,NEW.operation_id,true);
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_repository_claim_scope BEFORE INSERT ON repository_execution_claims
 FOR EACH ROW EXECUTE FUNCTION establish_repository_claim_scope();

ALTER TABLE repository_execution_claims ADD COLUMN write_fence_xid xid8;
ALTER TABLE repository_execution_claims ADD COLUMN fence_epoch bigint,
 ADD COLUMN fence_token uuid,
 ADD CONSTRAINT repository_claim_proof_consumed CHECK(fence_epoch IS NULL AND fence_token IS NULL);
CREATE FUNCTION stamp_repository_execution_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.fence_epoch IS DISTINCT FROM NEW.claim_epoch OR NEW.fence_token IS DISTINCT FROM NEW.claim_token THEN
  RAISE EXCEPTION 'Execution fence requires explicit claim identity';
 END IF;
 NEW.write_fence_xid := pg_current_xact_id();
 NEW.fence_epoch := NULL; NEW.fence_token := NULL;
 RETURN NEW;
END;
$$;
CREATE TRIGGER zz_repository_claim_stamp BEFORE INSERT OR UPDATE ON repository_execution_claims
 FOR EACH ROW EXECUTE FUNCTION stamp_repository_execution_claim();

CREATE FUNCTION require_repository_execution_claim(a text,p text,o uuid)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE required boolean;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT claim_required INTO required FROM repository_execution_scopes WHERE account_id=a AND principal=p AND operation_id=o;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'Repository execution scope is absent';
 END IF;
 IF required AND NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=a AND principal=p AND operation_id=o
  AND write_fence_xid=pg_current_xact_id_if_assigned() AND lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository mutation requires a live execution claim write fence in this transaction';
 END IF;
 RETURN true;
END;
$$;
CREATE FUNCTION fence_repository_command_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM establish_repository_execution_scope(NEW.account_id,NEW.principal,NEW.operation_id,false);
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND command_sha256<>NEW.command_sha256) THEN
  RAISE EXCEPTION 'Execution claim command differs from operation';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_repository_command_execution BEFORE INSERT ON repository_operations
 FOR EACH ROW EXECUTE FUNCTION fence_repository_command_execution();
CREATE FUNCTION fence_repository_owner_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Existing cleanup proof grants no publication write authority. Preserve it
 -- for expired/terminal operations without turning it into a live claim.
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN RETURN NEW; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_repository_owner_execution BEFORE INSERT OR UPDATE ON repository_operation_owners
 FOR EACH ROW EXECUTE FUNCTION fence_repository_owner_execution();

CREATE OR REPLACE FUNCTION require_repository_operation_write_fence(
 scoped_account text, scoped_principal text, scoped_operation uuid, scoped_generation bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_execution_claim(scoped_account,scoped_principal,scoped_operation);
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=scoped_account
  AND principal=scoped_principal AND operation_id=scoped_operation)
 OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=scoped_account
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
CREATE OR REPLACE FUNCTION require_repository_operation_success_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE count_members BIGINT; first_ordinal INTEGER; last_ordinal INTEGER; member document_revision_commits%ROWTYPE;
 total_parts BIGINT;
BEGIN
 PERFORM require_repository_read_committed();
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
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

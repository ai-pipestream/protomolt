-- An exact preparation opts this owner generation into the private journal.
CREATE FUNCTION guard_repository_journaled_owner() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE preparation repository_publication_preparations%ROWTYPE; modes repository_publication_modes%ROWTYPE;
BEGIN
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN RETURN NEW; END IF;
 SELECT * INTO preparation FROM repository_publication_preparations WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.owner_generation-1;
 IF NOT FOUND THEN RETURN NEW; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 SELECT * INTO modes FROM repository_publication_modes WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.owner_generation-1;
 IF NOT FOUND THEN RAISE EXCEPTION 'Journaled owner requires fixed modes'; END IF;
 IF preparation.owner_nonce<>NEW.owner_token OR modes.owner_nonce<>NEW.owner_token THEN
  RAISE EXCEPTION 'Journaled owner differs from preparation';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_journaled_owner_guard BEFORE INSERT OR UPDATE ON repository_operation_owners
 FOR EACH ROW EXECUTE FUNCTION guard_repository_journaled_owner();

CREATE OR REPLACE FUNCTION require_repository_assessment_start(a text,p text,o uuid,g bigint,id uuid,digest bytea,deadline timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE start repository_publication_assessment_starts%ROWTYPE; nonce uuid;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations WHERE account_id=a AND principal=p AND operation_id=o
   AND predecessor_generation=g-1) THEN RETURN; END IF;
 PERFORM require_repository_execution_claim(a,p,o);
 IF NOT EXISTS(SELECT 1 FROM repository_publication_modes WHERE account_id=a AND principal=p AND operation_id=o
   AND predecessor_generation=g-1) THEN RAISE EXCEPTION 'Journaled assessment requires fixed modes'; END IF;
 SELECT * INTO start FROM repository_publication_assessment_starts WHERE account_id=a AND principal=p AND operation_id=o
  AND predecessor_generation=g-1 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment requires durable start'; END IF;
 IF start.started_xid=pg_current_xact_id_if_assigned() THEN RAISE EXCEPTION 'Assessment start must be committed before creation'; END IF;
 SELECT owner_token INTO STRICT nonce FROM repository_operation_owners WHERE account_id=a AND principal=p AND operation_id=o;
 IF ROW(start.owner_nonce,start.command_sha256,start.assessment_id,start.retain_until)
  IS DISTINCT FROM ROW(nonce,digest,id,deadline) THEN RAISE EXCEPTION 'Assessment differs from durable start'; END IF;
END;
$$;

-- SQL admission closure only. Existing attempts may still issue provider calls;
-- local start permits and actual shutdown are required before LOCAL_DRAINED.
CREATE TABLE repository_coordinator_drains (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 claim_epoch bigint NOT NULL,
 claim_token uuid NOT NULL,
 incarnation uuid NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,claim_epoch),
 FOREIGN KEY(account_id,principal,operation_id,claim_epoch)
  REFERENCES repository_coordinator_bindings(account_id,principal,operation_id,claim_epoch)
);
CREATE FUNCTION protect_repository_coordinator_drain() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator drain is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF NOT EXISTS(SELECT 1 FROM repository_coordinator_bindings b JOIN repository_execution_claims c
  USING(account_id,principal,operation_id)
  WHERE b.account_id=NEW.account_id AND b.principal=NEW.principal AND b.operation_id=NEW.operation_id
  AND b.claim_epoch=NEW.claim_epoch AND b.claim_token=NEW.claim_token AND b.incarnation=NEW.incarnation
  AND c.claim_epoch=NEW.claim_epoch AND c.claim_token=NEW.claim_token) THEN
  RAISE EXCEPTION 'Coordinator drain requires original binding and current claim';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_drain_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_drains FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_drain();

CREATE FUNCTION require_repository_coordinator_admission(a text,p text,o uuid)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_execution_claim(a,p,o);
 -- Any epoch: low-level transfer cannot erase an unfinished drain protocol.
 IF EXISTS(SELECT 1 FROM repository_coordinator_drains WHERE account_id=a AND principal=p AND operation_id=o) THEN
  RAISE EXCEPTION 'Coordinator is draining; new admission is closed';
 END IF;
END;
$$;
CREATE FUNCTION guard_repository_coordinator_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
  IF NEW.owner_generation=OLD.owner_generation THEN RETURN NEW; END IF;
 END IF;
 PERFORM require_repository_coordinator_admission(NEW.account_id,NEW.principal,NEW.operation_id);
 RETURN NEW;
END;
$$;
-- AFTER INSERT deliberately excludes exact ON CONFLICT DO NOTHING retries.
CREATE TRIGGER coordinator_command_admission AFTER INSERT ON repository_operations
 FOR EACH ROW EXECUTE FUNCTION guard_repository_coordinator_admission();
CREATE TRIGGER coordinator_owner_admission AFTER INSERT OR UPDATE ON repository_operation_owners
 FOR EACH ROW EXECUTE FUNCTION guard_repository_coordinator_admission();
CREATE TRIGGER coordinator_preparation_admission AFTER INSERT ON repository_publication_preparations
 FOR EACH ROW EXECUTE FUNCTION guard_repository_coordinator_admission();
CREATE TRIGGER coordinator_modes_admission AFTER INSERT ON repository_publication_modes
 FOR EACH ROW EXECUTE FUNCTION guard_repository_coordinator_admission();
CREATE TRIGGER coordinator_assessment_start_admission AFTER INSERT ON repository_publication_assessment_starts
 FOR EACH ROW EXECUTE FUNCTION guard_repository_coordinator_admission();
CREATE FUNCTION guard_repository_coordinator_attempt_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.plan_kind='NEW_CONTENT' THEN
  PERFORM require_repository_coordinator_admission(NEW.account_id,NEW.operation_principal,NEW.operation_id);
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER coordinator_attempt_admission AFTER INSERT ON document_part_attempts
 FOR EACH ROW EXECUTE FUNCTION guard_repository_coordinator_attempt_admission();

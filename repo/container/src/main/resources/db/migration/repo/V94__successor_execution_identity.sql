-- Private execution identity; business authorization remains checked at each operation.
CREATE TABLE repository_successor_executions (
 account_id varchar(200) NOT NULL, principal varchar(200) NOT NULL, operation_id uuid NOT NULL,
 claim_epoch bigint NOT NULL, claim_token uuid NOT NULL, incarnation uuid NOT NULL,
 owner_generation bigint NOT NULL, owner_nonce uuid NOT NULL,
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 modes_sha256 bytea NOT NULL CHECK(octet_length(modes_sha256)=32),
 activation_xid xid8 NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,claim_epoch),
 FOREIGN KEY(account_id,principal,operation_id,claim_epoch)
  REFERENCES repository_successor_installs(account_id,principal,operation_id,successor_epoch)
);
CREATE FUNCTION protect_repository_successor_execution() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c repository_execution_claims%ROWTYPE; w repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Successor execution identity is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO c FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (c.claim_epoch,c.claim_token,c.command_sha256) IS DISTINCT FROM (NEW.claim_epoch,NEW.claim_token,NEW.command_sha256)
 OR c.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'Execution requires exact live successor claim'; END IF;
 SELECT * INTO w FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (w.owner_generation,w.owner_token) IS DISTINCT FROM (NEW.owner_generation,NEW.owner_nonce)
 OR w.lease_until<=clock_timestamp() OR c.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'Execution requires exact live installed owner'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_successor_installs WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
 AND successor_epoch=NEW.claim_epoch AND successor_token=NEW.claim_token AND successor_incarnation=NEW.incarnation
 AND predecessor_generation=NEW.owner_generation-1 AND owner_nonce=NEW.owner_nonce AND command_sha256=NEW.command_sha256
 AND preparation_sha256=NEW.preparation_sha256 AND modes_sha256=NEW.modes_sha256 AND install_xid<>pg_current_xact_id_if_assigned()) THEN
 RAISE EXCEPTION 'Execution differs from committed install'; END IF;
 IF EXISTS(SELECT 1 FROM repository_coordinator_drains WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id AND claim_epoch=NEW.claim_epoch)
 OR EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
 OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
 RAISE EXCEPTION 'Closed successor cannot activate'; END IF;
 NEW.activation_xid:=pg_current_xact_id(); RETURN NEW;
END; $$;
CREATE TRIGGER repository_successor_execution_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_successor_executions
 FOR EACH ROW EXECUTE FUNCTION protect_repository_successor_execution();
ALTER TABLE repository_coordinator_bindings DROP CONSTRAINT repository_coordinator_bindings_claim_epoch_check;
ALTER TABLE repository_coordinator_bindings ADD CHECK(claim_epoch>0);

CREATE FUNCTION repository_successor_execution_open(a text,p text,o uuid)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM repository_successor_executions e JOIN repository_execution_claims c
 USING(account_id,principal,operation_id) JOIN repository_operation_owners w USING(account_id,principal,operation_id)
 JOIN repository_coordinator_bindings b ON (b.account_id,b.principal,b.operation_id,b.claim_epoch)=(e.account_id,e.principal,e.operation_id,e.claim_epoch)
 WHERE e.account_id=a AND e.principal=p AND e.operation_id=o AND c.claim_epoch=e.claim_epoch AND c.claim_token=e.claim_token
 AND b.claim_token=e.claim_token AND b.incarnation=e.incarnation AND w.owner_generation=e.owner_generation AND w.owner_token=e.owner_nonce
 AND c.lease_until>clock_timestamp() AND w.lease_until>clock_timestamp()
 AND NOT EXISTS(SELECT 1 FROM repository_coordinator_local_drains d WHERE d.account_id=a AND d.principal=p AND d.operation_id=o AND d.claim_epoch=e.claim_epoch));
$$;
CREATE FUNCTION repository_successor_admission(a text,p text,o uuid,g bigint)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT repository_successor_execution_open(a,p,o) AND EXISTS(SELECT 1 FROM repository_successor_executions e
 JOIN repository_execution_claims c USING(account_id,principal,operation_id)
 WHERE e.account_id=a AND e.principal=p AND e.operation_id=o AND e.claim_epoch=c.claim_epoch AND e.owner_generation=g
 AND NOT EXISTS(SELECT 1 FROM repository_coordinator_drains d WHERE d.account_id=a AND d.principal=p AND d.operation_id=o AND d.claim_epoch=e.claim_epoch));
$$;
CREATE FUNCTION require_repository_successor_activation_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT repository_successor_execution_open(NEW.account_id,NEW.principal,NEW.operation_id) THEN
 RAISE EXCEPTION 'Successor activation requires live exact coordinator binding'; END IF;
 RETURN NULL;
END; $$;
CREATE CONSTRAINT TRIGGER repository_successor_activation_complete AFTER INSERT ON repository_successor_executions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_successor_activation_complete();

CREATE OR REPLACE FUNCTION protect_repository_coordinator_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator binding is immutable'; END IF;
 IF NEW.claim_epoch>1 THEN
  IF NOT EXISTS(SELECT 1 FROM repository_successor_executions e JOIN repository_execution_claims c USING(account_id,principal,operation_id)
   WHERE e.account_id=NEW.account_id AND e.principal=NEW.principal AND e.operation_id=NEW.operation_id
   AND e.claim_epoch=NEW.claim_epoch AND e.claim_token=NEW.claim_token AND e.incarnation=NEW.incarnation
   AND e.activation_xid=pg_current_xact_id_if_assigned() AND c.claim_epoch=e.claim_epoch AND c.claim_token=e.claim_token
   AND c.lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'Successor binding requires exact activation transaction'; END IF;
  RETURN NEW;
 END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND claim_epoch=NEW.claim_epoch AND claim_token=NEW.claim_token) THEN
  RAISE EXCEPTION 'Coordinator binding differs from current claim';
 END IF;
 IF EXISTS(SELECT 1 FROM repository_publication_preparations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Coordinator binding must precede preparation and operation admission';
 END IF;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION refuse_locally_drained_claim_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.claim_epoch=OLD.claim_epoch AND EXISTS(SELECT 1 FROM repository_coordinator_local_drains
  WHERE account_id=OLD.account_id AND principal=OLD.principal AND operation_id=OLD.operation_id)
 AND NOT repository_successor_execution_open(OLD.account_id,OLD.principal,OLD.operation_id) THEN
  RAISE EXCEPTION 'Coordinator is locally drained; execution is closed';
 END IF;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION require_repository_execution_claim(a text,p text,o uuid)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE required boolean;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT claim_required INTO required FROM repository_execution_scopes WHERE account_id=a AND principal=p AND operation_id=o;
 IF NOT FOUND THEN RAISE EXCEPTION 'Repository execution scope is absent'; END IF;
 -- Any epoch: low-level claim transfer is not a reviewed successor protocol.
 IF required AND EXISTS(SELECT 1 FROM repository_coordinator_local_drains
  WHERE account_id=a AND principal=p AND operation_id=o)
 AND NOT repository_successor_execution_open(a,p,o) THEN
  RAISE EXCEPTION 'Coordinator is locally drained; execution is closed';
 END IF;
 IF required AND NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=a AND principal=p AND operation_id=o
  AND write_fence_xid=pg_current_xact_id_if_assigned() AND lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository mutation requires a live execution claim write fence in this transaction';
 END IF;
 RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION guard_repository_coordinator_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='repository_publication_assessment_starts' THEN
  IF repository_successor_admission(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation+1) THEN
   PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id); RETURN NEW;
  END IF;
 END IF;
 IF TG_TABLE_NAME='repository_publication_preparations' THEN
  IF repository_successor_preparation(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce,NEW.command_sha256,NEW.preparation_sha256) THEN RETURN NEW; END IF;
 ELSIF TG_TABLE_NAME='repository_publication_modes' THEN
  IF repository_successor_modes(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce,NEW.modes) THEN RETURN NEW; END IF;
 ELSIF TG_TABLE_NAME='repository_operation_owners' AND TG_OP='UPDATE' THEN
  IF repository_successor_owner(OLD,NEW) THEN RETURN NEW; END IF;
 END IF;
 IF TG_OP='UPDATE' THEN
  IF NEW.owner_generation=OLD.owner_generation THEN RETURN NEW; END IF;
 END IF;
 PERFORM require_repository_coordinator_admission(NEW.account_id,NEW.principal,NEW.operation_id);
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION guard_repository_coordinator_attempt_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.plan_kind='NEW_CONTENT' THEN
  IF repository_successor_admission(NEW.account_id,NEW.operation_principal,NEW.operation_id,NEW.operation_generation) THEN
   PERFORM require_repository_execution_claim(NEW.account_id,NEW.operation_principal,NEW.operation_id); RETURN NEW;
  END IF;
  PERFORM require_repository_coordinator_admission(NEW.account_id,NEW.operation_principal,NEW.operation_id);
 END IF;
 RETURN NEW;
END;
$$;

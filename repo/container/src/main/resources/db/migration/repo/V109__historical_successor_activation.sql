-- Private activation evidence, not permission to publish or release retained history.
ALTER TABLE repository_successor_executions ADD COLUMN historical_capture_required boolean NOT NULL DEFAULT false;

CREATE TABLE repository_historical_activations (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 claim_epoch bigint NOT NULL,
 claim_token uuid NOT NULL,
 incarnation uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 retention_generation bigint NOT NULL CHECK(retention_generation>=0 AND retention_generation<predecessor_generation),
 retention_sha256 bytea NOT NULL CHECK(octet_length(retention_sha256)=32),
 pins_sha256 bytea NOT NULL CHECK(octet_length(pins_sha256)=32),
 creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 PRIMARY KEY(account_id,principal,operation_id,claim_epoch),
 FOREIGN KEY(account_id,principal,operation_id,claim_epoch) REFERENCES repository_successor_executions,
 FOREIGN KEY(account_id,principal,operation_id,retention_generation,pins_sha256)
  REFERENCES repository_preparation_pin_batches(account_id,principal,operation_id,predecessor_generation,pins_sha256)
);

CREATE FUNCTION guard_repository_historical_activation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_generation bigint; current_digest bytea; edge repository_successor_installs%ROWTYPE; hops integer:=0;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Historical activation is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF NEW.creation_xid<>pg_current_xact_id() OR NOT EXISTS(
  SELECT 1 FROM repository_successor_executions e
  JOIN repository_coordinator_bindings i USING(account_id,principal,operation_id,claim_epoch)
  WHERE e.account_id=NEW.account_id AND e.principal=NEW.principal AND e.operation_id=NEW.operation_id
   AND e.claim_epoch=NEW.claim_epoch AND e.claim_token=NEW.claim_token AND e.incarnation=NEW.incarnation
   AND i.claim_token=NEW.claim_token AND i.incarnation=NEW.incarnation
   AND e.owner_generation=NEW.predecessor_generation+1 AND e.preparation_sha256=NEW.preparation_sha256
   AND e.command_sha256=NEW.command_sha256 AND e.historical_capture_required
   AND e.activation_xid=pg_current_xact_id()) THEN
  RAISE EXCEPTION 'Historical activation requires its exact creation execution';
 END IF;
 -- Immutable install edges establish ancestry, not just a numerically older generation.
 current_generation:=NEW.predecessor_generation; current_digest:=NEW.preparation_sha256;
 WHILE current_generation>NEW.retention_generation LOOP
  hops:=hops+1;
  IF hops>64 THEN RAISE EXCEPTION 'Historical activation ancestry exceeds 64 links'; END IF;
  SELECT * INTO STRICT edge FROM repository_successor_installs
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
    AND predecessor_generation=current_generation AND preparation_sha256=current_digest;
  IF edge.command_sha256<>NEW.command_sha256 THEN RAISE EXCEPTION 'Historical activation command ancestry differs'; END IF;
  current_digest:=edge.predecessor_preparation_sha256; current_generation:=current_generation-1;
 END LOOP;
 IF current_digest<>NEW.retention_sha256 OR NOT EXISTS(
  SELECT 1 FROM repository_preparation_history_sets h
  JOIN repository_preparation_pin_batches b USING(account_id,principal,operation_id,predecessor_generation)
  JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
  WHERE h.account_id=NEW.account_id AND h.principal=NEW.principal AND h.operation_id=NEW.operation_id
   AND h.predecessor_generation=NEW.retention_generation AND h.preparation_sha256=NEW.retention_sha256
   AND h.command_sha256=NEW.command_sha256 AND h.sealed AND h.expected_count>0
   AND b.pins_sha256=NEW.pins_sha256 AND b.sealed AND NOT b.initial_capture
   AND b.creation_xid=pg_current_xact_id()
   AND o.claim_epoch=NEW.claim_epoch AND o.claim_token=NEW.claim_token AND o.incarnation=NEW.incarnation) THEN
  RAISE EXCEPTION 'Historical activation differs from retained ancestry or capture';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_historical_activation_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_historical_activations FOR EACH ROW EXECUTE FUNCTION guard_repository_historical_activation();

CREATE FUNCTION require_repository_historical_activation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.historical_capture_required AND NOT EXISTS(SELECT 1 FROM repository_historical_activations
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND claim_epoch=NEW.claim_epoch AND creation_xid=NEW.activation_xid) THEN
  RAISE EXCEPTION 'Historical execution requires atomic capture binding';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_historical_activation_complete AFTER INSERT ON repository_successor_executions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_historical_activation();

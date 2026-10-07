-- Capture ownership is not drain evidence. Older batches deliberately remain unknown.
CREATE TABLE repository_preparation_pin_owners (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 pins_sha256 bytea NOT NULL,
 claim_epoch bigint NOT NULL CHECK(claim_epoch>0),
 claim_token uuid NOT NULL,
 incarnation uuid NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256)
  REFERENCES repository_preparation_pin_batches,
 FOREIGN KEY(account_id,principal,operation_id,claim_epoch)
  REFERENCES repository_coordinator_bindings
);

CREATE FUNCTION guard_repository_preparation_pin_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation capture owner is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_pin_batches b
  JOIN repository_preparation_history_sets h USING(account_id,principal,operation_id,predecessor_generation)
  JOIN repository_execution_claims c USING(account_id,principal,operation_id)
  JOIN repository_coordinator_bindings i ON i.account_id=c.account_id AND i.principal=c.principal
   AND i.operation_id=c.operation_id AND i.claim_epoch=c.claim_epoch
  WHERE b.account_id=NEW.account_id AND b.principal=NEW.principal AND b.operation_id=NEW.operation_id
   AND b.predecessor_generation=NEW.predecessor_generation AND b.pins_sha256=NEW.pins_sha256
   AND NOT b.sealed AND b.creation_xid=pg_current_xact_id()
   AND c.claim_epoch=NEW.claim_epoch AND c.claim_token=NEW.claim_token AND c.command_sha256=h.command_sha256
   AND c.lease_until>clock_timestamp() AND c.write_fence_xid=pg_current_xact_id()
   AND i.claim_token=NEW.claim_token AND i.incarnation=NEW.incarnation) THEN
  RAISE EXCEPTION 'Preparation capture owner requires its exact live bound creation claim';
 END IF;
 IF EXISTS(SELECT 1 FROM repository_coordinator_drains
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id AND claim_epoch=NEW.claim_epoch)
  OR EXISTS(SELECT 1 FROM repository_operation_success
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Closed execution cannot own a new preparation capture';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_pin_owner_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_pin_owners FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_pin_owner();

CREATE FUNCTION require_preparation_pin_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_pin_owners
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation AND pins_sha256=NEW.pins_sha256) THEN
  RAISE EXCEPTION 'New preparation capture requires its exact coordinator owner';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_preparation_pin_owner_complete AFTER INSERT
 ON repository_preparation_pin_batches DEFERRABLE INITIALLY DEFERRED
 FOR EACH ROW EXECUTE FUNCTION require_preparation_pin_owner();

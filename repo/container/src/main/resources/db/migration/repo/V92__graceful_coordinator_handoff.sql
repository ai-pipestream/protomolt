-- Private successor reservation only. V90/V91 execution closure remains unchanged.
CREATE TABLE repository_coordinator_handoffs (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_epoch bigint NOT NULL CHECK(predecessor_epoch BETWEEN 1 AND 9223372036854775806),
 predecessor_token uuid NOT NULL,
 predecessor_incarnation uuid NOT NULL,
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 successor_epoch bigint NOT NULL CHECK(successor_epoch=predecessor_epoch+1),
 successor_token uuid NOT NULL CHECK(successor_token<>predecessor_token),
 successor_incarnation uuid NOT NULL CHECK(successor_incarnation<>predecessor_incarnation),
 lease_millis bigint NOT NULL CHECK(lease_millis BETWEEN 1000 AND 86400000),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_epoch),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_epoch)
  REFERENCES repository_coordinator_local_drains(account_id,principal,operation_id,claim_epoch)
);

CREATE FUNCTION protect_repository_coordinator_handoff() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_claim repository_execution_claims%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator handoff is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO current_claim FROM repository_execution_claims
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR current_claim.claim_epoch IS DISTINCT FROM NEW.predecessor_epoch
  OR current_claim.claim_token IS DISTINCT FROM NEW.predecessor_token
  OR current_claim.command_sha256 IS DISTINCT FROM NEW.command_sha256
  OR current_claim.lease_until>clock_timestamp() THEN
  RAISE EXCEPTION 'Handoff requires exact expired predecessor claim';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_coordinator_local_drains
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND claim_epoch=NEW.predecessor_epoch AND claim_token=NEW.predecessor_token
   AND incarnation=NEW.predecessor_incarnation AND command_sha256=NEW.command_sha256) THEN
  RAISE EXCEPTION 'Handoff requires exact local drain';
 END IF;
 PERFORM 1 FROM repository_operation_owners WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Terminal operation cannot hand off';
 END IF;
 UPDATE repository_execution_claims SET claim_epoch=NEW.successor_epoch,claim_token=NEW.successor_token,
  lease_until=clock_timestamp()+(NEW.lease_millis*interval '1 millisecond'),
  fence_epoch=NEW.successor_epoch,fence_token=NEW.successor_token
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_handoff_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_handoffs FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_handoff();

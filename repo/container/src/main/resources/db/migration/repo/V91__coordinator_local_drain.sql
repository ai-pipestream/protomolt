-- Private host attestation, not evidence that remote provider effects have settled.
-- The host must drain all owned work before inserting; no public API grants this.
CREATE TABLE repository_coordinator_local_drains (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 claim_epoch bigint NOT NULL,
 claim_token uuid NOT NULL,
 incarnation uuid NOT NULL,
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,claim_epoch),
 FOREIGN KEY(account_id,principal,operation_id,claim_epoch)
  REFERENCES repository_coordinator_drains(account_id,principal,operation_id,claim_epoch)
);

CREATE FUNCTION protect_repository_coordinator_local_drain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_claim repository_execution_claims%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator local drain is immutable'; END IF;
 PERFORM require_repository_read_committed();
 -- The first lock is the claim. This records completed work, without renewing a
 -- lease or stamping execution authority, even when the unchanged lease expired.
 SELECT * INTO current_claim FROM repository_execution_claims
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR current_claim.claim_epoch IS DISTINCT FROM NEW.claim_epoch
  OR current_claim.claim_token IS DISTINCT FROM NEW.claim_token
  OR current_claim.command_sha256 IS DISTINCT FROM NEW.command_sha256 THEN
  RAISE EXCEPTION 'Local drain requires the original current claim';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_coordinator_drains d JOIN repository_coordinator_bindings b
  USING(account_id,principal,operation_id,claim_epoch)
  WHERE d.account_id=NEW.account_id AND d.principal=NEW.principal AND d.operation_id=NEW.operation_id
  AND d.claim_epoch=NEW.claim_epoch AND d.claim_token=NEW.claim_token AND d.incarnation=NEW.incarnation
  AND b.claim_token=NEW.claim_token AND b.incarnation=NEW.incarnation) THEN
  RAISE EXCEPTION 'Local drain requires exact coordinator admission closure';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_local_drain_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_local_drains FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_local_drain();

CREATE FUNCTION refuse_locally_drained_claim_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.claim_epoch=OLD.claim_epoch AND EXISTS(SELECT 1 FROM repository_coordinator_local_drains
  WHERE account_id=OLD.account_id AND principal=OLD.principal AND operation_id=OLD.operation_id) THEN
  RAISE EXCEPTION 'Coordinator is locally drained; execution is closed';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER a1_repository_local_drain_claim BEFORE UPDATE ON repository_execution_claims
 FOR EACH ROW EXECUTE FUNCTION refuse_locally_drained_claim_update();

CREATE OR REPLACE FUNCTION require_repository_execution_claim(a text,p text,o uuid)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE required boolean;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT claim_required INTO required FROM repository_execution_scopes WHERE account_id=a AND principal=p AND operation_id=o;
 IF NOT FOUND THEN RAISE EXCEPTION 'Repository execution scope is absent'; END IF;
 -- Any epoch: low-level claim transfer is not a reviewed successor protocol.
 IF required AND EXISTS(SELECT 1 FROM repository_coordinator_local_drains
  WHERE account_id=a AND principal=p AND operation_id=o) THEN
  RAISE EXCEPTION 'Coordinator is locally drained; execution is closed';
 END IF;
 IF required AND NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=a AND principal=p AND operation_id=o
  AND write_fence_xid=pg_current_xact_id_if_assigned() AND lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository mutation requires a live execution claim write fence in this transaction';
 END IF;
 RETURN true;
END;
$$;

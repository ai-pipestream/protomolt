-- Expiry permits a fenced reservation, never a claim that the predecessor stopped.
ALTER TABLE repository_coordinator_reservations
 DROP CONSTRAINT repository_coordinator_reservations_kind_check,
 ADD COLUMN predecessor_owner_generation bigint,
 ADD COLUMN predecessor_owner_nonce uuid,
 ADD CONSTRAINT repository_reservation_kind_owner CHECK(
  (kind='GRACEFUL' AND predecessor_owner_generation IS NULL AND predecessor_owner_nonce IS NULL)
  OR (kind='EXPIRED_UNQUIESCED' AND predecessor_owner_generation IS NOT NULL
   AND predecessor_owner_generation BETWEEN 1 AND 9223372036854775806 AND predecessor_owner_nonce IS NOT NULL));

CREATE TABLE repository_coordinator_expirations
 (LIKE repository_coordinator_handoffs INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
ALTER TABLE repository_coordinator_expirations
 ADD COLUMN predecessor_owner_generation bigint NOT NULL CHECK(predecessor_owner_generation BETWEEN 1 AND 9223372036854775806),
 ADD COLUMN predecessor_owner_nonce uuid NOT NULL,
 ADD FOREIGN KEY(account_id,principal,operation_id,predecessor_epoch)
 REFERENCES repository_coordinator_bindings(account_id,principal,operation_id,claim_epoch);

CREATE FUNCTION protect_repository_coordinator_expiration() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c repository_execution_claims%ROWTYPE; w repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator expiration is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO c FROM repository_execution_claims
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (c.claim_epoch,c.claim_token,c.command_sha256)
  IS DISTINCT FROM (NEW.predecessor_epoch,NEW.predecessor_token,NEW.command_sha256)
  OR c.lease_until>clock_timestamp() THEN RAISE EXCEPTION 'Expiration requires exact expired predecessor claim'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_coordinator_bindings b WHERE b.account_id=NEW.account_id
  AND b.principal=NEW.principal AND b.operation_id=NEW.operation_id AND b.claim_epoch=NEW.predecessor_epoch
  AND b.claim_token=NEW.predecessor_token AND b.incarnation=NEW.predecessor_incarnation) THEN
  RAISE EXCEPTION 'Expiration requires exact bound predecessor'; END IF;
 IF EXISTS(SELECT 1 FROM repository_coordinator_local_drains WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND claim_epoch=NEW.predecessor_epoch) THEN
  RAISE EXCEPTION 'Locally drained predecessor requires graceful handoff'; END IF;
 SELECT * INTO w FROM repository_operation_owners
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (w.owner_generation,w.owner_token)
  IS DISTINCT FROM (NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce)
  OR w.lease_until>clock_timestamp() THEN RAISE EXCEPTION 'Expiration requires exact expired owner'; END IF;
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id)
 OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Terminal operation cannot reserve expiration'; END IF;
 UPDATE repository_execution_claims SET claim_epoch=NEW.successor_epoch,claim_token=NEW.successor_token,
  lease_until=clock_timestamp()+(NEW.lease_millis*interval '1 millisecond'),
  fence_epoch=NEW.successor_epoch,fence_token=NEW.successor_token
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_expiration_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_expirations FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_expiration();

CREATE OR REPLACE FUNCTION protect_repository_coordinator_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source_matches boolean := false;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator reservation is immutable'; END IF;
 IF NEW.kind='GRACEFUL' THEN
  SELECT EXISTS(SELECT 1 FROM repository_coordinator_handoffs h
   WHERE (h.account_id,h.principal,h.operation_id,h.predecessor_epoch,h.predecessor_token,h.predecessor_incarnation,h.command_sha256,h.successor_epoch,h.successor_token,h.successor_incarnation,h.lease_millis,h.recorded_at) IS NOT DISTINCT FROM (NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at)) INTO source_matches;
 ELSIF NEW.kind='EXPIRED_UNQUIESCED' THEN
  SELECT EXISTS(SELECT 1 FROM repository_coordinator_expirations h
   WHERE (h.account_id,h.principal,h.operation_id,h.predecessor_epoch,h.predecessor_token,h.predecessor_incarnation,h.command_sha256,h.successor_epoch,h.successor_token,h.successor_incarnation,h.lease_millis,h.recorded_at,h.predecessor_owner_generation,h.predecessor_owner_nonce)
    IS NOT DISTINCT FROM (NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at,NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce)) INTO source_matches;
 END IF;
 IF NOT source_matches OR NEW.predecessor_remote_state<>'UNKNOWN' THEN
  RAISE EXCEPTION 'Coordinator reservation requires exact source evidence'; END IF;
 RETURN NEW;
END;
$$;
CREATE FUNCTION publish_repository_expired_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO repository_coordinator_reservations
 (account_id,principal,operation_id,predecessor_epoch,predecessor_token,predecessor_incarnation,command_sha256,successor_epoch,successor_token,successor_incarnation,lease_millis,recorded_at,kind,predecessor_remote_state,predecessor_owner_generation,predecessor_owner_nonce)
 VALUES(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at,'EXPIRED_UNQUIESCED','UNKNOWN',NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce);
 RETURN NULL;
END;
$$;
CREATE TRIGGER repository_expired_reservation AFTER INSERT ON repository_coordinator_expirations
 FOR EACH ROW EXECUTE FUNCTION publish_repository_expired_reservation();

CREATE OR REPLACE FUNCTION protect_repository_successor_install() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE claim repository_execution_claims%ROWTYPE; owner repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Successor install is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO claim FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (claim.claim_epoch,claim.claim_token,claim.command_sha256)
  IS DISTINCT FROM (NEW.successor_epoch,NEW.successor_token,NEW.command_sha256) OR claim.lease_until<=clock_timestamp() THEN
  RAISE EXCEPTION 'Install requires exact live successor'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_coordinator_reservations WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_epoch=NEW.predecessor_epoch AND successor_epoch=NEW.successor_epoch
  AND successor_token=NEW.successor_token AND successor_incarnation=NEW.successor_incarnation AND command_sha256=NEW.command_sha256
  AND (kind='GRACEFUL' OR (kind='EXPIRED_UNQUIESCED'
   AND predecessor_owner_generation=NEW.predecessor_generation AND predecessor_owner_nonce=NEW.predecessor_nonce))) THEN
  RAISE EXCEPTION 'Install differs from reserved handoff'; END IF;
 SELECT * INTO owner FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (owner.owner_generation,owner.owner_token) IS DISTINCT FROM (NEW.predecessor_generation,NEW.predecessor_nonce)
  OR owner.lease_until>clock_timestamp() THEN RAISE EXCEPTION 'Install requires exact expired owner'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation-1
  AND owner_nonce=NEW.predecessor_nonce AND preparation_sha256=NEW.predecessor_preparation_sha256
  AND command_sha256=NEW.command_sha256) THEN RAISE EXCEPTION 'Install differs from predecessor preparation'; END IF;
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Terminal operation cannot install successor'; END IF;
 NEW.install_xid:=pg_current_xact_id();
 RETURN NEW;
END; $$;

-- An unactivated reservation can expire without ever acquiring a coordinator binding.
-- Supersession preserves that fact; it never manufactures local drain or activation.
ALTER TABLE repository_coordinator_reservations DROP CONSTRAINT repository_reservation_kind_owner,
 ADD CONSTRAINT repository_reservation_kind_owner CHECK(
  (kind='GRACEFUL' AND predecessor_owner_generation IS NULL AND predecessor_owner_nonce IS NULL)
  OR (kind IN ('EXPIRED_UNQUIESCED','SUPERSEDED_UNACTIVATED') AND predecessor_owner_generation IS NOT NULL
   AND predecessor_owner_generation BETWEEN 1 AND 9223372036854775806 AND predecessor_owner_nonce IS NOT NULL));

CREATE TABLE repository_coordinator_supersessions
 (LIKE repository_coordinator_expirations INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
ALTER TABLE repository_coordinator_supersessions
 ADD CONSTRAINT repository_supersession_epoch CHECK(predecessor_epoch>=2),
 ADD COLUMN source_predecessor_epoch bigint GENERATED ALWAYS AS (predecessor_epoch-1) STORED,
 ADD COLUMN phase text NOT NULL CHECK(phase IN ('RESERVED_ONLY','INSTALLED')),
 ADD COLUMN preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 ADD COLUMN install_predecessor_preparation_sha256 bytea,
 ADD COLUMN install_preparation_sha256 bytea,
 ADD COLUMN install_modes_sha256 bytea,
 ADD CONSTRAINT repository_supersession_phase CHECK(
  (phase='RESERVED_ONLY' AND install_predecessor_preparation_sha256 IS NULL
   AND install_preparation_sha256 IS NULL AND install_modes_sha256 IS NULL)
  OR (phase='INSTALLED' AND install_predecessor_preparation_sha256 IS NOT NULL
   AND install_preparation_sha256 IS NOT NULL AND install_modes_sha256 IS NOT NULL
   AND octet_length(install_predecessor_preparation_sha256)=32
   AND octet_length(install_preparation_sha256)=32 AND octet_length(install_modes_sha256)=32
   AND preparation_sha256=install_preparation_sha256)),
 ADD FOREIGN KEY(account_id,principal,operation_id,source_predecessor_epoch)
 REFERENCES repository_coordinator_reservations(account_id,principal,operation_id,predecessor_epoch);

CREATE FUNCTION protect_repository_coordinator_supersession() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c repository_execution_claims%ROWTYPE; w repository_operation_owners%ROWTYPE;
 r repository_coordinator_reservations%ROWTYPE; i repository_successor_installs%ROWTYPE; installed boolean;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator supersession is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO c FROM repository_execution_claims
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (c.claim_epoch,c.claim_token,c.command_sha256)
  IS DISTINCT FROM (NEW.predecessor_epoch,NEW.predecessor_token,NEW.command_sha256)
  OR c.lease_until>clock_timestamp() THEN RAISE EXCEPTION 'Supersession requires exact expired reserved claim'; END IF;
 SELECT * INTO w FROM repository_operation_owners
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (w.owner_generation,w.owner_token)
  IS DISTINCT FROM (NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce)
  OR w.lease_until>clock_timestamp() THEN RAISE EXCEPTION 'Supersession requires exact expired owner'; END IF;
 SELECT * INTO r FROM repository_coordinator_reservations
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_epoch=NEW.predecessor_epoch-1;
 IF NOT FOUND OR (r.successor_epoch,r.successor_token,r.successor_incarnation,r.command_sha256)
  IS DISTINCT FROM (NEW.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256) THEN
  RAISE EXCEPTION 'Supersession requires exact predecessor reservation'; END IF;
 IF EXISTS(SELECT 1 FROM repository_coordinator_bindings WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND claim_epoch=NEW.predecessor_epoch)
  OR EXISTS(SELECT 1 FROM repository_successor_executions WHERE account_id=NEW.account_id AND principal=NEW.principal
   AND operation_id=NEW.operation_id AND claim_epoch=NEW.predecessor_epoch) THEN
  RAISE EXCEPTION 'Activated coordinator requires bound recovery'; END IF;
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Terminal operation cannot supersede reservation'; END IF;
 SELECT * INTO i FROM repository_successor_installs WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND successor_epoch=NEW.predecessor_epoch;
 installed:=FOUND;
 IF NEW.phase='RESERVED_ONLY' THEN
  IF installed THEN RAISE EXCEPTION 'Supersession phase changed to installed'; END IF;
  IF r.kind<>'GRACEFUL' AND (r.predecessor_owner_generation,r.predecessor_owner_nonce)
   IS DISTINCT FROM (w.owner_generation,w.owner_token) THEN
   RAISE EXCEPTION 'Reserved owner differs from source reservation'; END IF;
 ELSIF NEW.phase='INSTALLED' THEN
  IF NOT installed OR (i.predecessor_epoch,i.successor_token,i.successor_incarnation,i.command_sha256,
   i.predecessor_generation+1,i.owner_nonce,i.predecessor_preparation_sha256,i.preparation_sha256,i.modes_sha256)
   IS DISTINCT FROM (r.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256,
    NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce,NEW.install_predecessor_preparation_sha256,
    NEW.install_preparation_sha256,NEW.install_modes_sha256) THEN
   RAISE EXCEPTION 'Supersession requires exact prior installation'; END IF;
  IF r.kind<>'GRACEFUL' AND (r.predecessor_owner_generation,r.predecessor_owner_nonce)
   IS DISTINCT FROM (i.predecessor_generation,i.predecessor_nonce) THEN
   RAISE EXCEPTION 'Installed predecessor differs from source reservation'; END IF;
 ELSE RAISE EXCEPTION 'Unknown supersession phase'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations p JOIN repository_publication_modes m
  USING(account_id,principal,operation_id,predecessor_generation)
  WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id
   AND p.predecessor_generation=NEW.predecessor_owner_generation-1 AND p.owner_nonce=NEW.predecessor_owner_nonce
   AND m.owner_nonce=NEW.predecessor_owner_nonce AND p.command_sha256=NEW.command_sha256
   AND p.preparation_sha256=NEW.preparation_sha256) THEN
  RAISE EXCEPTION 'Supersession requires exact retained preparation and modes'; END IF;
 UPDATE repository_execution_claims SET claim_epoch=NEW.successor_epoch,claim_token=NEW.successor_token,
  lease_until=clock_timestamp()+(NEW.lease_millis*interval '1 millisecond'),
  fence_epoch=NEW.successor_epoch,fence_token=NEW.successor_token
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_supersession_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_supersessions FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_supersession();

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
 ELSIF NEW.kind='SUPERSEDED_UNACTIVATED' THEN
  SELECT EXISTS(SELECT 1 FROM repository_coordinator_supersessions h
   WHERE (h.account_id,h.principal,h.operation_id,h.predecessor_epoch,h.predecessor_token,h.predecessor_incarnation,h.command_sha256,h.successor_epoch,h.successor_token,h.successor_incarnation,h.lease_millis,h.recorded_at,h.predecessor_owner_generation,h.predecessor_owner_nonce)
    IS NOT DISTINCT FROM (NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at,NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce)) INTO source_matches;
 END IF;
 IF NOT source_matches OR NEW.predecessor_remote_state<>'UNKNOWN' THEN
  RAISE EXCEPTION 'Coordinator reservation requires exact source evidence'; END IF;
 RETURN NEW;
END;
$$;
CREATE FUNCTION publish_repository_superseded_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO repository_coordinator_reservations
 (account_id,principal,operation_id,predecessor_epoch,predecessor_token,predecessor_incarnation,command_sha256,successor_epoch,successor_token,successor_incarnation,lease_millis,recorded_at,kind,predecessor_remote_state,predecessor_owner_generation,predecessor_owner_nonce)
 VALUES(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at,'SUPERSEDED_UNACTIVATED','UNKNOWN',NEW.predecessor_owner_generation,NEW.predecessor_owner_nonce);
 RETURN NULL;
END;
$$;
CREATE TRIGGER repository_superseded_reservation AFTER INSERT ON repository_coordinator_supersessions
 FOR EACH ROW EXECUTE FUNCTION publish_repository_superseded_reservation();

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
  AND (kind='GRACEFUL' OR (kind IN ('EXPIRED_UNQUIESCED','SUPERSEDED_UNACTIVATED')
   AND predecessor_owner_generation=NEW.predecessor_generation AND predecessor_owner_nonce=NEW.predecessor_nonce))) THEN
  RAISE EXCEPTION 'Install differs from reserved handoff'; END IF;
 IF EXISTS(SELECT 1 FROM repository_coordinator_supersessions s WHERE s.account_id=NEW.account_id AND s.principal=NEW.principal
  AND s.operation_id=NEW.operation_id AND s.predecessor_epoch=NEW.predecessor_epoch
  AND s.preparation_sha256 IS DISTINCT FROM NEW.predecessor_preparation_sha256) THEN
  RAISE EXCEPTION 'Install differs from superseded preparation'; END IF;
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

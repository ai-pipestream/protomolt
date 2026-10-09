-- Common immutable reservation identity. Graceful evidence stays in V92 and still requires V91.
-- LIKE copies shape/checks/indexes, not the V92 foreign key to local drains.
CREATE TABLE repository_coordinator_reservations
 (LIKE repository_coordinator_handoffs INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
ALTER TABLE repository_coordinator_reservations
 ADD COLUMN kind text NOT NULL DEFAULT 'GRACEFUL' CHECK(kind='GRACEFUL'),
 ADD COLUMN predecessor_remote_state text NOT NULL DEFAULT 'UNKNOWN' CHECK(predecessor_remote_state='UNKNOWN');

-- Existing graceful transfers remain valid, including expired and activated successors.
INSERT INTO repository_coordinator_reservations SELECT h.*, 'GRACEFUL', 'UNKNOWN'
 FROM repository_coordinator_handoffs h;

CREATE FUNCTION protect_repository_coordinator_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator reservation is immutable'; END IF;
 IF NEW.kind<>'GRACEFUL' OR NEW.predecessor_remote_state<>'UNKNOWN'
 OR NOT EXISTS(SELECT 1 FROM repository_coordinator_handoffs h
  WHERE (h.account_id,h.principal,h.operation_id,h.predecessor_epoch,h.predecessor_token,
   h.predecessor_incarnation,h.command_sha256,h.successor_epoch,h.successor_token,
   h.successor_incarnation,h.lease_millis,h.recorded_at)
  IS NOT DISTINCT FROM
  (NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,
   NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,
   NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at)) THEN
  RAISE EXCEPTION 'Coordinator reservation requires exact source evidence';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_reservation_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_reservations FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_reservation();

CREATE FUNCTION publish_repository_graceful_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO repository_coordinator_reservations
 (account_id,principal,operation_id,predecessor_epoch,predecessor_token,predecessor_incarnation,
  command_sha256,successor_epoch,successor_token,successor_incarnation,lease_millis,recorded_at,
  kind,predecessor_remote_state)
 VALUES(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_epoch,NEW.predecessor_token,
  NEW.predecessor_incarnation,NEW.command_sha256,NEW.successor_epoch,NEW.successor_token,
  NEW.successor_incarnation,NEW.lease_millis,NEW.recorded_at,'GRACEFUL','UNKNOWN');
 RETURN NULL;
END;
$$;
CREATE TRIGGER repository_graceful_reservation AFTER INSERT ON repository_coordinator_handoffs
 FOR EACH ROW EXECUTE FUNCTION publish_repository_graceful_reservation();

DO $$
DECLARE old_constraint text;
BEGIN
 SELECT conname INTO STRICT old_constraint FROM pg_constraint
 WHERE conrelid='repository_successor_installs'::regclass
  AND confrelid='repository_coordinator_handoffs'::regclass AND contype='f';
 EXECUTE format('ALTER TABLE repository_successor_installs DROP CONSTRAINT %I',old_constraint);
END;
$$;
ALTER TABLE repository_successor_installs ADD CONSTRAINT repository_successor_install_reservation
 FOREIGN KEY(account_id,principal,operation_id,predecessor_epoch)
 REFERENCES repository_coordinator_reservations(account_id,principal,operation_id,predecessor_epoch);

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
  AND successor_token=NEW.successor_token AND successor_incarnation=NEW.successor_incarnation AND command_sha256=NEW.command_sha256) THEN
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

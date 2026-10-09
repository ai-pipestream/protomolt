-- Exact registration transaction only; no execution or provider grant.
CREATE TABLE repository_successor_installs (
 account_id varchar(200) NOT NULL, principal varchar(200) NOT NULL, operation_id uuid NOT NULL,
 predecessor_epoch bigint NOT NULL, successor_epoch bigint NOT NULL,
 successor_token uuid NOT NULL, successor_incarnation uuid NOT NULL,
 predecessor_generation bigint NOT NULL CHECK(predecessor_generation BETWEEN 1 AND 9223372036854775806),
 predecessor_nonce uuid NOT NULL, predecessor_preparation_sha256 bytea NOT NULL CHECK(octet_length(predecessor_preparation_sha256)=32),
 owner_nonce uuid NOT NULL CHECK(owner_nonce<>predecessor_nonce),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 modes_sha256 bytea NOT NULL CHECK(octet_length(modes_sha256)=32),
 install_xid xid8 NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,successor_epoch),
 UNIQUE(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_epoch)
 REFERENCES repository_coordinator_handoffs(account_id,principal,operation_id,predecessor_epoch)
);
CREATE FUNCTION protect_repository_successor_install() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE claim repository_execution_claims%ROWTYPE; owner repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Successor install is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO claim FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR (claim.claim_epoch,claim.claim_token,claim.command_sha256)
  IS DISTINCT FROM (NEW.successor_epoch,NEW.successor_token,NEW.command_sha256) OR claim.lease_until<=clock_timestamp() THEN
  RAISE EXCEPTION 'Install requires exact live successor'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_coordinator_handoffs WHERE account_id=NEW.account_id AND principal=NEW.principal
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
CREATE TRIGGER repository_successor_install_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_successor_installs
 FOR EACH ROW EXECUTE FUNCTION protect_repository_successor_install();

CREATE FUNCTION repository_successor_registration(a text,p text,o uuid,g bigint,n uuid)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT EXISTS(SELECT 1 FROM repository_successor_installs i JOIN repository_execution_claims c
 USING(account_id,principal,operation_id) WHERE i.account_id=a AND i.principal=p AND i.operation_id=o
 AND i.predecessor_generation=g AND i.owner_nonce=n AND i.install_xid=pg_current_xact_id_if_assigned()
 AND c.claim_epoch=i.successor_epoch AND c.claim_token=i.successor_token AND c.lease_until>clock_timestamp());
$$;
CREATE FUNCTION repository_successor_preparation(a text,p text,o uuid,g bigint,n uuid,d bytea,s bytea)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT repository_successor_registration(a,p,o,g,n) AND EXISTS(SELECT 1 FROM repository_successor_installs
 WHERE account_id=a AND principal=p AND operation_id=o AND predecessor_generation=g AND owner_nonce=n
 AND command_sha256=d AND preparation_sha256=s);
$$;
CREATE FUNCTION repository_successor_modes(a text,p text,o uuid,g bigint,n uuid,m jsonb)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT repository_successor_registration(a,p,o,g,n) AND EXISTS(SELECT 1 FROM repository_successor_installs
 WHERE account_id=a AND principal=p AND operation_id=o AND predecessor_generation=g AND owner_nonce=n
 AND modes_sha256=sha256(convert_to(m::text,'UTF8')));
$$;
CREATE FUNCTION repository_successor_owner(old_owner repository_operation_owners,new_owner repository_operation_owners)
RETURNS boolean LANGUAGE sql VOLATILE AS $$
 SELECT new_owner.owner_generation=old_owner.owner_generation+1
 AND (new_owner.account_id,new_owner.principal,new_owner.operation_id)=(old_owner.account_id,old_owner.principal,old_owner.operation_id)
 AND repository_successor_registration(new_owner.account_id,new_owner.principal,new_owner.operation_id,old_owner.owner_generation,new_owner.owner_token)
 AND EXISTS(SELECT 1 FROM repository_successor_installs WHERE account_id=old_owner.account_id AND principal=old_owner.principal
 AND operation_id=old_owner.operation_id AND predecessor_generation=old_owner.owner_generation AND predecessor_nonce=old_owner.owner_token);
$$;
CREATE FUNCTION require_repository_successor_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT repository_successor_registration(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce) THEN
 RAISE EXCEPTION 'Successor claim expired before install commit'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations p JOIN repository_publication_modes m
 USING(account_id,principal,operation_id,predecessor_generation) JOIN repository_operation_owners o
 USING(account_id,principal,operation_id)
 WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id
 AND p.predecessor_generation=NEW.predecessor_generation AND p.owner_nonce=NEW.owner_nonce
 AND p.preparation_sha256=NEW.preparation_sha256 AND p.command_sha256=NEW.command_sha256
 AND m.owner_nonce=NEW.owner_nonce AND sha256(convert_to(m.modes::text,'UTF8'))=NEW.modes_sha256
 AND o.owner_generation=NEW.predecessor_generation+1 AND o.owner_token=NEW.owner_nonce
 AND o.lease_until>clock_timestamp()) THEN
 RAISE EXCEPTION 'Successor install must atomically complete preparation modes and owner'; END IF;
 RETURN NULL;
END; $$;
CREATE CONSTRAINT TRIGGER repository_successor_complete AFTER INSERT ON repository_successor_installs
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_successor_complete();

CREATE OR REPLACE FUNCTION protect_repository_publication_preparation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE existing repository_publication_preparations%ROWTYPE; predecessor repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Publication preparation is immutable'; END IF;
 IF NOT repository_successor_preparation(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce,NEW.command_sha256,NEW.preparation_sha256) THEN PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id); END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND command_sha256=NEW.command_sha256) THEN
  RAISE EXCEPTION 'Publication preparation differs from claimed command';
 END IF;
 SELECT * INTO existing FROM repository_publication_preparations WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 IF FOUND THEN
  IF ROW(existing.owner_nonce,existing.command_codec,existing.command_version,existing.command_bytes,existing.command_sha256,
          existing.preparation_bytes,existing.preparation_sha256)
   IS DISTINCT FROM ROW(NEW.owner_nonce,NEW.command_codec,NEW.command_version,NEW.command_bytes,NEW.command_sha256,
          NEW.preparation_bytes,NEW.preparation_sha256) THEN
   RAISE EXCEPTION 'Publication preparation identity conflicts with saved record';
  END IF;
  RETURN NEW; -- Exact retry after admission or a lost acknowledgment; no replacement.
 END IF;
 IF NEW.predecessor_generation=0 THEN
  IF EXISTS(SELECT 1 FROM repository_operations WHERE account_id=NEW.account_id AND principal=NEW.principal
   AND operation_id=NEW.operation_id) THEN
   RAISE EXCEPTION 'Initial preparation must precede operation admission';
  END IF;
 ELSE
  SELECT * INTO predecessor FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal
   AND operation_id=NEW.operation_id FOR UPDATE;
  IF NOT FOUND OR predecessor.owner_generation<>NEW.predecessor_generation OR predecessor.owner_token=NEW.owner_nonce
   OR predecessor.lease_until>clock_timestamp() THEN
   RAISE EXCEPTION 'Recovery preparation requires exact expired predecessor';
  END IF;
  IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
   OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
   RAISE EXCEPTION 'Terminal operation cannot prepare a new owner';
  END IF;
 END IF;
 -- Recheck the execution lease after any predecessor row lock wait.
 IF NOT repository_successor_preparation(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce,NEW.command_sha256,NEW.preparation_sha256) THEN PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id); END IF;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION protect_repository_publication_modes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE preparation repository_publication_preparations%ROWTYPE; existing repository_publication_modes%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Publication modes are immutable'; END IF;
 IF NOT repository_successor_modes(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce,NEW.modes) THEN PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id); END IF;
 SELECT * INTO preparation FROM repository_publication_preparations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF NOT FOUND OR preparation.owner_nonce<>NEW.owner_nonce THEN RAISE EXCEPTION 'Publication modes require exact preparation'; END IF;
 IF jsonb_typeof(NEW.modes)<>'object' THEN RAISE EXCEPTION 'Publication modes require object'; END IF;
 IF (SELECT count(*) FROM jsonb_each(NEW.modes)) NOT BETWEEN 1 AND 10000
  OR EXISTS(SELECT 1 FROM jsonb_each(NEW.modes) entry WHERE length(entry.key)=0
    OR length(entry.key)>200 OR entry.value NOT IN ('"TYPED"'::jsonb,'"OPAQUE"'::jsonb)) THEN
  RAISE EXCEPTION 'Publication modes contain invalid entries';
 END IF;
 SELECT * INTO existing FROM repository_publication_modes WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 IF FOUND THEN
  IF existing.owner_nonce<>NEW.owner_nonce OR existing.modes<>NEW.modes THEN RAISE EXCEPTION 'Publication modes changed'; END IF;
 ELSE
  -- Choices must precede the owner generation that can execute them.
  IF EXISTS(SELECT 1 FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal
    AND operation_id=NEW.operation_id AND owner_generation>NEW.predecessor_generation) THEN
   RAISE EXCEPTION 'Publication modes must precede owner admission';
  END IF;
 END IF;
 IF NOT repository_successor_modes(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation,NEW.owner_nonce,NEW.modes) THEN PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id); END IF;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION guard_repository_journaled_owner() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE preparation repository_publication_preparations%ROWTYPE; modes repository_publication_modes%ROWTYPE;
BEGIN
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN RETURN NEW; END IF;
 SELECT * INTO preparation FROM repository_publication_preparations WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.owner_generation-1;
 IF NOT FOUND THEN RETURN NEW; END IF;
 IF NOT repository_successor_registration(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation-1,NEW.owner_token) THEN PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id); END IF;
 SELECT * INTO modes FROM repository_publication_modes WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.owner_generation-1;
 IF NOT FOUND THEN RAISE EXCEPTION 'Journaled owner requires fixed modes'; END IF;
 IF preparation.owner_nonce<>NEW.owner_token OR modes.owner_nonce<>NEW.owner_token THEN
  RAISE EXCEPTION 'Journaled owner differs from preparation';
 END IF;
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION fence_repository_owner_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Existing cleanup proof grants no publication write authority. Preserve it
 -- for expired/terminal operations without turning it into a live claim.
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND repository_successor_owner(OLD,NEW) THEN RETURN NEW; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION guard_repository_coordinator_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
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

-- Host-private immutable journal. This does not activate automatic session recovery.
CREATE TABLE repository_publication_preparations (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL CHECK(predecessor_generation>=0 AND predecessor_generation<9223372036854775807),
 owner_nonce uuid NOT NULL,
 command_codec text NOT NULL CHECK(command_codec='document-publication'),
 command_version integer NOT NULL CHECK(command_version=1),
 command_bytes bytea NOT NULL CHECK(octet_length(command_bytes) BETWEEN 1 AND 1048576),
 command_sha256 bytea NOT NULL CHECK(command_sha256=sha256(command_bytes)),
 preparation_bytes bytea NOT NULL CHECK(octet_length(preparation_bytes) BETWEEN 1 AND 16777216),
 preparation_sha256 bytea NOT NULL CONSTRAINT repository_preparation_digest CHECK(preparation_sha256=sha256(preparation_bytes)),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 UNIQUE(account_id,principal,operation_id,owner_nonce),
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_execution_claims(account_id,principal,operation_id)
);
CREATE FUNCTION protect_repository_publication_preparation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE existing repository_publication_preparations%ROWTYPE; predecessor repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Publication preparation is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
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
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_publication_preparation_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_publication_preparations
 FOR EACH ROW EXECUTE FUNCTION protect_repository_publication_preparation();

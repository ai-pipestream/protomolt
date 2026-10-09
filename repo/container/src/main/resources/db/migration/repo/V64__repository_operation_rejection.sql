-- Drain writers before rollout; existing successes remain unchanged.
LOCK TABLE repository_operation_owners, repository_operation_success IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE repository_operation_rejection (
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 command_codec VARCHAR(128) NOT NULL,
 command_version INTEGER NOT NULL,
 command_sha256 BYTEA NOT NULL CHECK(octet_length(command_sha256)=32),
 result_codec VARCHAR(128) NOT NULL CHECK(result_codec='document-publication-rejection'),
 result_version INTEGER NOT NULL CHECK(result_version=1),
 result_bytes BYTEA NOT NULL CHECK(octet_length(result_bytes) BETWEEN 1 AND 4096),
 result_sha256 BYTEA NOT NULL CHECK(result_sha256=sha256(result_bytes)),
 recorded_at_epoch_micros BIGINT NOT NULL CHECK(recorded_at_epoch_micros BETWEEN 1 AND 253402300799999999),
 disposition INTEGER NOT NULL CHECK(disposition IN (1,2)),
 reason INTEGER NOT NULL CHECK(reason IN (1,2,3)),
 creation_xid xid8 NOT NULL,
 CHECK((disposition=2)=(reason=3)),
 PRIMARY KEY(account_id,principal,operation_id),
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_operations
);

CREATE OR REPLACE FUNCTION require_repository_operation_write_fence(
 scoped_account text, scoped_principal text, scoped_operation uuid, scoped_generation bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=scoped_account
  AND principal=scoped_principal AND operation_id=scoped_operation)
 OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=scoped_account
  AND principal=scoped_principal AND operation_id=scoped_operation) THEN
  RAISE EXCEPTION 'Repository operation is terminal';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_operation_owners
  WHERE account_id=scoped_account AND principal=scoped_principal AND operation_id=scoped_operation
   AND owner_generation=scoped_generation AND write_fence_xid=pg_current_xact_id_if_assigned()
   AND lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository mutation requires a live owner write fence in this transaction';
 END IF;
 RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION reject_terminal_repository_owner_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Cleanup's recovery fence grants no write authority and must remain available.
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN RETURN NEW; END IF;
 IF EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id)
 OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Repository operation is terminal';
 END IF;
 RETURN NEW;
END;
$$;

CREATE FUNCTION guard_repository_operation_rejection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE command repository_operations%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Repository operation rejection is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT command FROM repository_operations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF ROW(NEW.command_codec,NEW.command_version,NEW.command_sha256)
  IS DISTINCT FROM ROW(command.command_codec,command.command_version,command.command_sha256)
  OR command.command_codec<>'document-publication' OR command.command_version<>1 THEN
  RAISE EXCEPTION 'Repository rejection differs from admitted command identity';
 END IF;
 IF EXISTS(SELECT 1 FROM document_revision_commits WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Repository rejection cannot accompany publication';
 END IF;
 IF NEW.recorded_at_epoch_micros < floor(extract(epoch FROM transaction_timestamp())*1000000)
 OR NEW.recorded_at_epoch_micros > floor(extract(epoch FROM clock_timestamp())*1000000) THEN
  RAISE EXCEPTION 'Repository rejection time must belong to its decision transaction';
 END IF;
 NEW.creation_xid := pg_current_xact_id();
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_operation_rejection_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_operation_rejection
 FOR EACH ROW EXECUTE FUNCTION guard_repository_operation_rejection();

-- First-admission arbitration stays with V79's unique scope insertion, without locking existing scopes.
ALTER TABLE repository_execution_scopes ADD COLUMN scope_created_xid xid8;
CREATE FUNCTION stamp_repository_execution_scope_creation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.scope_created_xid:=pg_current_xact_id(); RETURN NEW; END;
$$;
CREATE TRIGGER repository_execution_scope_creation BEFORE INSERT ON repository_execution_scopes
 FOR EACH ROW EXECUTE FUNCTION stamp_repository_execution_scope_creation();

CREATE TABLE repository_creation_grants (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 issuer varchar(128) NOT NULL,
 credential_id uuid NOT NULL,
 credential_generation bigint NOT NULL CHECK(credential_generation>0),
 command_codec text NOT NULL CHECK(command_codec='document-publication'),
 command_version int NOT NULL CHECK(command_version=1),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 placement_version int NOT NULL CHECK(placement_version=1),
 placement_sha256 bytea NOT NULL CHECK(octet_length(placement_sha256)=32),
 expires_at_epoch_micros bigint NOT NULL CHECK(expires_at_epoch_micros>0),
 revoked boolean NOT NULL DEFAULT false,
 PRIMARY KEY(account_id,principal,operation_id),
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_execution_scopes,
 FOREIGN KEY(issuer,credential_id) REFERENCES repository_credential_authorities
);
CREATE FUNCTION protect_repository_creation_grant() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE scope repository_execution_scopes%ROWTYPE;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Creation grant tombstones cannot be deleted'; END IF;
 IF TG_OP='INSERT' THEN
  SELECT * INTO scope FROM repository_execution_scopes
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
  IF NOT FOUND OR NOT scope.claim_required OR scope.scope_created_xid IS DISTINCT FROM pg_current_xact_id_if_assigned()
   OR EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
   OR EXISTS(SELECT 1 FROM repository_operations WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
   RAISE EXCEPTION 'Creation grant requires a new unadmitted scope in this transaction';
  END IF;
  PERFORM 1 FROM repository_credential_authorities WHERE issuer=NEW.issuer AND credential_id=NEW.credential_id
   AND generation=NEW.credential_generation AND principal=NEW.principal AND NOT revoked FOR SHARE;
  IF NOT FOUND OR NEW.revoked OR NEW.expires_at_epoch_micros<=floor(extract(epoch FROM clock_timestamp())*1000000)::bigint THEN
   RAISE EXCEPTION 'Creation grant requires live credential authority and future expiry';
  END IF;
 ELSIF (to_jsonb(NEW)-'revoked') IS DISTINCT FROM (to_jsonb(OLD)-'revoked') OR (OLD.revoked AND NOT NEW.revoked) THEN
  RAISE EXCEPTION 'Creation grant identity is immutable and revocation is terminal';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_creation_grant_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_creation_grants
 FOR EACH ROW EXECUTE FUNCTION protect_repository_creation_grant();

CREATE FUNCTION lock_repository_creation_grant(a text,p text,o uuid)
RETURNS TABLE(issuer varchar,credential_id uuid,credential_generation bigint,command_sha256 bytea,
 placement_sha256 bytea,expires_at_epoch_micros bigint,live boolean) LANGUAGE plpgsql AS $$
DECLARE g repository_creation_grants%ROWTYPE;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT * INTO g FROM repository_creation_grants r WHERE r.account_id=a AND r.principal=p AND r.operation_id=o FOR SHARE;
 IF NOT FOUND THEN RETURN; END IF;
 -- Evaluate the clock after any lock wait. Grant identity is immutable.
 RETURN QUERY SELECT g.issuer,g.credential_id,g.credential_generation,g.command_sha256,g.placement_sha256,
  g.expires_at_epoch_micros,NOT g.revoked AND g.expires_at_epoch_micros>floor(extract(epoch FROM clock_timestamp())*1000000)::bigint;
END;
$$;

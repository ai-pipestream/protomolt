-- Internal staging only. No revision bindings, publication or cleanup are enabled.
CREATE TABLE repository_schema_artifacts (
 account_id VARCHAR(200) NOT NULL CHECK(btrim(account_id)<>''),
 artifact_sha256 BYTEA NOT NULL CHECK(octet_length(artifact_sha256)=32),
 artifact_bytes BYTEA NOT NULL,
 size_bytes INTEGER GENERATED ALWAYS AS (octet_length(artifact_bytes)) STORED
   CHECK(size_bytes BETWEEN 1 AND 16777216),
 creator_principal VARCHAR(200) NOT NULL,
 creator_operation UUID NOT NULL,
 creator_generation BIGINT NOT NULL CHECK(creator_generation>0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,artifact_sha256),
 CHECK(artifact_sha256=sha256(artifact_bytes)),
 FOREIGN KEY(account_id,creator_principal,creator_operation)
   REFERENCES repository_operations(account_id,principal,operation_id)
);
CREATE TABLE repository_schema_artifact_claims (
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 artifact_sha256 BYTEA NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,owner_generation,artifact_sha256),
 FOREIGN KEY(account_id,principal,operation_id)
   REFERENCES repository_operations(account_id,principal,operation_id),
 FOREIGN KEY(account_id,artifact_sha256)
   REFERENCES repository_schema_artifacts(account_id,artifact_sha256)
);
CREATE INDEX repository_schema_claim_artifact
 ON repository_schema_artifact_claims(account_id,artifact_sha256);

CREATE FUNCTION guard_repository_schema_artifact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'Schema artifact mutation requires the future retention cleanup protocol';
 END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.creator_principal,
   NEW.creator_operation,NEW.creator_generation);
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_schema_artifact_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_schema_artifacts
 FOR EACH ROW EXECUTE FUNCTION guard_repository_schema_artifact();

CREATE FUNCTION guard_repository_schema_claim() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE existing_count BIGINT; existing_bytes BIGINT; incoming_bytes INTEGER;
BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'Schema claim mutation requires the future retention cleanup protocol';
 END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT size_bytes INTO STRICT incoming_bytes FROM repository_schema_artifacts
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF EXISTS(SELECT 1 FROM repository_schema_artifact_claims
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
    AND owner_generation=NEW.owner_generation AND artifact_sha256=NEW.artifact_sha256) THEN RETURN NEW; END IF;
 SELECT count(*),COALESCE(sum(a.size_bytes),0) INTO existing_count,existing_bytes
 FROM repository_schema_artifact_claims c JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
 WHERE c.account_id=NEW.account_id AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id
  AND c.owner_generation=NEW.owner_generation;
 IF existing_count>=64 OR existing_bytes+incoming_bytes>67108864 THEN
  RAISE EXCEPTION 'Schema staging exceeds operation generation limits';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_schema_claim_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_schema_artifact_claims
 FOR EACH ROW EXECUTE FUNCTION guard_repository_schema_claim();

CREATE FUNCTION require_repository_schema_initial_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_schema_artifact_claims
   WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256
    AND principal=NEW.creator_principal AND operation_id=NEW.creator_operation
    AND owner_generation=NEW.creator_generation) THEN
  RAISE EXCEPTION 'New schema artifact requires an atomic staging claim';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_schema_initial_claim AFTER INSERT ON repository_schema_artifacts
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_schema_initial_claim();

CREATE FUNCTION stage_repository_schema_artifact(
 p_account TEXT,p_principal TEXT,p_operation UUID,p_generation BIGINT,p_sha BYTEA,p_bytes BYTEA)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE exact_match BOOLEAN;
BEGIN
 PERFORM require_repository_operation_write_fence(p_account,p_principal,p_operation,p_generation);
 IF p_bytes IS NULL OR octet_length(p_bytes) NOT BETWEEN 1 AND 16777216
   OR p_sha IS NULL OR p_sha<>sha256(p_bytes) THEN
  RAISE EXCEPTION 'Invalid schema artifact size or digest';
 END IF;
 INSERT INTO repository_schema_artifacts(account_id,artifact_sha256,artifact_bytes,
   creator_principal,creator_operation,creator_generation)
 VALUES(p_account,p_sha,p_bytes,p_principal,p_operation,p_generation)
 ON CONFLICT(account_id,artifact_sha256) DO NOTHING;
 SELECT artifact_bytes=p_bytes INTO STRICT exact_match FROM repository_schema_artifacts
  WHERE account_id=p_account AND artifact_sha256=p_sha FOR KEY SHARE;
 IF NOT exact_match THEN RAISE EXCEPTION 'Schema artifact digest collision'; END IF;
 INSERT INTO repository_schema_artifact_claims(account_id,principal,operation_id,owner_generation,artifact_sha256)
 VALUES(p_account,p_principal,p_operation,p_generation,p_sha)
 ON CONFLICT(account_id,principal,operation_id,owner_generation,artifact_sha256) DO NOTHING;
 PERFORM require_repository_operation_write_fence(p_account,p_principal,p_operation,p_generation);
END;
$$;

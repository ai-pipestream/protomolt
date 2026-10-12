-- Schema payload separation: descriptor bytes move from the identity catalog to a
-- normalized payload relation keyed by the existing identity. Catalog identities,
-- claims and V55/V56/V57 associations are preserved. No reclamation, pruning or
-- delete permission is enabled: payload rows are immutable once staged.
LOCK TABLE repository_schema_artifacts, repository_schema_artifact_claims IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE repository_schema_artifact_payloads (
 account_id VARCHAR(200) NOT NULL,
 artifact_sha256 BYTEA NOT NULL CHECK(octet_length(artifact_sha256)=32),
 payload_bytes BYTEA NOT NULL,
 size_bytes INTEGER GENERATED ALWAYS AS (octet_length(payload_bytes)) STORED
   CHECK(size_bytes BETWEEN 1 AND 16777216),
 PRIMARY KEY(account_id,artifact_sha256),
 CHECK(artifact_sha256=sha256(payload_bytes)),
 FOREIGN KEY(account_id,artifact_sha256)
  REFERENCES repository_schema_artifacts(account_id,artifact_sha256)
);

-- Backfill from the catalog. The row CHECKs verify digest and size bounds for
-- every migrated payload; the count check below proves none was missed.
INSERT INTO repository_schema_artifact_payloads(account_id,artifact_sha256,payload_bytes)
 SELECT account_id,artifact_sha256,artifact_bytes FROM repository_schema_artifacts;

DO $$ DECLARE catalog BIGINT; payload BIGINT; BEGIN
 SELECT count(*) INTO catalog FROM repository_schema_artifacts;
 SELECT count(*) INTO payload FROM repository_schema_artifact_payloads;
 IF catalog<>payload THEN RAISE EXCEPTION 'Schema payload backfill is incomplete'; END IF;
 IF EXISTS(SELECT 1 FROM repository_schema_artifacts c LEFT JOIN repository_schema_artifact_payloads p
  USING(account_id,artifact_sha256) WHERE p.artifact_sha256 IS NULL) THEN
  RAISE EXCEPTION 'Schema payload backfill missed a catalog identity';
 END IF;
END $$;

-- Remove the redundant catalog payload storage only after that verification.
DO $$ DECLARE c text; BEGIN
 SELECT conname INTO c FROM pg_constraint
  WHERE conrelid='repository_schema_artifacts'::regclass AND contype='c'
   AND pg_get_constraintdef(oid) LIKE '%sha256(artifact_bytes)%';
 IF c IS NULL THEN RAISE EXCEPTION 'Catalog digest check is missing'; END IF;
 EXECUTE format('ALTER TABLE repository_schema_artifacts DROP CONSTRAINT %I', c);
END $$;
ALTER TABLE repository_schema_artifacts DROP COLUMN size_bytes;
ALTER TABLE repository_schema_artifacts DROP COLUMN artifact_bytes;

CREATE FUNCTION guard_repository_schema_artifact_payload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'Schema payload mutation requires the future retention cleanup protocol';
 END IF;
 -- Staging inserts catalog, payload and claim in one fenced transaction; the
 -- deferred catalog trigger requires the payload at commit. Pin the identity.
 PERFORM 1 FROM repository_schema_artifacts
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Schema payload requires its catalog identity'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_schema_artifact_payload_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_schema_artifact_payloads
 FOR EACH ROW EXECUTE FUNCTION guard_repository_schema_artifact_payload();

CREATE FUNCTION require_repository_schema_payload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_schema_artifact_payloads
   WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256) THEN
  RAISE EXCEPTION 'New schema artifact requires its payload bytes';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_schema_payload_required AFTER INSERT ON repository_schema_artifacts
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_schema_payload();

-- New behavior in this migration: the payload row is written only by the call
-- that creates the catalog identity. Staging onto an existing identity requires
-- and verifies its retained payload; it never recreates lost bytes.
CREATE OR REPLACE FUNCTION stage_repository_schema_artifact(
 p_account TEXT,p_principal TEXT,p_operation UUID,p_generation BIGINT,p_sha BYTEA,p_bytes BYTEA)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE created_identity BOOLEAN; retained_bytes BYTEA;
BEGIN
 PERFORM require_repository_operation_write_fence(p_account,p_principal,p_operation,p_generation);
 IF p_bytes IS NULL OR octet_length(p_bytes) NOT BETWEEN 1 AND 16777216
   OR p_sha IS NULL OR p_sha<>sha256(p_bytes) THEN
  RAISE EXCEPTION 'Invalid schema artifact size or digest';
 END IF;
 INSERT INTO repository_schema_artifacts(account_id,artifact_sha256,
   creator_principal,creator_operation,creator_generation)
 VALUES(p_account,p_sha,p_principal,p_operation,p_generation)
 ON CONFLICT(account_id,artifact_sha256) DO NOTHING
 RETURNING TRUE INTO created_identity;
 IF created_identity IS TRUE THEN
  INSERT INTO repository_schema_artifact_payloads(account_id,artifact_sha256,payload_bytes)
  VALUES(p_account,p_sha,p_bytes);
 ELSE
  SELECT payload_bytes INTO retained_bytes FROM repository_schema_artifact_payloads
   WHERE account_id=p_account AND artifact_sha256=p_sha FOR KEY SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'Schema artifact payload is missing'; END IF;
  IF retained_bytes<>p_bytes THEN RAISE EXCEPTION 'Schema artifact digest collision'; END IF;
 END IF;
 INSERT INTO repository_schema_artifact_claims(account_id,principal,operation_id,owner_generation,artifact_sha256)
 VALUES(p_account,p_principal,p_operation,p_generation,p_sha)
 ON CONFLICT(account_id,principal,operation_id,owner_generation,artifact_sha256) DO NOTHING;
 PERFORM require_repository_operation_write_fence(p_account,p_principal,p_operation,p_generation);
END;
$$;

-- Retargeted in this migration: staging byte accounting reads the payload
-- relation. Limits and lock order are unchanged: identity rows first, then
-- claims, in caller digest order.
CREATE OR REPLACE FUNCTION guard_repository_schema_claim() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE existing_count BIGINT; existing_bytes BIGINT; incoming_bytes INTEGER;
BEGIN
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(OLD.account_id,OLD.principal,OLD.operation_id);
  IF NOT EXISTS(SELECT 1 FROM repository_operation_owners o
   JOIN repository_schema_artifact_claims c
    ON c.account_id=o.account_id AND c.principal=o.principal AND c.operation_id=o.operation_id
     AND c.owner_generation=o.owner_generation AND c.artifact_sha256=OLD.artifact_sha256
   WHERE o.account_id=OLD.account_id AND o.principal=OLD.principal AND o.operation_id=OLD.operation_id
    AND o.owner_generation>OLD.owner_generation) THEN
   RAISE EXCEPTION 'Schema claim release requires a current-generation replacement claim';
  END IF;
  RETURN OLD;
 END IF;
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'Schema claim mutation requires the future retention cleanup protocol';
 END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT size_bytes INTO incoming_bytes FROM repository_schema_artifact_payloads
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Schema artifact payload is missing'; END IF;
 IF EXISTS(SELECT 1 FROM repository_schema_artifact_claims
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
    AND owner_generation=NEW.owner_generation AND artifact_sha256=NEW.artifact_sha256) THEN RETURN NEW; END IF;
 SELECT count(*),COALESCE(sum(a.size_bytes),0) INTO existing_count,existing_bytes
 FROM repository_schema_artifact_claims c JOIN repository_schema_artifact_payloads a USING(account_id,artifact_sha256)
 WHERE c.account_id=NEW.account_id AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id
  AND c.owner_generation=NEW.owner_generation;
 IF existing_count>=64 OR existing_bytes+incoming_bytes>67108864 THEN
  RAISE EXCEPTION 'Schema staging exceeds operation generation limits';
 END IF;
 RETURN NEW;
END;
$$;

-- Retargeted in this migration: the metadata existence and size check reads
-- the payload relation. Bounds and binding rules are unchanged.
CREATE OR REPLACE FUNCTION guard_document_revision_schema_asset() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c document_revision_commits%ROWTYPE;
 r document_revision_publications%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Revision schema asset bindings are immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.revision_id;
 IF c.account_id<>NEW.account_id OR c.principal<>NEW.principal OR c.operation_id<>NEW.operation_id
  OR c.owner_generation<>NEW.owner_generation OR c.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema asset differs from native commit owner or transaction';
 END IF;
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=NEW.revision_id FOR UPDATE;
 IF r.native_binding IS DISTINCT FROM NEW.revision_id OR r.projection_sealed
  OR r.projection_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema asset requires the current unsealed native projection';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_schema_artifact_payloads
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.metadata_sha256
   AND size_bytes BETWEEN 1 AND 524288) THEN
  RAISE EXCEPTION 'Revision schema metadata is missing or exceeds byte bound';
 END IF;
 IF (SELECT count(*) FROM document_revision_schema_assets WHERE revision_id=NEW.revision_id)>=64 THEN
  RAISE EXCEPTION 'Revision schema asset binding count exceeds bound';
 END IF;
 RETURN NEW;
END;
$$;

-- Retargeted in this migration: the completeness byte sum reads the payload
-- relation. Expected counts and the byte budget are unchanged.
CREATE OR REPLACE FUNCTION require_complete_document_assessment_artifacts() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected INTEGER; actual BIGINT; bytes BIGINT;
BEGIN
 SELECT expected_artifacts INTO expected FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT count(*),COALESCE(sum(a.size_bytes::bigint),0) INTO actual,bytes
 FROM document_assessment_artifacts r JOIN repository_schema_artifact_payloads a USING(account_id,artifact_sha256)
 WHERE r.assessment_id=NEW.assessment_id;
 IF actual<>expected OR bytes>67108864 THEN
  RAISE EXCEPTION 'Assessment artifact set is incomplete or exceeds byte budget';
 END IF;
 RETURN NULL;
END;
$$;

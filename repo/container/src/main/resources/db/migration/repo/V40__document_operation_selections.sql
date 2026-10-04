-- Initial selection only. Retry replacement and terminal publication are separate
-- boundaries; existing attempts are not adopted by guessing which one was chosen.
CREATE TABLE document_operation_selections (
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 member_id VARCHAR(128) NOT NULL CHECK(member_id ~ '^[a-zA-Z0-9_.-]+$'),
 node_id UUID NOT NULL,
 sampled_revision BIGINT NOT NULL CHECK(sampled_revision>=0),
 drive_id UUID NOT NULL,
 drive_snapshot JSONB NOT NULL CHECK(octet_length(drive_snapshot::text)<=16384),
 drive_sha256 BYTEA NOT NULL CHECK(octet_length(drive_sha256)=32),
 drive_fence_version INTEGER NOT NULL DEFAULT 1 CHECK(drive_fence_version=1),
 backend_generation VARCHAR(128) NOT NULL REFERENCES managed_backend_profiles(generation),
 storage_realm VARCHAR(128) NOT NULL,
 storage_namespace TEXT NOT NULL CHECK(btrim(storage_namespace)<>'' AND octet_length(storage_namespace)<=4096),
 upload_count INTEGER NOT NULL CHECK(upload_count BETWEEN 0 AND 10000),
 attempt_id UUID UNIQUE REFERENCES document_part_attempts(attempt_id),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,owner_generation,member_id),
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_operations(account_id,principal,operation_id),
 CHECK((upload_count=0)=(attempt_id IS NULL))
);

-- Do not duplicate free-form metadata, provider options or credential references.
-- The digest still fences changes to every sampled configuration field.
CREATE FUNCTION document_operation_drive_snapshot(d drives) RETURNS JSONB LANGUAGE sql IMMUTABLE AS $$
 SELECT jsonb_build_object('drive_id',d.drive_id,'account_id',d.account_id,'name',d.name,
  'drive_type',d.drive_type,'provider',d.provider,'namespace',d.bucket,'prefix',d.prefix,
  'region',d.region,'status',d.status)
$$;

-- Version the digest recipe explicitly. Adding a drive column must not silently
-- change how retained version-1 fences are compared during future recovery.
CREATE FUNCTION document_operation_drive_digest_v1(d drives) RETURNS BYTEA LANGUAGE sql IMMUTABLE AS $$
 SELECT sha256(convert_to(jsonb_build_object('drive_id',d.drive_id,'account_id',d.account_id,'name',d.name,
  'drive_type',d.drive_type,'provider',d.provider,'bucket',d.bucket,'prefix',d.prefix,'region',d.region,
  'credentials_ref',d.credentials_ref,'status',d.status,'metadata',d.metadata,'provider_config',d.provider_config)::text,'UTF8'))
$$;

CREATE FUNCTION guard_document_operation_selection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a document_part_attempts%ROWTYPE; d drives%ROWTYPE; profile managed_backend_profiles%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Initial document operation selection is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT d FROM drives WHERE drive_id=NEW.drive_id;
 SELECT * INTO STRICT profile FROM managed_backend_profiles WHERE generation=NEW.backend_generation;
 IF d.account_id<>NEW.account_id OR d.status<>'ACTIVE' OR d.provider<>profile.provider
  OR NEW.storage_realm<>profile.storage_realm OR NEW.storage_namespace<>d.bucket
  OR NEW.drive_snapshot IS DISTINCT FROM document_operation_drive_snapshot(d)
  OR NEW.drive_sha256 IS DISTINCT FROM document_operation_drive_digest_v1(d) THEN
  RAISE EXCEPTION 'Document operation selection differs from sampled placement';
 END IF;
 IF NEW.attempt_id IS NOT NULL THEN
  SELECT * INTO STRICT a FROM document_part_attempts WHERE attempt_id=NEW.attempt_id;
  IF a.plan_kind<>'NEW_CONTENT' OR a.state<>'STAGING' OR a.lease_until<=clock_timestamp()
   OR ROW(a.account_id,a.operation_principal,a.operation_id,a.operation_generation,a.member_id,a.node_id,
          a.sampled_revision,a.drive_id,a.backend_generation,a.storage_realm,a.storage_namespace,a.planned_count)
    IS DISTINCT FROM ROW(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,NEW.member_id,NEW.node_id,
          NEW.sampled_revision,NEW.drive_id,NEW.backend_generation,NEW.storage_realm,NEW.storage_namespace,NEW.upload_count)
   OR EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=NEW.attempt_id) THEN
   RAISE EXCEPTION 'Document operation selection requires its exact live new-content attempt';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_operation_selection_guard BEFORE INSERT OR UPDATE OR DELETE ON document_operation_selections
 FOR EACH ROW EXECUTE FUNCTION guard_document_operation_selection();

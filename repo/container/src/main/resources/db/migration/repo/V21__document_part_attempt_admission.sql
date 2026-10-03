-- Admission only: no existing document is adopted and no cleanup worker is enabled.
CREATE TABLE document_part_attempts (
    attempt_id UUID PRIMARY KEY,
    node_id UUID NOT NULL,
    account_id VARCHAR(200) NOT NULL CHECK (btrim(account_id) <> '' AND position('/' in account_id)=0),
    sampled_revision BIGINT NOT NULL CHECK (sampled_revision >= 0),
    backend_generation VARCHAR(200) NOT NULL REFERENCES managed_backend_profiles(generation),
    storage_realm VARCHAR(200) NOT NULL CHECK (btrim(storage_realm) <> ''),
    storage_namespace VARCHAR(1024) NOT NULL CHECK (btrim(storage_namespace) <> ''),
    planned_count INTEGER NOT NULL CHECK (planned_count BETWEEN 1 AND 10000),
    source_count INTEGER NOT NULL CHECK (source_count BETWEEN 0 AND 10000),
    lease_token UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('PLANNING','STAGING','VERIFIED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE document_part_attempt_objects (
    attempt_id UUID NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    part INTEGER NOT NULL CHECK (part BETWEEN 1 AND 4),
    sub_key VARCHAR(1024) NOT NULL,
    storage_realm VARCHAR(200) NOT NULL,
    storage_namespace VARCHAR(1024) NOT NULL,
    object_key VARCHAR(2048) NOT NULL CHECK (btrim(object_key) <> ''),
    expected_size BIGINT NOT NULL CHECK (expected_size >= 0),
    expected_sha256 VARCHAR(64) NOT NULL CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
    content_type VARCHAR(1024) NOT NULL CHECK (btrim(content_type) <> ''),
    verified BOOLEAN NOT NULL DEFAULT false,
    provider_version TEXT,
    etag TEXT,
    namespace_digest BYTEA NOT NULL CHECK (namespace_digest=sha256(convert_to(storage_namespace,'UTF8'))),
    key_digest BYTEA NOT NULL CHECK (key_digest=sha256(convert_to(object_key,'UTF8'))),
    sub_key_digest BYTEA NOT NULL CHECK (sub_key_digest=sha256(convert_to(sub_key,'UTF8'))),
    PRIMARY KEY (attempt_id, ordinal),
    FOREIGN KEY (attempt_id) REFERENCES document_part_attempts(attempt_id),
    CHECK ((part = 3 AND btrim(sub_key) <> '') OR (part <> 3 AND sub_key = '')),
    CHECK (object_key ~ '(^|/)documents(/|$)' AND object_key !~ '(^|/)(archive|\.protomolt-managed)(/|$)'),
    CHECK (verified OR (provider_version IS NULL AND etag IS NULL))
);

-- Hash indexes bound UTF-8 index width. A digest collision fails closed by refusing
-- admission; exact coordinates remain stored and are used for all provider I/O.
CREATE UNIQUE INDEX document_part_physical_identity ON document_part_attempt_objects
    (storage_realm, namespace_digest, key_digest);
CREATE UNIQUE INDEX document_part_slot_identity ON document_part_attempt_objects
    (attempt_id, part, sub_key_digest);
CREATE INDEX document_part_unverified ON document_part_attempt_objects(attempt_id) WHERE NOT verified;

CREATE TABLE document_part_attempt_sources (
    attempt_id UUID NOT NULL REFERENCES document_part_attempts(attempt_id),
    source_node_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    PRIMARY KEY (attempt_id, source_node_id)
);
CREATE INDEX document_part_attempt_expiry ON document_part_attempts(lease_until, attempt_id);

CREATE FUNCTION protect_document_part_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE realm TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Document part attempt identity cannot be deleted';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT storage_realm INTO realm FROM managed_backend_profiles WHERE generation=NEW.backend_generation;
        IF realm IS DISTINCT FROM NEW.storage_realm OR NEW.state <> 'PLANNING'
           OR NEW.lease_until <= clock_timestamp() THEN
            RAISE EXCEPTION 'Document part attempt requires an original profile and unsealed plan';
        END IF;
    ELSE
        IF ROW(NEW.attempt_id,NEW.node_id,NEW.account_id,NEW.sampled_revision,NEW.backend_generation,
               NEW.storage_realm,NEW.storage_namespace,NEW.planned_count,NEW.source_count,NEW.lease_token,NEW.created_at)
           IS DISTINCT FROM
           ROW(OLD.attempt_id,OLD.node_id,OLD.account_id,OLD.sampled_revision,OLD.backend_generation,
               OLD.storage_realm,OLD.storage_namespace,OLD.planned_count,OLD.source_count,OLD.lease_token,OLD.created_at) THEN
            RAISE EXCEPTION 'Document part attempt identity is immutable';
        END IF;
        IF OLD.state='VERIFIED' AND NEW.state <> 'VERIFIED'
           OR OLD.state='STAGING' AND NEW.state NOT IN ('STAGING','VERIFIED') THEN
            RAISE EXCEPTION 'Document part attempt state cannot regress';
        END IF;
        IF OLD.lease_until <= clock_timestamp() OR NEW.lease_until < OLD.lease_until THEN
            RAISE EXCEPTION 'Document part attempt lease is expired or shortened';
        END IF;
        IF OLD.state='PLANNING' AND NEW.state='STAGING' THEN
            IF (SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id) <> NEW.planned_count
               OR (SELECT count(*) FROM document_part_attempt_sources WHERE attempt_id=NEW.attempt_id) <> NEW.source_count
               OR (SELECT max(ordinal) FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id) <> NEW.planned_count - 1
               OR (SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id AND part=1) <> 1 THEN
                RAISE EXCEPTION 'Document part attempt plan is incomplete';
            END IF;
        ELSIF OLD.state='PLANNING' AND NEW.state <> 'PLANNING' THEN
            RAISE EXCEPTION 'Document part attempt must seal before verification';
        END IF;
        IF NEW.state='VERIFIED' AND EXISTS (
                SELECT 1 FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id AND NOT verified) THEN
            RAISE EXCEPTION 'Document part attempt has unverified objects';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER document_part_attempt_identity BEFORE INSERT OR UPDATE OR DELETE ON document_part_attempts
FOR EACH ROW EXECUTE FUNCTION protect_document_part_attempt();

CREATE FUNCTION require_sealed_document_part_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id AND state='PLANNING') THEN
        RAISE EXCEPTION 'Document part attempt cannot commit an unsealed plan';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_part_attempt_sealed AFTER INSERT OR UPDATE ON document_part_attempts
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_sealed_document_part_attempt();

CREATE FUNCTION protect_document_part_plan() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_part_attempts%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Document part plan cannot be deleted'; END IF;
    SELECT * INTO STRICT owner FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF owner.state <> 'PLANNING' THEN RAISE EXCEPTION 'Document part plan is sealed'; END IF;
        IF TG_TABLE_NAME='document_part_attempt_objects' THEN
            IF NEW.verified THEN RAISE EXCEPTION 'Document part object must start unverified'; END IF;
            IF NEW.storage_realm <> owner.storage_realm OR NEW.storage_namespace <> owner.storage_namespace THEN
                RAISE EXCEPTION 'Document part object storage differs from attempt';
            END IF;
            NEW.namespace_digest := sha256(convert_to(NEW.storage_namespace,'UTF8'));
            NEW.key_digest := sha256(convert_to(NEW.object_key,'UTF8'));
            NEW.sub_key_digest := sha256(convert_to(NEW.sub_key,'UTF8'));
            IF position('/documents/' || owner.account_id || '/' || owner.node_id::text || '/attempts/' || owner.attempt_id::text || '/' in '/' || NEW.object_key)=0
               OR right(NEW.object_key,1)='/' THEN
                RAISE EXCEPTION 'Object key is outside its document attempt';
            END IF;
        ELSE
            IF NEW.source_node_id=owner.node_id AND NEW.revision <> owner.sampled_revision THEN
                RAISE EXCEPTION 'Same-node source revision differs from destination';
            END IF;
        END IF;
    ELSIF TG_TABLE_NAME='document_part_attempt_sources' THEN
        RAISE EXCEPTION 'Document part source revision is immutable';
    ELSE
        IF ROW(NEW.attempt_id,NEW.ordinal,NEW.part,NEW.sub_key,NEW.storage_realm,NEW.storage_namespace,
               NEW.object_key,NEW.expected_size,NEW.expected_sha256,NEW.content_type)
           IS DISTINCT FROM
           ROW(OLD.attempt_id,OLD.ordinal,OLD.part,OLD.sub_key,OLD.storage_realm,OLD.storage_namespace,
               OLD.object_key,OLD.expected_size,OLD.expected_sha256,OLD.content_type) THEN
            RAISE EXCEPTION 'Document part object identity is immutable';
        END IF;
        IF OLD.verified AND ROW(NEW.verified,NEW.provider_version,NEW.etag)
                IS DISTINCT FROM ROW(OLD.verified,OLD.provider_version,OLD.etag) THEN
            RAISE EXCEPTION 'Verified document part identity is immutable';
        END IF;
        IF ROW(NEW.verified,NEW.provider_version,NEW.etag) IS DISTINCT FROM ROW(OLD.verified,OLD.provider_version,OLD.etag)
           AND (owner.state <> 'STAGING' OR owner.lease_until <= clock_timestamp()) THEN
            RAISE EXCEPTION 'Document part verification requires a live staging lease';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER document_part_object_plan BEFORE INSERT OR UPDATE OR DELETE ON document_part_attempt_objects
FOR EACH ROW EXECUTE FUNCTION protect_document_part_plan();
CREATE TRIGGER document_part_source_plan BEFORE INSERT OR UPDATE OR DELETE ON document_part_attempt_sources
FOR EACH ROW EXECUTE FUNCTION protect_document_part_plan();

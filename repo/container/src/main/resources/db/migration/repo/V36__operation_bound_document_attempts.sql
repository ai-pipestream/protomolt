-- Internal NEW_CONTENT storage, not a public admission or publication API.
ALTER TABLE document_part_attempts
    ADD COLUMN plan_kind text NOT NULL DEFAULT 'FULL_REVISION',
    ADD COLUMN operation_principal varchar(200),
    ADD COLUMN operation_id uuid,
    ADD COLUMN operation_generation bigint,
    ADD COLUMN member_id varchar(128),
    ADD COLUMN drive_id uuid,
    ADD CONSTRAINT document_attempt_operation_scope CHECK (
        (plan_kind='FULL_REVISION' AND operation_principal IS NULL AND operation_id IS NULL
            AND operation_generation IS NULL AND member_id IS NULL AND drive_id IS NULL)
        OR (plan_kind='NEW_CONTENT' AND operation_principal IS NOT NULL AND btrim(operation_principal)<>''
            AND operation_id IS NOT NULL AND operation_generation IS NOT NULL AND operation_generation>0
            AND member_id IS NOT NULL AND member_id ~ '^[a-zA-Z0-9_.-]+$' AND drive_id IS NOT NULL)),
    ADD FOREIGN KEY(account_id,operation_principal,operation_id)
        REFERENCES repository_operations(account_id,principal,operation_id);
-- Replacements may be necessary within one still-live owner generation. A
-- future coordinator selects the exact attempt; do not make retry impossible.
CREATE INDEX document_attempt_operation_member ON document_part_attempts
    (account_id,operation_principal,operation_id,operation_generation,member_id)
    WHERE plan_kind='NEW_CONTENT';

ALTER TABLE document_part_attempt_objects ADD COLUMN revision_ordinal integer;
UPDATE document_part_attempt_objects SET revision_ordinal=ordinal;
ALTER TABLE document_part_attempt_objects ALTER COLUMN revision_ordinal SET NOT NULL;
ALTER TABLE document_part_attempt_objects ADD CHECK(revision_ordinal BETWEEN 0 AND 9999);
CREATE UNIQUE INDEX document_attempt_revision_slot ON document_part_attempt_objects(attempt_id,revision_ordinal);

CREATE FUNCTION fence_document_attempt_operation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RETURN OLD; END IF; -- Existing guards prohibit deletion.
    IF TG_OP='UPDATE' AND ROW(NEW.plan_kind,NEW.operation_principal,NEW.operation_id,
            NEW.operation_generation,NEW.member_id,NEW.drive_id)
        IS DISTINCT FROM ROW(OLD.plan_kind,OLD.operation_principal,OLD.operation_id,
            OLD.operation_generation,OLD.member_id,OLD.drive_id) THEN
        RAISE EXCEPTION 'Document attempt operation binding is immutable';
    END IF;
    IF NEW.plan_kind='NEW_CONTENT' THEN
        PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.operation_principal,
            NEW.operation_id,NEW.operation_generation);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER aa_document_attempt_operation_fence BEFORE INSERT OR UPDATE ON document_part_attempts
FOR EACH ROW EXECUTE FUNCTION fence_document_attempt_operation();

-- Fold operation proof into the existing parent lookup. Legacy writes keep one
-- parent lookup and do not read operation ownership.
CREATE OR REPLACE FUNCTION protect_document_part_plan() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_part_attempts%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Document part plan cannot be deleted'; END IF;
    SELECT * INTO STRICT owner FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    IF owner.plan_kind='NEW_CONTENT' THEN
        -- Recheck database time after any wait. This check takes no owner lock.
        PERFORM require_repository_operation_write_fence(owner.account_id,owner.operation_principal,
            owner.operation_id,owner.operation_generation);
    END IF;
    IF TG_TABLE_NAME='document_part_attempt_objects' THEN
        IF TG_OP='INSERT' AND owner.plan_kind='FULL_REVISION' AND NEW.revision_ordinal IS NULL THEN
            NEW.revision_ordinal := NEW.ordinal;
        END IF;
        IF (TG_OP='UPDATE' AND NEW.revision_ordinal IS DISTINCT FROM OLD.revision_ordinal)
            OR (owner.plan_kind='FULL_REVISION' AND NEW.revision_ordinal IS DISTINCT FROM NEW.ordinal) THEN
            RAISE EXCEPTION 'Document attempt revision ordinal is immutable and must match its plan kind';
        END IF;
    END IF;
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

-- Preserve V21 identity, lease, ordering and verification checks. Only the CORE
-- requirement branches: a new-byte subset may omit a retained CORE.
CREATE OR REPLACE FUNCTION protect_document_part_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
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
               OR (NEW.plan_kind='FULL_REVISION' AND
                    (SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=NEW.attempt_id AND part=1) <> 1) THEN
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

-- V22 represents a whole uploaded revision, never a partial new-byte subset.
CREATE FUNCTION reject_partial_attempt_legacy_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id AND plan_kind='NEW_CONTENT') THEN
        RAISE EXCEPTION 'NEW_CONTENT requires complete revision publication, not the legacy uploaded manifest';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER a0_reject_partial_document_history BEFORE INSERT ON document_part_publication_history
FOR EACH ROW EXECUTE FUNCTION reject_partial_attempt_legacy_publication();
CREATE TRIGGER a0_reject_partial_document_current BEFORE INSERT OR UPDATE ON document_part_publications
FOR EACH ROW EXECUTE FUNCTION reject_partial_attempt_legacy_publication();

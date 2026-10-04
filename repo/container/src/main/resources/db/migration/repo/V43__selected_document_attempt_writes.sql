-- NEW_CONTENT write admission is separate from selection and publication.
-- The owner must already be fenced and the caller must hold the attempt row.
-- No lock is acquired on the selection pointer: the owner fence excludes other
-- transactions, while a fresh lookup also sees a CAS in this same transaction.
CREATE FUNCTION require_selected_document_attempt(a document_part_attempts)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_operation_write_fence(a.account_id,a.operation_principal,a.operation_id,a.operation_generation);
 IF a.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'Document part attempt lease is expired or shortened'; END IF;
 IF NOT EXISTS(
  SELECT 1 FROM document_operation_selection_current c
  JOIN document_operation_selection_attempts h
  USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
  WHERE c.account_id=a.account_id AND c.principal=a.operation_principal AND c.operation_id=a.operation_id
   AND c.owner_generation=a.operation_generation AND c.member_id=a.member_id AND h.attempt_id=a.attempt_id
 ) OR EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=a.attempt_id) THEN
  RAISE EXCEPTION 'Document write requires its current selected attempt without cleanup';
 END IF;
 RETURN true;
END;
$$;
CREATE OR REPLACE FUNCTION fence_document_attempt_operation() RETURNS trigger LANGUAGE plpgsql AS $$
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
    IF TG_OP='UPDATE' AND NEW.plan_kind='NEW_CONTENT' AND OLD.state<>'PLANNING' THEN
        PERFORM require_selected_document_attempt(OLD);
    END IF;
    RETURN NEW;
END;
$$;
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
    IF TG_OP='UPDATE' AND owner.plan_kind='NEW_CONTENT' THEN
        PERFORM require_selected_document_attempt(owner);
    END IF;
    RETURN NEW;
END;
$$;

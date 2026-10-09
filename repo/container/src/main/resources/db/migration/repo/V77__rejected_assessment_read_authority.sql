-- Existing live-owner captures retain their authority and release protocol.
ALTER TABLE document_assessment_read_sessions ADD COLUMN authority TEXT NOT NULL DEFAULT 'LIVE_OWNER'
 CHECK(authority IN ('LIVE_OWNER','ADMISSION_REJECTION'));

CREATE OR REPLACE FUNCTION guard_document_assessment_read_session() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_assessment_owners%ROWTYPE; receipt repository_operation_rejection%ROWTYPE;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-'release_xid') IS DISTINCT FROM (to_jsonb(OLD)-'release_xid')
   OR OLD.release_xid IS NOT NULL OR NEW.release_xid IS NULL
   OR NEW.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment read session identity is immutable';
  END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN
  IF OLD.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned()
   AND NOT EXISTS(SELECT 1 FROM repository_reader_incarnations
    WHERE incarnation=OLD.reader_incarnation AND state='QUIESCED') THEN
   RAISE EXCEPTION 'Assessment read release requires local drain or proven reader quiescence';
  END IF;
  RETURN OLD;
 END IF;
 IF NEW.release_xid IS NOT NULL THEN RAISE EXCEPTION 'New assessment read session cannot be released'; END IF;
 PERFORM require_active_repository_reader(NEW.reader_incarnation);
 SELECT * INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id;
 IF NEW.authority='LIVE_OWNER' THEN
  PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 ELSE
  -- Shared owner lock precedes assessment/session locks. A receipt grants no
  -- write fence and does not require the expired writer's token or lease.
  PERFORM 1 FROM repository_operation_owners WHERE account_id=owner.account_id AND principal=owner.principal
   AND operation_id=owner.operation_id AND owner_generation=owner.owner_generation FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'Rejected assessment read requires its deciding generation'; END IF;
  SELECT * INTO STRICT receipt FROM repository_operation_rejection
   WHERE account_id=owner.account_id AND principal=owner.principal AND operation_id=owner.operation_id;
  IF receipt.reason<>2 OR receipt.disposition<>1
   OR ROW(receipt.owner_generation,receipt.command_codec,receipt.command_version,receipt.command_sha256,
          receipt.assessment_id,receipt.manifest_sha256)
    IS DISTINCT FROM ROW(owner.owner_generation,owner.command_codec,owner.command_version,owner.command_sha256,
          owner.assessment_id,owner.manifest_sha256)
   OR receipt.manifest_codec<>'document-publication-assessment' OR receipt.manifest_version<>1
   OR receipt.retain_until_epoch_micros<>floor(extract(epoch FROM owner.retain_until)*1000000)
   OR EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=owner.account_id
      AND principal=owner.principal AND operation_id=owner.operation_id) THEN
   RAISE EXCEPTION 'Rejected assessment read requires its exact admission rejection';
  END IF;
 END IF;
 SELECT * INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id FOR SHARE;
 IF NEW.authority='LIVE_OWNER' THEN
  PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 END IF;
 IF NOT owner.sealed OR owner.release_xid IS NOT NULL OR owner.retain_until<=clock_timestamp() THEN
  RAISE EXCEPTION 'Assessment reader requires an available sealed owner';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM document_assessment_slot_snapshots WHERE assessment_id=NEW.assessment_id) THEN
  RAISE EXCEPTION 'Assessment reader requires retained slot provenance';
 END IF;
 -- Complete canonical verification and current target/source READ authorization
 -- remain duties of the trusted Java handler, independently of this SQL guard.
 RETURN NEW;
END;
$$;

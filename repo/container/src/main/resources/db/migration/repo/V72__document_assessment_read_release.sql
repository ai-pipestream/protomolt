-- Normal release is invoked only by the owning local lifecycle after all Uses
-- and provider batches drain. SQL validates identity/atomicity, not Java drain.
-- Direct table DML is inside this same trusted database-host boundary; a role
-- able to mark and delete rows can invoke the equivalent transition directly.
ALTER TABLE document_assessment_read_sessions ADD COLUMN release_xid xid8;

CREATE OR REPLACE FUNCTION guard_document_assessment_read_session() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_assessment_owners%ROWTYPE;
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
 PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 SELECT * INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id FOR SHARE;
 PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 IF NOT owner.sealed OR owner.release_xid IS NOT NULL OR owner.retain_until<=clock_timestamp() THEN
  RAISE EXCEPTION 'Assessment reader requires an available sealed owner';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM document_assessment_slot_snapshots WHERE assessment_id=NEW.assessment_id) THEN
  RAISE EXCEPTION 'Assessment reader requires retained slot provenance';
 END IF;
 RETURN NEW;
END;
$$;

CREATE FUNCTION require_document_assessment_read_release_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE session_id=NEW.session_id AND release_xid IS NOT NULL) THEN
  RAISE EXCEPTION 'Assessment read release must remove its session atomically';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_assessment_read_release_complete AFTER UPDATE ON document_assessment_read_sessions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_assessment_read_release_complete();

CREATE FUNCTION release_document_assessment_read_session(p_session UUID,p_reader UUID,p_assessment UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF p_session IS NULL OR p_reader IS NULL OR p_assessment IS NULL THEN
  RAISE EXCEPTION 'Assessment read release requires complete identity';
 END IF;
 PERFORM 1 FROM document_assessment_read_sessions WHERE session_id=p_session FOR UPDATE;
 IF EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE session_id=p_session
  AND (reader_incarnation<>p_reader OR assessment_id<>p_assessment)) THEN
  RAISE EXCEPTION 'Assessment read session belongs to another reader or assessment';
 END IF;
 UPDATE document_assessment_read_sessions SET release_xid=pg_current_xact_id()
  WHERE session_id=p_session AND reader_incarnation=p_reader AND assessment_id=p_assessment;
 DELETE FROM document_assessment_read_sessions WHERE session_id=p_session
  AND reader_incarnation=p_reader AND assessment_id=p_assessment;
 RETURN true;
END;
$$;

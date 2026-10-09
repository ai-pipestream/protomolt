-- Whole-assessment reader retention. This is not an authorization or read API.
-- Existing ASSESSMENT references protect all bytes; this session additionally
-- keeps schemas, root evidence and the manifest under the same retained owner.
CREATE TABLE document_assessment_read_sessions (
 session_id UUID PRIMARY KEY,
 reader_incarnation UUID NOT NULL REFERENCES repository_reader_incarnations(incarnation),
 assessment_id UUID NOT NULL REFERENCES document_assessment_owners(assessment_id) ON DELETE RESTRICT,
 acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX document_assessment_read_session_owner ON document_assessment_read_sessions(assessment_id,session_id);
CREATE INDEX document_assessment_read_session_reader ON document_assessment_read_sessions(reader_incarnation,session_id);

CREATE FUNCTION guard_document_assessment_read_session() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_assessment_owners%ROWTYPE;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Assessment read session identity is immutable'; END IF;
 IF TG_OP='DELETE' THEN
  -- Until bounded local-handle release is integrated, only durable quiescence
  -- permits deletion. FENCED or elapsed time is never enough.
  IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations
   WHERE incarnation=OLD.reader_incarnation AND state='QUIESCED') THEN
   RAISE EXCEPTION 'Assessment read release requires proven reader quiescence';
  END IF;
  RETURN OLD;
 END IF;
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
 -- SQL identity checks do not replace complete canonical manifest/slot checks
 -- and current authorization by the trusted capture handler.
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_read_session_guard BEFORE INSERT OR UPDATE OR DELETE
 ON document_assessment_read_sessions FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_read_session();

CREATE FUNCTION recover_quiesced_document_assessment_read_session(p_session UUID,p_reader UUID,p_assessment UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF p_session IS NULL OR p_reader IS NULL OR p_assessment IS NULL THEN
  RAISE EXCEPTION 'Assessment read release requires complete identity';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE incarnation=p_reader AND state='QUIESCED') THEN
  RAISE EXCEPTION 'Assessment read release requires proven reader quiescence';
 END IF;
 -- No owner/physical locks after session locks. Quiescence is permanent and the
 -- native session protects its parent until this exact DELETE commits.
 PERFORM 1 FROM document_assessment_read_sessions WHERE session_id=p_session FOR UPDATE;
 IF EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE session_id=p_session
  AND (reader_incarnation<>p_reader OR assessment_id<>p_assessment)) THEN
  RAISE EXCEPTION 'Assessment read session belongs to another reader or assessment';
 END IF;
 DELETE FROM document_assessment_read_sessions WHERE session_id=p_session
  AND reader_incarnation=p_reader AND assessment_id=p_assessment;
 RETURN true;
END;
$$;
CREATE OR REPLACE FUNCTION guard_document_assessment_owner() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE command repository_operations%ROWTYPE; objects UUID[];
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(OLD.account_id,OLD.principal,OLD.operation_id);
  IF OLD.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment owner deletion requires explicit recovery';
  END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='INSERT' THEN
  PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
  SELECT * INTO STRICT command FROM repository_operations WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id;
  IF ROW(NEW.command_codec,NEW.command_version,NEW.command_sha256)
   IS DISTINCT FROM ROW(command.command_codec,command.command_version,command.command_sha256)
   OR command.command_codec<>'document-publication' OR command.command_version<>1 THEN
   RAISE EXCEPTION 'Assessment differs from admitted document command';
  END IF;
  IF NEW.sealed OR NEW.release_xid IS NOT NULL OR NEW.retain_until<=clock_timestamp()
   OR NEW.retain_until>clock_timestamp()+interval '1 day' THEN
   RAISE EXCEPTION 'Assessment requires unsealed staging with an explicit deadline within one day';
  END IF;
  NEW.creation_xid:=pg_current_xact_id();
  RETURN NEW;
 END IF;
 IF (to_jsonb(NEW)-'sealed'-'release_xid') IS DISTINCT FROM (to_jsonb(OLD)-'sealed'-'release_xid') THEN
  RAISE EXCEPTION 'Assessment owner identity and evidence are immutable';
 END IF;
 IF NOT OLD.sealed AND NEW.sealed AND OLD.creation_xid=pg_current_xact_id_if_assigned()
  AND OLD.release_xid IS NULL AND NEW.release_xid IS NULL THEN
  PERFORM check_document_assessment_slots(NEW);
  RETURN NEW;
 END IF;
 IF OLD.sealed AND NEW.sealed AND OLD.release_xid IS NULL AND NEW.release_xid=pg_current_xact_id_if_assigned() THEN
  PERFORM require_repository_operation_recovery_fence(OLD.account_id,OLD.principal,OLD.operation_id);
  IF OLD.retain_until>clock_timestamp() THEN RAISE EXCEPTION 'Assessment staging has not expired'; END IF;
  IF EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE assessment_id=OLD.assessment_id) THEN
   RAISE EXCEPTION 'Assessment release requires all reader sessions to drain';
  END IF;
  SELECT array_agg(object_id ORDER BY object_id) INTO objects FROM document_assessment_objects WHERE assessment_id=OLD.assessment_id;
  PERFORM lock_repository_retention_set(COALESCE(objects,'{}'::UUID[]));
  RETURN NEW;
 END IF;
 RAISE EXCEPTION 'Unsupported assessment owner transition';
END;
$$;

-- Capture identities outlive session release. Absence alone is not rollback
-- evidence while an original capture transaction may still commit.
LOCK TABLE document_assessment_read_sessions IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE document_assessment_read_identities (
 session_id UUID PRIMARY KEY,
 reader_incarnation UUID NOT NULL REFERENCES repository_reader_incarnations(incarnation),
 assessment_id UUID NOT NULL,
 released BOOLEAN NOT NULL DEFAULT false,
 UNIQUE(session_id,reader_incarnation,assessment_id)
);
INSERT INTO document_assessment_read_identities(session_id,reader_incarnation,assessment_id)
 SELECT session_id,reader_incarnation,assessment_id FROM document_assessment_read_sessions;
ALTER TABLE document_assessment_read_sessions ADD CONSTRAINT document_assessment_read_identity_fk
 FOREIGN KEY(session_id,reader_incarnation,assessment_id)
 REFERENCES document_assessment_read_identities(session_id,reader_incarnation,assessment_id)
 DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION guard_document_assessment_read_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Assessment read identities are permanent'; END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.released OR NOT EXISTS(SELECT 1 FROM document_assessment_read_sessions
   WHERE session_id=NEW.session_id AND reader_incarnation=NEW.reader_incarnation AND assessment_id=NEW.assessment_id) THEN
   RAISE EXCEPTION 'Assessment read identity requires its exact live session';
  END IF;
  RETURN NEW;
 END IF;
 IF ROW(NEW.session_id,NEW.reader_incarnation,NEW.assessment_id) IS DISTINCT FROM
  ROW(OLD.session_id,OLD.reader_incarnation,OLD.assessment_id) OR OLD.released OR NOT NEW.released
  OR EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE session_id=OLD.session_id) THEN
  RAISE EXCEPTION 'Assessment read identity permits only completed release';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_read_identity_guard BEFORE INSERT OR UPDATE OR DELETE
 ON document_assessment_read_identities FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_read_identity();

CREATE FUNCTION record_document_assessment_read_identity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE changed BIGINT;
BEGIN
 IF TG_OP='INSERT' THEN
  -- The permanent unique key rejects reuse even after a concurrent prior DELETE.
  INSERT INTO document_assessment_read_identities(session_id,reader_incarnation,assessment_id)
   VALUES(NEW.session_id,NEW.reader_incarnation,NEW.assessment_id);
  RETURN NEW;
 END IF;
 UPDATE document_assessment_read_identities SET released=true WHERE session_id=OLD.session_id
  AND reader_incarnation=OLD.reader_incarnation AND assessment_id=OLD.assessment_id AND NOT released;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'Assessment read release lost its exact capture identity'; END IF;
 RETURN OLD;
END;
$$;
CREATE TRIGGER document_assessment_read_identity_record AFTER INSERT OR DELETE ON document_assessment_read_sessions
 FOR EACH ROW EXECUTE FUNCTION record_document_assessment_read_identity();

CREATE FUNCTION require_document_assessment_read_identity_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE identity document_assessment_read_identities%ROWTYPE; present BOOLEAN;
BEGIN
 SELECT * INTO STRICT identity FROM document_assessment_read_identities WHERE session_id=NEW.session_id;
 SELECT EXISTS(SELECT 1 FROM document_assessment_read_sessions WHERE session_id=identity.session_id
  AND reader_incarnation=identity.reader_incarnation AND assessment_id=identity.assessment_id) INTO present;
 IF present=identity.released THEN RAISE EXCEPTION 'Assessment read identity and session release differ'; END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_assessment_read_identity_complete AFTER INSERT OR UPDATE ON document_assessment_read_identities
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_assessment_read_identity_complete();

CREATE OR REPLACE FUNCTION release_document_assessment_read_session(p_session UUID,p_reader UUID,p_assessment UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE identity document_assessment_read_identities%ROWTYPE; locked_session BOOLEAN;
BEGIN
 PERFORM require_repository_read_committed();
 IF p_session IS NULL OR p_reader IS NULL OR p_assessment IS NULL THEN
  RAISE EXCEPTION 'Assessment read release requires complete identity';
 END IF;
 -- Native session before identity, matching its AFTER INSERT/DELETE triggers.
 PERFORM 1 FROM document_assessment_read_sessions WHERE session_id=p_session FOR UPDATE;
 locked_session:=FOUND;
 SELECT * INTO identity FROM document_assessment_read_identities WHERE session_id=p_session FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment capture outcome is not established'; END IF;
 IF identity.reader_incarnation<>p_reader OR identity.assessment_id<>p_assessment THEN
  RAISE EXCEPTION 'Assessment read session belongs to another reader or assessment';
 END IF;
 IF identity.released THEN RETURN true; END IF;
 -- A capture may commit between the two SELECTs. Never acquire a newly visible
 -- session after its identity lock: that reverses the normal release order.
 IF NOT locked_session THEN RAISE EXCEPTION 'Assessment capture outcome is not established'; END IF;
 UPDATE document_assessment_read_sessions SET release_xid=pg_current_xact_id()
  WHERE session_id=p_session AND reader_incarnation=p_reader AND assessment_id=p_assessment;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment read identity has no live session'; END IF;
 DELETE FROM document_assessment_read_sessions WHERE session_id=p_session
  AND reader_incarnation=p_reader AND assessment_id=p_assessment;
 RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION recover_quiesced_document_assessment_read_session(p_session UUID,p_reader UUID,p_assessment UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF p_session IS NULL OR p_reader IS NULL OR p_assessment IS NULL THEN
  RAISE EXCEPTION 'Assessment read release requires complete identity';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE incarnation=p_reader AND state='QUIESCED') THEN
  RAISE EXCEPTION 'Assessment read release requires proven reader quiescence';
 END IF;
 -- QUIESCED is permanent and its fence waited for active capture transactions.
 -- Unknown absence is conclusive only here; no capture by this reader can follow.
 IF NOT EXISTS(SELECT 1 FROM document_assessment_read_identities WHERE session_id=p_session) THEN RETURN true; END IF;
 RETURN release_document_assessment_read_session(p_session,p_reader,p_assessment);
END;
$$;

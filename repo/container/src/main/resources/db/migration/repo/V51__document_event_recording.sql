-- Event storage is independent of broker delivery. This does not activate native
-- revision publication or supply its still-required operation/member event links.
LOCK TABLE document_events_outbox IN SHARE ROW EXCLUSIVE MODE;

-- Existing events have unknown insertion provenance; do not assign this migration's
-- transaction to them. Only a real subsequent INSERT receives a stamped xid.
ALTER TABLE document_events_outbox ADD COLUMN insertion_xid xid8;
ALTER TABLE document_events_outbox DROP CONSTRAINT chk_document_events_outbox_status;
ALTER TABLE document_events_outbox ADD CONSTRAINT chk_document_events_outbox_status
 CHECK(status IN ('RECORDED','PENDING','PUBLISHED','FAILED'));
ALTER TABLE document_events_outbox ADD CONSTRAINT chk_document_event_recorded_delivery
 CHECK(status<>'RECORDED' OR (attempts=0 AND published_at IS NULL AND last_error IS NULL));

CREATE FUNCTION protect_document_event_recording() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.insertion_xid := pg_current_xact_id();
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN
  IF OLD.status='RECORDED' THEN
   RAISE EXCEPTION 'Recorded event is retained without delivery';
  END IF;
  RETURN OLD;
 END IF;
 IF ROW(NEW.event_id,NEW.event_type,NEW.payload,NEW.kafka_key,NEW.created_at,NEW.insertion_xid)
  IS DISTINCT FROM ROW(OLD.event_id,OLD.event_type,OLD.payload,OLD.kafka_key,OLD.created_at,OLD.insertion_xid) THEN
  RAISE EXCEPTION 'Document event identity is immutable';
 END IF;
 IF (OLD.status='RECORDED' OR NEW.status='RECORDED') AND NEW.status<>OLD.status THEN
  RAISE EXCEPTION 'Recorded event has no delivery request';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_event_recording_guard BEFORE INSERT OR UPDATE OR DELETE ON document_events_outbox
 FOR EACH ROW EXECUTE FUNCTION protect_document_event_recording();

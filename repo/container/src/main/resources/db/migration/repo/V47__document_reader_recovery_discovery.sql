-- Support bounded discovery without sorting the reader's entire remaining pin set.
CREATE INDEX document_read_pin_incarnation_order
 ON document_read_pins(reader_incarnation,pin_id) INCLUDE(object_id);
DROP INDEX document_read_pin_incarnation;

-- Discovers durable identities after local drain has already been attested. This
-- does not prove that an ACTIVE, FENCED or UNKNOWN remote process has stopped.
CREATE FUNCTION recover_quiesced_document_read_pin_batch(p_reader UUID, p_limit INTEGER)
RETURNS INTEGER LANGUAGE plpgsql VOLATILE AS $$
DECLARE claims JSONB; observed INTEGER;
BEGIN
 PERFORM require_repository_read_committed();
 IF p_limit IS NULL OR p_limit < 1 OR p_limit > 10000 THEN
  RAISE EXCEPTION 'Reader recovery batch requires 1 to 10000 claims'; END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE incarnation=p_reader AND state='QUIESCED') THEN
  RAISE EXCEPTION 'Reader recovery requires proven quiescence'; END IF;
 -- Do not lock native pins here: V45 must acquire the complete origin and
 -- retention sets before pin locks. Concurrent recovery may observe the same set.
 SELECT jsonb_agg(jsonb_build_object('pin',q.pin_id,'object',q.object_id) ORDER BY q.pin_id)
 INTO claims FROM (
  SELECT pin_id,object_id FROM document_read_pins
  WHERE reader_incarnation=p_reader ORDER BY pin_id LIMIT p_limit
 ) q;
 IF claims IS NULL THEN RETURN 0; END IF;
 observed := jsonb_array_length(claims);
 IF NOT recover_quiesced_document_read_pins(p_reader,claims) THEN
  RAISE EXCEPTION 'Reader recovery was not acknowledged'; END IF;
 -- Number selected, not a deletion count: a concurrent release may win first.
 RETURN observed;
END;
$$;

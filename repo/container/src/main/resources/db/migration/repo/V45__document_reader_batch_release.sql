-- Hosts must prove all provider calls/batch owners have drained before release.
-- This function checks durable identity and atomicity, not process quiescence.
CREATE FUNCTION release_document_read_pins(p_reader UUID, p_claims JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE requested BIGINT; unique_pins BIGINT; complete BIGINT; locked_origins BIGINT; objects UUID[];
BEGIN
 PERFORM require_repository_read_committed();
 IF p_reader IS NULL OR p_claims IS NULL OR jsonb_typeof(p_claims)<>'array' THEN
  RAISE EXCEPTION 'Document pin release requires reader and claim array'; END IF;
 requested := jsonb_array_length(p_claims);
 IF requested<1 OR requested>10000 THEN RAISE EXCEPTION 'Document pin release requires 1 to 10000 claims'; END IF;
 SELECT count(DISTINCT q.pin),count(*) FILTER(WHERE q.pin IS NOT NULL AND q.object IS NOT NULL),array_agg(DISTINCT q.object)
  INTO unique_pins,complete,objects FROM jsonb_to_recordset(p_claims) q(pin uuid,object uuid);
 IF unique_pins<>requested OR complete<>requested THEN
  RAISE EXCEPTION 'Document pin release requires unique complete pin identities'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_to_recordset(p_claims) q(pin uuid,object uuid)
   JOIN document_read_pins p ON p.pin_id=q.pin
   WHERE p.reader_incarnation<>p_reader OR p.object_id<>q.object) THEN
  RAISE EXCEPTION 'Document read pin belongs to another incarnation or object'; END IF;
 IF (SELECT count(*) FROM repository_physical_locations WHERE object_id=ANY(objects) AND source_kind='DOCUMENT_PART')<>cardinality(objects) THEN
  RAISE EXCEPTION 'Document release object is not registered'; END IF;
 -- Complete origin set before any retention row. Mirrors only reacquire SHARE.
 PERFORM 1 FROM document_part_attempts a WHERE a.attempt_id IN (
  SELECT source_id FROM repository_physical_locations WHERE object_id=ANY(objects))
  ORDER BY a.attempt_id FOR SHARE OF a;
 GET DIAGNOSTICS locked_origins = ROW_COUNT;
 IF locked_origins<>(SELECT count(DISTINCT source_id) FROM repository_physical_locations WHERE object_id=ANY(objects)) THEN
  RAISE EXCEPTION 'Document release requires durable origins'; END IF;
 PERFORM 1 FROM repository_object_retention r WHERE r.object_id=ANY(objects) ORDER BY r.object_id FOR SHARE OF r;
 IF (SELECT count(*) FROM repository_object_retention WHERE object_id=ANY(objects))<>cardinality(objects) THEN
  RAISE EXCEPTION 'Repository object retention row is missing'; END IF;
 -- Concurrent releases of overlapping batches lock their native rows in one order.
 PERFORM 1 FROM document_read_pins p WHERE p.pin_id IN (
  SELECT q.pin FROM jsonb_to_recordset(p_claims) q(pin uuid)) ORDER BY p.pin_id FOR UPDATE OF p;
 IF EXISTS(SELECT 1 FROM jsonb_to_recordset(p_claims) q(pin uuid,object uuid)
   JOIN document_read_pins p ON p.pin_id=q.pin
   WHERE p.reader_incarnation<>p_reader OR p.object_id<>q.object) THEN
  RAISE EXCEPTION 'Document read pin belongs to another incarnation or object'; END IF;
 DELETE FROM document_read_pins p USING jsonb_to_recordset(p_claims) q(pin uuid,object uuid)
  WHERE p.pin_id=q.pin AND p.reader_incarnation=p_reader AND p.object_id=q.object;
 -- Missing pins represent a prior committed release. The mirror trigger removes
 -- references in this transaction; any failed mirror rolls back the complete batch.
 RETURN true;
END;
$$;

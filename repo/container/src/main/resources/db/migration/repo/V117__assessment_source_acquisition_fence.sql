-- Pruning prerequisite: source-slot creation must serialize with a source
-- document's exclusive decision, including SQL callers without Java prelocks.
-- This does not certify canonical source coverage or enable revision pruning.
CREATE FUNCTION fence_assessment_source_acquisition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source uuid;
BEGIN
 IF NEW.declaration IN ('REUSE','HISTORICAL_REUSE') THEN
  -- REUSE deliberately has no source_node. Resolve both modes from the immutable
  -- revision identity, never a caller-supplied document address alone.
  SELECT node_id INTO source FROM document_revision_publications WHERE revision_id=NEW.source_revision;
  IF FOUND THEN PERFORM require_document_source_acquisition(source); END IF;
  -- Existing slot shape and seal guards still reject absent/wrong revisions,
  -- historical node mismatches, stale current sources and wrong physical objects.
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_document_assessment_source_acquisition BEFORE INSERT ON document_assessment_slots
 FOR EACH ROW EXECUTE FUNCTION fence_assessment_source_acquisition();

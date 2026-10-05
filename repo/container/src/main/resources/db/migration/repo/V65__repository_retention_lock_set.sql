-- Shared lock primitive for multi-object owners. No reference or admission is
-- created here. Callers hold operation, authorization and native-owner fences
-- before entry and supply their complete set before mutating any references.
-- This is an exclusive writer/recovery path, not the shared reader path.
CREATE FUNCTION lock_repository_retention_set(p_objects UUID[])
RETURNS INTEGER LANGUAGE plpgsql VOLATILE AS $$
DECLARE objects UUID[]; archives UUID[]; attempts UUID[]; expected INTEGER; locked INTEGER;
BEGIN
 PERFORM require_repository_read_committed();
 IF p_objects IS NULL OR cardinality(p_objects)>10000
  OR COALESCE(array_ndims(p_objects),1)<>1 THEN
  RAISE EXCEPTION 'Repository retention lock set requires at most 10000 object identities';
 END IF;
 IF EXISTS(SELECT 1 FROM unnest(p_objects) id WHERE id IS NULL) THEN
  RAISE EXCEPTION 'Repository retention lock set cannot contain null identities';
 END IF;
 SELECT COALESCE(array_agg(DISTINCT id ORDER BY id),'{}'::UUID[]) INTO objects FROM unnest(p_objects) id;
 expected:=cardinality(objects);
 IF expected=0 THEN RETURN 0; END IF;
 IF (SELECT count(*) FROM repository_physical_locations WHERE object_id=ANY(objects))<>expected THEN
  RAISE EXCEPTION 'Repository retention lock set requires every physical location';
 END IF;
 -- Physical identities are immutable. Use PostgreSQL UUID ordering throughout,
 -- with ARCHIVE before DOCUMENT_PART, independent of input order/duplicates.
 SELECT COALESCE(array_agg(DISTINCT source_id ORDER BY source_id),'{}'::UUID[]) INTO archives
 FROM repository_physical_locations WHERE object_id=ANY(objects) AND source_kind='ARCHIVE';
 SELECT COALESCE(array_agg(DISTINCT source_id ORDER BY source_id),'{}'::UUID[]) INTO attempts
 FROM repository_physical_locations WHERE object_id=ANY(objects) AND source_kind='DOCUMENT_PART';
 PERFORM 1 FROM archive_object_uploads WHERE object_id=ANY(archives) ORDER BY object_id FOR UPDATE;
 GET DIAGNOSTICS locked=ROW_COUNT;
 IF locked<>cardinality(archives) THEN
  RAISE EXCEPTION 'Repository retention lock set requires every archive source owner';
 END IF;
 PERFORM 1 FROM document_part_attempts WHERE attempt_id=ANY(attempts) ORDER BY attempt_id FOR UPDATE;
 GET DIAGNOSTICS locked=ROW_COUNT;
 IF locked<>cardinality(attempts) THEN
  RAISE EXCEPTION 'Repository retention lock set requires every document source owner';
 END IF;
 -- Never interleave source locks and retention locks, even across source kinds.
 PERFORM 1 FROM repository_object_retention WHERE object_id=ANY(objects) ORDER BY object_id FOR UPDATE;
 GET DIAGNOSTICS locked=ROW_COUNT;
 IF locked<>expected THEN
  RAISE EXCEPTION 'Repository retention lock set requires every physical retention row';
 END IF;
 -- Retired/reclaiming objects are deliberately allowed: releasing an existing
 -- owner needs the same locks. Acquisition must separately refuse these states.
 RETURN locked;
END;
$$;

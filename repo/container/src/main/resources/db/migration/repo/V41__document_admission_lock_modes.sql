-- Stage read-only sources concurrently while protecting policy and revisions.
-- All advisory locks precede row locks. A colliding write key always wins.
CREATE FUNCTION lock_document_admission_keys(read_keys bigint[],write_keys bigint[]) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE lock_key bigint; exclusive_mode boolean;
BEGIN
 PERFORM require_repository_read_committed();
 IF read_keys IS NULL OR write_keys IS NULL
  OR cardinality(read_keys)>10064 OR cardinality(write_keys)>64
  OR COALESCE(array_ndims(read_keys),1)<>1 OR COALESCE(array_ndims(write_keys),1)<>1
  OR array_position(read_keys,NULL) IS NOT NULL OR array_position(write_keys,NULL) IS NOT NULL
  OR (SELECT count(DISTINCT k) FROM unnest(read_keys || write_keys) keys(k))>10064 THEN
  RAISE EXCEPTION 'Document admission lock keys exceed their non-null one-dimensional bounds';
 END IF;
 FOR lock_key,exclusive_mode IN
  SELECT k,bool_or(w) FROM (
   SELECT k,false w FROM unnest(read_keys) keys(k)
   UNION ALL SELECT k,true w FROM unnest(write_keys) keys(k)
  ) requested GROUP BY k ORDER BY k
 LOOP
  IF exclusive_mode THEN PERFORM pg_advisory_xact_lock(lock_key);
  ELSE PERFORM pg_advisory_xact_lock_shared(lock_key); END IF;
 END LOOP;
 RETURN true;
END;
$$;

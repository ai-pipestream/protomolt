-- Keep the existing global two-phase order: every advisory key first, then
-- document rows. The loop makes lock acquisition order explicit, independent
-- of SQL target-list evaluation. Missing document rows still have a lock.
CREATE FUNCTION lock_document_revision_keys(keys bigint[]) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    lock_key bigint;
BEGIN
    PERFORM require_repository_read_committed();
    IF keys IS NULL OR cardinality(keys) > 10064
       OR COALESCE(array_ndims(keys), 1) <> 1
       OR array_position(keys, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'Document revision lock keys must be a non-null array of at most 10064 non-null keys';
    END IF;
    FOR lock_key IN SELECT DISTINCT k FROM unnest(keys) AS input(k) ORDER BY k LOOP
        PERFORM pg_advisory_xact_lock(lock_key);
    END LOOP;
    RETURN true;
END;
$$;

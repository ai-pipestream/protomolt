SELECT a.attempt_id FROM document_part_attempts a
LEFT JOIN document_part_attempt_cleanup c ON c.attempt_id=a.attempt_id
WHERE a.lease_until <= clock_timestamp() AND a.state<>'PLANNING'
    AND (CAST($1 AS text) IS NULL OR a.backend_generation=CAST($2 AS text))
    AND NOT EXISTS (SELECT 1 FROM document_part_publication_history h WHERE h.attempt_id=a.attempt_id)
    AND NOT EXISTS (SELECT 1 FROM document_part_attempt_objects o
        JOIN repository_object_references r ON r.object_id=o.physical_object_id
        WHERE o.attempt_id=a.attempt_id AND r.owner_kind='ASSESSMENT')
    AND (c.attempt_id IS NULL OR (c.claim_until <= clock_timestamp()
        AND c.last_checked_at <= clock_timestamp()-($3 * interval '1 millisecond')))
ORDER BY COALESCE(c.last_checked_at,a.lease_until),a.attempt_id LIMIT $4
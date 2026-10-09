SELECT a.attempt_id FROM document_part_attempts a
LEFT JOIN document_part_attempt_cleanup c ON c.attempt_id=a.attempt_id
WHERE a.lease_until <= clock_timestamp() AND a.state<>$5
    AND (CAST($1 AS text) IS NULL OR a.backend_generation=CAST($2 AS text))
    AND NOT EXISTS (SELECT $6 FROM document_part_publication_history h WHERE h.attempt_id=a.attempt_id)
    AND NOT EXISTS (SELECT $7 FROM document_part_attempt_objects o
        JOIN repository_object_references r ON r.object_id=o.physical_object_id
        WHERE o.attempt_id=a.attempt_id AND r.owner_kind=$8)
    AND (c.attempt_id IS NULL OR (c.claim_until <= clock_timestamp()
        AND c.last_checked_at <= clock_timestamp()-($3 * interval $9)))
ORDER BY COALESCE(c.last_checked_at,a.lease_until),a.attempt_id LIMIT $4
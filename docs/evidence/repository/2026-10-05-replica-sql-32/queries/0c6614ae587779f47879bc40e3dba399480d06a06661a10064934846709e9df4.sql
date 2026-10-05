SELECT u.object_id FROM archive_object_uploads u WHERE u.updated_at<=$1
AND u.state IN ($2 /*, ... */)
AND (u.state<>$3 OR u.cleanup_error IS NOT NULL OR u.updated_at<=$4)
AND EXISTS (SELECT $5 FROM archive_mutation_targets t WHERE t.object_id=u.object_id)
AND NOT EXISTS (SELECT $6 FROM archive_version_object_refs r WHERE r.object_id=u.object_id)
AND NOT EXISTS (SELECT $7 FROM repository_object_references r WHERE r.object_id=u.object_id)
ORDER BY u.updated_at,u.object_id fetch first $8 rows only
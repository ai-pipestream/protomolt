SELECT u.object_id FROM archive_object_uploads u WHERE u.updated_at<=$1
AND (u.state NOT IN ($2 /*, ... */) OR u.lease_until<=clock_timestamp())
AND NOT EXISTS (SELECT $3 FROM archive_version_object_refs r WHERE r.object_id=u.object_id)
AND NOT EXISTS (SELECT $4 FROM repository_object_references r WHERE r.object_id=u.object_id)
ORDER BY u.updated_at,u.object_id fetch first $5 rows only
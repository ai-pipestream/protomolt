SELECT r.* FROM raw_objects r
WHERE r.updated_at <= $1
  AND (r.state NOT IN ('STAGING', 'VERIFIED') OR r.lease_until <= clock_timestamp())
  AND NOT EXISTS (SELECT 1 FROM document_raw_refs d WHERE d.raw_id = r.raw_id)
ORDER BY r.updated_at, r.raw_id fetch first $2 rows only
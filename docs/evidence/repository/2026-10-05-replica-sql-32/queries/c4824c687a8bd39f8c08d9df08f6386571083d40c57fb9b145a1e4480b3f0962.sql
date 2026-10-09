SELECT r.* FROM raw_objects r
WHERE r.updated_at <= $1
  AND (r.state NOT IN ($2 /*, ... */) OR r.lease_until <= clock_timestamp())
  AND NOT EXISTS (SELECT $3 FROM document_raw_refs d WHERE d.raw_id = r.raw_id)
ORDER BY r.updated_at, r.raw_id fetch first $4 rows only
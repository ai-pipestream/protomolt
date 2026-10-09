INSERT INTO archive_object_uploads(object_id,expected_size,content_type,lease_token,lease_until,state)
VALUES ($1,$2,$3,$4,clock_timestamp()+($5 * interval '1 millisecond'),'STAGING')
SELECT object_id,lease_token,EXTRACT($2 FROM lease_until),
       expected_size,state,sha256,provider_version,etag
FROM archive_object_uploads WHERE object_id=$1 FOR UPDATE
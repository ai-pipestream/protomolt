UPDATE archive_object_uploads SET state='VERIFIED',sha256=$1,provider_version=$2,etag=$3
WHERE object_id=$4
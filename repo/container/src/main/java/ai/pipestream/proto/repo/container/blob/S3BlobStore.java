package ai.pipestream.proto.repo.container.blob;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.util.Base64;
import java.util.HexFormat;

/**
 * {@link BlobStore} over an AWS SDK v2 synchronous {@link S3Client} (S3-compatible stores:
 * SeaweedFS in the stack, AWS S3 in prod). This class and the client producers are the
 * adapter layer — the only code allowed to import {@code software.amazon.awssdk.*}.
 * Every call blocks the (virtual) calling thread for the full S3 round trip.
 */
public final class S3BlobStore implements BlobStore {

    private final S3Client client;

    /**
     * Wraps a client. Stateless beyond the reference — cheap to construct per use.
     *
     * @param client the S3 client to adapt
     */
    public S3BlobStore(S3Client client) {
        this.client = client;
    }

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        var r = client.putObject(putRequest(spec, body.length), RequestBody.fromBytes(body));
        return new PutResult(r.eTag(), r.versionId());
    }

    @Override
    public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        var r = client.putObject(putRequest(spec, contentLength),
                RequestBody.fromInputStream(body, contentLength));
        return new PutResult(r.eTag(), r.versionId());
    }

    private static PutObjectRequest putRequest(PutSpec spec, long contentLength) {
        PutObjectRequest.Builder b = PutObjectRequest.builder()
                .bucket(spec.bucket())
                .key(spec.key())
                .contentType(spec.contentType())
                .contentLength(contentLength);
        if (spec.metadata() != null && !spec.metadata().isEmpty()) {
            b.metadata(spec.metadata());
        }
        if (spec.sha256Hex() != null && !spec.sha256Hex().isEmpty()) {
            // SDK checksum trailer: the store compares the landed bytes against
            // this digest and fails the PUT on mismatch (verified write).
            b.checksumSHA256(Base64.getEncoder().encodeToString(HexFormat.of().parseHex(spec.sha256Hex())));
        }
        return b.build();
    }

    @Override
    public BatchDeleteResult deleteAll(String bucket, java.util.List<String> keys) {
        java.util.List<String> clean = keys.stream()
                .filter(k -> k != null && !k.isBlank())
                .distinct()
                .toList();
        java.util.Map<String, String> failed = new java.util.HashMap<>();
        for (int from = 0; from < clean.size(); from += 1000) {
            java.util.List<String> chunk = clean.subList(from, Math.min(from + 1000, clean.size()));
            var response = client.deleteObjects(
                    software.amazon.awssdk.services.s3.model.DeleteObjectsRequest.builder()
                            .bucket(bucket)
                            .delete(software.amazon.awssdk.services.s3.model.Delete.builder()
                                    .objects(chunk.stream()
                                            .map(k -> software.amazon.awssdk.services.s3.model.ObjectIdentifier
                                                    .builder().key(k).build())
                                            .toList())
                                    .quiet(true) // only errors come back
                                    .build())
                            .build());
            for (var err : response.errors()) {
                // NoSuchKey parity with delete(): absent keys are success.
                if (!"NoSuchKey".equals(err.code())) {
                    failed.put(err.key(), err.code());
                }
            }
        }
        return new BatchDeleteResult(java.util.Map.copyOf(failed));
    }

    @Override
    public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
        // Fail fast BEFORE any S3 call: a blank source bucket/key would make the
        // SDK send a malformed x-amz-copy-source header, which S3 rejects with a
        // generic (non-NoSuchKey) S3Exception that nothing maps — surfacing as
        // UNKNOWN instead of the not-found contract callers rely on.
        if (srcBucket == null || srcBucket.isBlank() || srcKey == null || srcKey.isBlank()) {
            throw new BlobNotFoundException("copy source not addressable: blank source "
                    + (srcBucket == null || srcBucket.isBlank() ? "bucket" : "key")
                    + " (src=s3://" + srcBucket + "/" + srcKey + ")");
        }
        try {
            client.copyObject(software.amazon.awssdk.services.s3.model.CopyObjectRequest.builder()
                    .sourceBucket(srcBucket)
                    .sourceKey(srcKey)
                    .destinationBucket(dstBucket)
                    .destinationKey(dstKey)
                    .build());
        } catch (NoSuchKeyException nsk) {
            throw new BlobNotFoundException("copy source not found: s3://" + srcBucket + "/" + srcKey, nsk);
        }
    }

    @Override
    public GetResult get(String bucket, String key, String versionId) {
        GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(bucket).key(key);
        if (versionId != null && !versionId.isEmpty()) {
            b.versionId(versionId);
        }
        try {
            ResponseBytes<GetObjectResponse> r = client.getObjectAsBytes(b.build());
            return new GetResult(r.asByteArray(), r.response().contentType(),
                    r.response().eTag(), r.response().versionId());
        } catch (NoSuchKeyException nsk) {
            throw new BlobNotFoundException("blob not found: s3://" + bucket + "/" + key
                    + (versionId != null ? "@" + versionId : ""), nsk);
        }
    }

    @Override
    public boolean delete(String bucket, String key) {
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public java.util.List<ListedObject> list(String bucket, String prefix) {
        java.util.List<ListedObject> out = new java.util.ArrayList<>();
        var req = software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder().bucket(bucket);
        if (prefix != null && !prefix.isBlank()) {
            req.prefix(prefix);
        }
        String token = null;
        do {
            var response = client.listObjectsV2(req.continuationToken(token).build());
            for (var o : response.contents()) {
                long ms = o.lastModified() != null ? o.lastModified().toEpochMilli() : 0L;
                long size = o.size() != null ? o.size() : 0L;
                out.add(new ListedObject(o.key(), size, ms));
            }
            token = Boolean.TRUE.equals(response.isTruncated()) ? response.nextContinuationToken() : null;
        } while (token != null);
        return out;
    }

    @Override
    public void headBucket(String bucket) {
        client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
    }

    @Override
    public void headObject(String bucket, String key) {
        try {
            client.headObject(software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
        } catch (NoSuchKeyException nsk) {
            throw new BlobNotFoundException("blob not found: s3://" + bucket + "/" + key, nsk);
        }
    }
}

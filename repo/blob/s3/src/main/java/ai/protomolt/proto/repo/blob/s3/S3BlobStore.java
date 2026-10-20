package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.stream.Gatherers;

/**
 * {@link BlobStore} over an AWS SDK v2 synchronous {@link S3Client} (S3-compatible stores:
 * SeaweedFS in the stack, AWS S3 in prod). This class and the client producers are the
 * adapter layer — the only code allowed to import {@code software.amazon.awssdk.*}.
 * Every call blocks the (virtual) calling thread for the full S3 round trip.
 */
public final class S3BlobStore implements BlobStore {

    /** S3's DeleteObjects accepts at most 1000 keys per request. */
    private static final int DELETE_BATCH = 1000;

    private final S3Client client;
    private final boolean conditionalWritesEnabled;

    /**
     * Wraps a client. Stateless beyond the reference — cheap to construct per use.
     *
     * @param client the S3 client to adapt
     */
    public S3BlobStore(S3Client client) {
        this(client, false);
    }

    /** Enables conditional operations only for an endpoint separately qualified for atomic S3 preconditions. */
    public S3BlobStore(S3Client client, boolean conditionalWritesEnabled) {
        this.client = java.util.Objects.requireNonNull(client, "client");
        this.conditionalWritesEnabled = conditionalWritesEnabled;
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

    @Override
    public InputStream openStream(String bucket, String key, long offset, long length) {
        if (offset < 0) throw new IllegalArgumentException("Read offset must not be negative");
        var request = GetObjectRequest.builder().bucket(bucket).key(key);
        if (length == 0) return InputStream.nullInputStream();
        if (length > 0) request.range("bytes=" + offset + "-" + Math.addExact(offset, length - 1));
        else if (offset > 0) request.range("bytes=" + offset + "-");
        final ResponseInputStream<GetObjectResponse> stream;
        try {
            stream = client.getObject(request.build());
        } catch (NoSuchKeyException nsk) {
            throw new BlobNotFoundException("blob not found: s3://" + bucket + "/" + key, nsk);
        }
        return new java.io.FilterInputStream(stream) {
            private boolean ended;
            @Override public int read() throws IOException {
                int value = super.read();
                if (value < 0) ended = true;
                return value;
            }
            @Override public int read(byte[] buffer, int offset, int length) throws IOException {
                int count = super.read(buffer, offset, length);
                if (count < 0) ended = true;
                return count;
            }
            @Override public void close() throws IOException {
                // Closing an unfinished SDK stream drains the rest of the body; abandon it instead.
                if (!ended) stream.abort();
                super.close();
            }
        };
    }

    @Override
    public GetResult getForUpdate(String bucket, String key) {
        requireConditionalSupport();
        // Keep bytes and the ETag from one authoritative GET. Do not use getObjectAsBytes:
        // an existing object may be arbitrarily larger than this bounded API allows.
        try (ResponseInputStream<GetObjectResponse> stream = client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build())) {
            try {
                GetObjectResponse response = stream.response();
                String tag = strongBackingEtag(response.eTag());
                Long length = response.contentLength();
                if (length != null && length > MAX_CONDITIONAL_BYTES) {
                    throw new IllegalArgumentException("conditional object exceeds 9 MiB");
                }
                byte[] data = stream.readNBytes(MAX_CONDITIONAL_BYTES + 1);
                if (data.length > MAX_CONDITIONAL_BYTES) {
                    throw new IllegalArgumentException("conditional object exceeds 9 MiB");
                }
                if (length != null && (length < 0 || data.length != length)) {
                    throw new IllegalStateException("conditional object length does not match S3 metadata");
                }
                return new GetResult(data, response.contentType(), tag, response.versionId());
            } catch (RuntimeException | IOException failure) {
                stream.abort(); // Closing alone may drain an unread oversized body.
                throw failure;
            }
        } catch (NoSuchKeyException nsk) {
            throw new BlobNotFoundException("blob not found: s3://" + bucket + "/" + key, nsk);
        } catch (IOException io) {
            throw new UncheckedIOException("conditional object read failed", io);
        }
    }

    @Override
    public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
        requireConditionalSupport();
        java.util.Objects.requireNonNull(spec);
        java.util.Objects.requireNonNull(body);
        java.util.Objects.requireNonNull(condition);
        if (body.length > MAX_CONDITIONAL_BYTES) {
            throw new IllegalArgumentException("conditional object exceeds 9 MiB");
        }
        PutObjectRequest.Builder request = putRequest(spec, body.length).toBuilder();
        if (condition.ifAbsent()) request.ifNoneMatch("*");
        else request.ifMatch(BlobStore.requireStrongEtag(condition.expectedEtag()));
        try {
            var response = client.putObject(request.build(), RequestBody.fromBytes(body));
            return new PutResult(strongBackingEtag(response.eTag()), response.versionId());
        } catch (S3Exception conflict) {
            if (conflict.statusCode() == 409 || conflict.statusCode() == 412) {
                throw new BlobConflictException("conditional object write conflicted", conflict);
            }
            throw conflict;
        }
    }

    private static String strongBackingEtag(String tag) {
        try {
            return BlobStore.requireStrongEtag(tag);
        } catch (IllegalArgumentException incompatible) {
            throw new UnsupportedOperationException("backing store did not return one strong ETag", incompatible);
        }
    }

    private void requireConditionalSupport() {
        if (!conditionalWritesEnabled) {
            throw new UnsupportedOperationException("S3 conditional operations require a qualified endpoint");
        }
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
        java.util.List<java.util.List<String>> batches = keys.stream()
                .filter(k -> k != null && !k.isBlank())
                .distinct()
                .gather(Gatherers.windowFixed(DELETE_BATCH))
                .toList();
        java.util.Map<String, String> failed = new java.util.HashMap<>();
        for (java.util.List<String> chunk : batches) {
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
        return S3ObjectReads.read(client, bucket, key, versionId, null);
    }

    @Override
    public GetResult getBounded(String bucket, String key, String versionId, int maxBytes) {
        return S3ObjectReads.read(client, bucket, key, versionId, maxBytes);
    }

    @Override
    public boolean delete(String bucket, String key) {
        // S3's DeleteObject is success-even-when-absent by design, so the
        // port's "false when the key did not exist" contract needs an
        // existence probe first — one extra HEAD round trip per delete.
        try {
            client.headObject(software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
        } catch (NoSuchKeyException e) {
            return false;
        }
        client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        return true;
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
    public long objectLength(String bucket, String key) {
        try {
            var response = client.headObject(software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            var length = response.contentLength();
            if (length == null || length < 0) throw new IllegalStateException("S3 reported no object length");
            return length;
        } catch (NoSuchKeyException nsk) {
            throw new BlobNotFoundException("blob not found: s3://" + bucket + "/" + key, nsk);
        }
    }

    /** S3 multipart parts are at least 5 MiB, except the last. */
    static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    @Override
    public PutResult putMultipart(PutSpec spec, InputStream body, int partSize) {
        if (partSize < MIN_PART_SIZE) throw new IllegalArgumentException("S3 multipart parts are at least 5 MiB");
        if (spec.sha256Hex() != null) throw new IllegalArgumentException("A multipart write takes no whole-object checksum");
        var create = software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest.builder()
                .bucket(spec.bucket()).key(spec.key()).contentType(spec.contentType())
                // Each part carries its own SHA-256, which the store checks on arrival.
                .checksumAlgorithm(software.amazon.awssdk.services.s3.model.ChecksumAlgorithm.SHA256);
        if (spec.metadata() != null && !spec.metadata().isEmpty()) create.metadata(spec.metadata());
        String uploadId = client.createMultipartUpload(create.build()).uploadId();
        try {
            var parts = new java.util.ArrayList<software.amazon.awssdk.services.s3.model.CompletedPart>();
            byte[] buffer = new byte[partSize];
            int number = 1;
            while (true) {
                int filled = body.readNBytes(buffer, 0, partSize);
                // An empty object still needs one (empty) part; otherwise stop at the end.
                if (filled == 0 && number > 1) break;
                if (Thread.currentThread().isInterrupted()) throw new java.io.InterruptedIOException("multipart write interrupted");
                var part = client.uploadPart(software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
                        .bucket(spec.bucket()).key(spec.key()).uploadId(uploadId).partNumber(number)
                        .contentLength((long) filled)
                        .checksumAlgorithm(software.amazon.awssdk.services.s3.model.ChecksumAlgorithm.SHA256).build(),
                        RequestBody.fromBytes(java.util.Arrays.copyOf(buffer, filled)));
                parts.add(software.amazon.awssdk.services.s3.model.CompletedPart.builder().partNumber(number)
                        .eTag(part.eTag()).checksumSHA256(part.checksumSHA256()).build());
                number++;
                if (filled < partSize) break;
            }
            var done = client.completeMultipartUpload(software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
                    .builder().bucket(spec.bucket()).key(spec.key()).uploadId(uploadId)
                    .multipartUpload(software.amazon.awssdk.services.s3.model.CompletedMultipartUpload.builder()
                            .parts(parts).build()).build());
            return new PutResult(done.eTag(), done.versionId());
        } catch (IOException failure) {
            abortMultipart(spec, uploadId, failure);
            throw new UncheckedIOException("multipart write body failed", failure);
        } catch (RuntimeException | Error failure) {
            abortMultipart(spec, uploadId, failure);
            throw failure;
        }
    }

    private void abortMultipart(PutSpec spec, String uploadId, Throwable failure) {
        try {
            client.abortMultipartUpload(software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest.builder()
                    .bucket(spec.bucket()).key(spec.key()).uploadId(uploadId).build());
        } catch (RuntimeException abort) {
            failure.addSuppressed(abort);
        }
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

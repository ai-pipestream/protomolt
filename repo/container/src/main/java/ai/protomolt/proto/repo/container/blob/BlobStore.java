package ai.protomolt.proto.repo.container.blob;

import java.io.InputStream;
import java.util.Map;

/**
 * Object-storage port: the ONLY surface business code and transport handlers use to touch
 * blob storage. Implementations adapt a concrete provider (today S3/SeaweedFS via
 * {@link S3BlobStore}); callers never see provider SDK types, so a future backend
 * (Azure Blob, GCS, a packing/columnar decorator) is one new implementation, not a sweep
 * through the handlers.
 *
 * <p>All operations BLOCK the calling thread — callers run on virtual threads, so a
 * blocked S3 round trip parks the virtual thread instead of tying up a carrier thread.
 * Failures surface as runtime exceptions from the provider adapter.
 *
 * <p>Scope is deliberately object operations only. Bucket lifecycle (create) is admin-plane
 * work and stays on the raw provider client at its admin call sites;
 * {@link #headBucket(String)} exists because drive-status probes are part of normal reads.
 *
 * <p>Checksums cross this interface as lowercase hex SHA-256 (the platform's canonical form);
 * implementations convert to whatever their provider's integrity mechanism wants.
 */
public interface BlobStore {

    /**
     * What to write and where — everything about a put except the body bytes.
     *
     * @param bucket the target bucket
     * @param key the object key
     * @param contentType the MIME type stored with the object
     * @param metadata provider metadata entries stored with the object; may be {@code null}
     * @param sha256Hex lowercase hex SHA-256 of the body for a verified write (the store
     *        rejects the put if the landed bytes do not match), or {@code null} to skip
     *        integrity verification
     */
    record PutSpec(String bucket, String key, String contentType, Map<String, String> metadata, String sha256Hex) {
    }

    /**
     * Coordinates of a written object.
     *
     * @param eTag the provider's entity tag, or {@code null} when not supplied
     * @param versionId the provider's version id, or {@code null} when versioning is off
     */
    record PutResult(String eTag, String versionId) {
    }

    /**
     * A fetched object.
     *
     * @param data the object bytes
     * @param contentType the stored MIME type, or {@code null} when the provider has none
     * @param eTag the provider's entity tag, or {@code null}
     * @param versionId the fetched version id, or {@code null}
     */
    record GetResult(byte[] data, String contentType, String eTag, String versionId) {
    }

    /** Failure completing a {@link #get}: the key (or requested version) does not exist. */
    class BlobNotFoundException extends RuntimeException {
        /**
         * Creates the failure.
         *
         * @param message which bucket/key/version was missing
         * @param cause the provider's underlying not-found error
         */
        public BlobNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Creates the failure with no provider cause — for callers that detect
         * an unaddressable blob (e.g. a blank bucket/key) before any provider
         * call is made.
         *
         * @param message which bucket/key/version was missing or unaddressable
         */
        public BlobNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Writes an in-memory body.
     *
     * @param spec what and where to write
     * @param body the object bytes
     * @return the written object's coordinates
     */
    PutResult put(PutSpec spec, byte[] body);

    /**
     * Writes a streamed body of known length (large uploads that must not buffer in memory).
     *
     * @param spec what and where to write
     * @param body the object bytes as a stream
     * @param contentLength the exact stream length in bytes
     * @return the written object's coordinates
     */
    PutResult put(PutSpec spec, InputStream body, long contentLength);

    /**
     * Fetches the current version of an object.
     *
     * @param bucket the bucket
     * @param key the object key
     * @return the object
     * @throws BlobNotFoundException when the key does not exist
     */
    default GetResult get(String bucket, String key) {
        return get(bucket, key, null);
    }

    /**
     * Fetches an object, optionally a specific version.
     *
     * @param bucket the bucket
     * @param key the object key
     * @param versionId the version to fetch, or {@code null} for the current one
     * @return the object
     * @throws BlobNotFoundException when the key/version does not exist
     */
    GetResult get(String bucket, String key, String versionId);

    /**
     * Server-side copy of one object within this store (the four-part partial
     * save's copy-forward: unwritten parts move between addresses without
     * their bytes ever transiting the service).
     *
     * @param srcBucket the source bucket
     * @param srcKey the source object key
     * @param dstBucket the destination bucket
     * @param dstKey the destination object key
     * @throws BlobNotFoundException when the source object does not exist
     */
    void copy(String srcBucket, String srcKey, String dstBucket, String dstKey);

    /**
     * Deletes an object if it exists.
     *
     * @param bucket the bucket
     * @param key the object key
     * @return {@code true} when an object was deleted, {@code false} when the key did not
     *         exist; throws only on real provider errors
     */
    boolean delete(String bucket, String key);

    /**
     * Outcome of a batch delete: the keys the provider REJECTED with an error.
     * Missing keys are NOT failures — S3 DeleteObjects treats delete-of-absent
     * as success, which matches the purge path's NoSuchKey-is-success rule.
     *
     * @param failedKeys keys the provider errored on (retryable), key → error code
     */
    record BatchDeleteResult(java.util.Map<String, String> failedKeys) {
        /**
         * Whether every key was deleted (or already absent).
         *
         * @return {@code true} when nothing failed
         */
        public boolean allSucceeded() {
            return failedKeys.isEmpty();
        }
    }

    /**
     * Deletes many objects in as few provider calls as possible (S3
     * {@code DeleteObjects}: up to 1000 keys per call; implementations chunk
     * larger lists). One round trip replaces N — the four-part purge path's
     * throughput primitive.
     *
     * @param bucket the bucket
     * @param keys the object keys to delete; blanks are skipped
     * @return which keys failed (empty map = full success)
     */
    BatchDeleteResult deleteAll(String bucket, java.util.List<String> keys);

    /**
     * One object seen while listing.
     *
     * @param key the object key
     * @param sizeBytes the object size in bytes
     * @param lastModifiedEpochMs the object's last-modified time as epoch millis, or
     *        {@code 0} when the provider does not report it
     */
    record ListedObject(String key, long sizeBytes, long lastModifiedEpochMs) {
    }

    /**
     * Lists every object under a key prefix (S3 {@code ListObjectsV2}, following
     * continuation tokens so the full keyspace is returned, not just the first
     * page). The storage-reconcile sweep's read side: enumerate what is actually
     * in the bucket so it can be diffed against the owning rows.
     *
     * @param bucket the bucket
     * @param prefix the key prefix to list under; empty or {@code null} lists the whole bucket
     * @return every object under the prefix
     */
    java.util.List<ListedObject> list(String bucket, String prefix);

    /**
     * Probes bucket reachability (drive-status checks).
     *
     * @param bucket the bucket to probe
     * @throws RuntimeException when the bucket is not reachable
     */
    void headBucket(String bucket);

    /**
     * Probes an object's existence WITHOUT fetching its bytes (S3
     * {@code HeadObject}) — the coherence path's is-it-really-gone re-check,
     * where a full GET of a multi-megabyte part just to prove existence would
     * be waste.
     *
     * @param bucket the bucket
     * @param key the object key
     * @throws BlobNotFoundException when the key does not exist
     */
    void headObject(String bucket, String key);
}

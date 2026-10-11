package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import ai.protomolt.proto.repo.blob.spi.ExactVersionReclaimer;
import java.util.Objects;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Exact immutable-version deletion over a borrowed client; its owner retains
 * cleanup responsibility, as with {@link S3ObjectReclaimer}.
 *
 * <p>Each call is bounded to four API calls on the injected client: namespace
 * qualification ({@code GetBucketVersioning}, ENABLED required, 501 reported
 * as UNIMPLEMENTED), a versioned HEAD for existence, a versioned DELETE as the
 * only mutation, and a versioned HEAD that must confirm absence. SDK retry
 * attempts inside one API call follow the client owner's configured retry
 * policy and are safe: the target is immutable and the delete idempotent.
 *
 * <p>Absence classifies on HTTP status, never the S3 error code — HEAD
 * carries no error body, and S3-compatible backends answer {@code NoSuchKey}
 * rather than {@code NoSuchVersion} for a deleted version id — except for the
 * DELETE 404, whose body is read: {@code NoSuchVersion} means a racing
 * same-target deleter already removed the version, so the call continues to
 * verification and reports {@code ABSENT}, never {@code DELETED}, because this
 * call acknowledged no deletion. Nothing here ever substitutes the current
 * version or widens to the whole key.
 */
public final class S3ExactVersionReclaimer implements ExactVersionReclaimer {
    private final S3Client client;

    public S3ExactVersionReclaimer(S3Client client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override public ExactVersionOutcome reclaim(String bucket, String key, String versionId) {
        if (bucket == null || bucket.isBlank())
            throw new IllegalArgumentException("Exact bucket is required for exact-version reclamation");
        if (key == null || key.isEmpty())
            throw new IllegalArgumentException("Exact key is required for exact-version reclamation");
        if (versionId == null || versionId.isBlank() || versionId.equals("null"))
            throw new IllegalArgumentException("Exact immutable version id is required for exact-version reclamation"
                    + " (the current version is never substituted)");

        ExactVersionOutcome blocked = qualify(bucket, key, versionId);
        if (blocked != null) return blocked;
        HeadState existence = head(bucket, key, versionId, true);
        if (existence != HeadState.PRESENT)
            return existence == HeadState.ABSENT ? ExactVersionOutcome.ABSENT : ExactVersionOutcome.UNCERTAIN;
        DeleteResult deleted = delete(bucket, key, versionId);
        if (deleted == DeleteResult.UNCERTAIN) return ExactVersionOutcome.UNCERTAIN;
        return head(bucket, key, versionId, false) == HeadState.ABSENT
                ? (deleted == DeleteResult.ACKNOWLEDGED ? ExactVersionOutcome.DELETED : ExactVersionOutcome.ABSENT)
                : ExactVersionOutcome.UNCERTAIN;
    }

    /**
     * Namespace qualification at call time, never assumed from endpoint brand.
     * Returns null when the bucket reports ENABLED versioning; UNCERTAIN when the
     * state could not be read (nothing was applied); throws FAILED_PRECONDITION
     * when versioning is SUSPENDED or never enabled, NOT_FOUND when the bucket is
     * absent, UNIMPLEMENTED when the backend reports (501) it lacks the semantic,
     * and the shared 400/401/403 refusals.
     */
    private ExactVersionOutcome qualify(String bucket, String key, String versionId) {
        final BucketVersioningStatus status;
        try {
            status = client.getBucketVersioning(b -> b.bucket(bucket)).status();
        } catch (S3Exception failure) {
            switch (failure.statusCode()) {
                case 400 -> throw refused(BlobStoreException.Code.INVALID_ARGUMENT, bucket, key, versionId, failure);
                case 401 -> throw refused(BlobStoreException.Code.UNAUTHENTICATED, bucket, key, versionId, failure);
                case 403 -> throw refused(BlobStoreException.Code.PERMISSION_DENIED, bucket, key, versionId, failure);
                case 404 -> throw refused(BlobStoreException.Code.NOT_FOUND, bucket, key, versionId, failure);
                case 501 -> throw refused(BlobStoreException.Code.UNIMPLEMENTED, bucket, key, versionId, failure);
                default -> { return ExactVersionOutcome.UNCERTAIN; }
            }
        } catch (SdkClientException failure) {
            restoreInterrupt(failure);
            return ExactVersionOutcome.UNCERTAIN;
        }
        if (status == BucketVersioningStatus.ENABLED) return null;
        throw new BlobStoreException(BlobStoreException.Code.FAILED_PRECONDITION,
                "Exact-version reclamation requires versioning ENABLED on bucket " + bucket
                        + " for target " + bucket + "/" + key + "@" + versionId
                        + " (reported " + (status == null ? "never enabled" : status) + ")", null);
    }

    /**
     * Versioned HEAD classified on HTTP status: 200 present, 405 a delete-marker
     * version (still an exact target), 404 verified absence. With
     * {@code refuseExplicit}, 400/401/403 throw and every other failure is
     * UNCERTAIN; with it false, every non-404 answer is UNCERTAIN, which is the
     * honest classification for post-delete verification.
     */
    private HeadState head(String bucket, String key, String versionId, boolean refuseExplicit) {
        try {
            client.headObject(b -> b.bucket(bucket).key(key).versionId(versionId));
            return HeadState.PRESENT;
        } catch (S3Exception failure) {
            if (!refuseExplicit && (failure.statusCode() == 400 || failure.statusCode() == 401
                    || failure.statusCode() == 403)) return HeadState.UNCERTAIN;
            return switch (failure.statusCode()) {
                case 404 -> HeadState.ABSENT;
                case 405 -> HeadState.PRESENT;
                case 400 -> throw refused(BlobStoreException.Code.INVALID_ARGUMENT, bucket, key, versionId, failure);
                case 401 -> throw refused(BlobStoreException.Code.UNAUTHENTICATED, bucket, key, versionId, failure);
                case 403 -> throw refused(BlobStoreException.Code.PERMISSION_DENIED, bucket, key, versionId, failure);
                default -> HeadState.UNCERTAIN;
            };
        } catch (SdkClientException failure) {
            restoreInterrupt(failure);
            return HeadState.UNCERTAIN;
        }
    }

    /**
     * The only mutation, addressed to the exact version id. Returns ACKNOWLEDGED
     * when the provider accepted the delete, ALREADY_ABSENT when a 404
     * {@code NoSuchVersion} body reports a racing same-target deleter already
     * removed the version, and UNCERTAIN when the request may or may not have
     * been applied (retry of the same immutable target is safe). A 404 under any
     * other error code, or without error details, throws NOT_FOUND; so do the
     * shared 400/401/403 refusals.
     */
    private DeleteResult delete(String bucket, String key, String versionId) {
        try {
            client.deleteObject(b -> b.bucket(bucket).key(key).versionId(versionId));
            return DeleteResult.ACKNOWLEDGED;
        } catch (S3Exception failure) {
            switch (failure.statusCode()) {
                case 400 -> throw refused(BlobStoreException.Code.INVALID_ARGUMENT, bucket, key, versionId, failure);
                case 401 -> throw refused(BlobStoreException.Code.UNAUTHENTICATED, bucket, key, versionId, failure);
                case 403 -> throw refused(BlobStoreException.Code.PERMISSION_DENIED, bucket, key, versionId, failure);
                case 404 -> {
                    String error = failure.awsErrorDetails() == null ? "" : failure.awsErrorDetails().errorCode();
                    if ("NoSuchVersion".equals(error)) return DeleteResult.ALREADY_ABSENT;
                    throw refused(BlobStoreException.Code.NOT_FOUND, bucket, key, versionId, failure);
                }
                default -> { return DeleteResult.UNCERTAIN; }
            }
        } catch (SdkClientException failure) {
            restoreInterrupt(failure);
            return DeleteResult.UNCERTAIN;
        }
    }

    /** Public messages carry the target coordinate and code only: never endpoints, never credentials. */
    private static BlobStoreException refused(BlobStoreException.Code code, String bucket, String key,
            String versionId, Throwable cause) {
        return new BlobStoreException(code, "Exact-version reclamation of " + bucket + "/" + key + "@" + versionId
                + " was refused with " + code, cause);
    }

    /**
     * A destructive request cannot prove non-application on interruption, so every
     * client-side failure stays UNCERTAIN and an interrupted thread keeps its flag
     * for the caller; there is deliberately no CANCELLED mapping here.
     */
    private static void restoreInterrupt(SdkClientException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private enum HeadState { PRESENT, ABSENT, UNCERTAIN }

    /** What the DELETE proved: accepted, already removed by a racing same-target deleter, or inconclusive. */
    private enum DeleteResult { ACKNOWLEDGED, ALREADY_ABSENT, UNCERTAIN }
}

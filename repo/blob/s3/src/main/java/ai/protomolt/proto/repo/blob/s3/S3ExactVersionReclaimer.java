package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome;
import ai.protomolt.proto.repo.blob.spi.ExactVersionOutcome.Phase;
import ai.protomolt.proto.repo.blob.spi.ExactVersionReclaimer;
import java.util.Objects;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Exact immutable-version deletion over a borrowed, explicitly configured client. */
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

        Phase phase = Phase.QUALIFICATION;
        try {
            if (client.getBucketVersioning(b -> b.bucket(bucket)).status() != BucketVersioningStatus.ENABLED)
                throw new BlobStoreException(BlobStoreException.Code.FAILED_PRECONDITION,
                        "Exact-version reclamation requires versioning ENABLED on bucket " + bucket, null);
            phase = Phase.EXISTENCE;
            if (!present(bucket, key, versionId)) return ExactVersionOutcome.ABSENT;
            phase = Phase.DELETE;
            boolean acknowledged = true;
            try {
                client.deleteObject(b -> b.bucket(bucket).key(key).versionId(versionId));
            } catch (S3Exception failure) {
                if (failure.statusCode() != 404 || failure.awsErrorDetails() == null
                        || !"NoSuchVersion".equals(failure.awsErrorDetails().errorCode())) throw failure;
                acknowledged = false;
            }
            phase = Phase.VERIFICATION;
            if (!present(bucket, key, versionId))
                return acknowledged ? ExactVersionOutcome.DELETED : ExactVersionOutcome.ABSENT;
            return ExactVersionOutcome.uncertain(phase, diagnostic(BlobStoreException.Code.ABORTED, phase,
                    new IllegalStateException("Exact version remains present after deletion")));
        } catch (S3Exception failure) {
            var code = switch (failure.statusCode()) {
                case 400 -> BlobStoreException.Code.INVALID_ARGUMENT;
                case 401 -> BlobStoreException.Code.UNAUTHENTICATED;
                case 403 -> BlobStoreException.Code.PERMISSION_DENIED;
                case 404 -> BlobStoreException.Code.NOT_FOUND;
                case 501 -> BlobStoreException.Code.UNIMPLEMENTED;
                default -> failure.statusCode() >= 500 ? BlobStoreException.Code.UNAVAILABLE
                        : BlobStoreException.Code.UNKNOWN;
            };
            var diagnostic = diagnostic(code, phase, failure);
            // A failed verification cannot undo an acknowledged delete. Preserve
            // both uncertainty about completion and the specific failure to verify.
            if (phase == Phase.VERIFICATION || code == BlobStoreException.Code.UNAVAILABLE
                    || code == BlobStoreException.Code.UNKNOWN)
                return ExactVersionOutcome.uncertain(phase, diagnostic);
            throw diagnostic;
        } catch (SdkClientException failure) {
            boolean interrupted = Thread.currentThread().isInterrupted();
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause instanceof InterruptedException) { interrupted = true; break; }
            }
            if (interrupted) Thread.currentThread().interrupt();
            var code = interrupted ? BlobStoreException.Code.CANCELLED
                    : failure instanceof software.amazon.awssdk.core.exception.ApiCallTimeoutException
                            || failure instanceof software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
                            ? BlobStoreException.Code.DEADLINE_EXCEEDED : BlobStoreException.Code.UNAVAILABLE;
            return ExactVersionOutcome.uncertain(phase, diagnostic(code, phase, failure));
        }
    }

    /** HEAD has no error body; only an exact-version 404 establishes absence. */
    private boolean present(String bucket, String key, String versionId) {
        try {
            client.headObject(b -> b.bucket(bucket).key(key).versionId(versionId));
            return true;
        } catch (S3Exception failure) {
            if (failure.statusCode() == 404) return false;
            if (failure.statusCode() == 405) return true; // Exact delete-marker version.
            throw failure;
        }
    }

    private static BlobStoreException diagnostic(BlobStoreException.Code code, Phase phase, Throwable cause) {
        return new BlobStoreException(code, "Exact-version reclamation failed during " + phase + " with " + code, cause);
    }
}

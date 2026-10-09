package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import java.io.IOException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Shared provider identity and failure semantics for ordinary and bounded reads. */
final class S3ObjectReads {
    private S3ObjectReads() {}

    static BlobStore.GetResult read(S3Client client, String bucket, String key, String versionId, Integer maxBytes) {
        if (maxBytes != null && maxBytes < 0) throw new IllegalArgumentException("Read limit must not be negative");
        var request = GetObjectRequest.builder().bucket(bucket).key(key);
        if (versionId != null && !versionId.isEmpty()) request.versionId(versionId);
        try {
            if (maxBytes == null) {
                var result = client.getObjectAsBytes(request.build());
                return new BlobStore.GetResult(result.asByteArray(), result.response().contentType(),
                        result.response().eTag(), result.response().versionId());
            }
            try (var stream = client.getObject(request.build())) {
                try {
                    var response = stream.response();
                    Long length = response.contentLength();
                    if (length != null && length > maxBytes) throw new BlobStore.BlobReadLimitException(maxBytes);
                    if (length != null && length < 0) throw corruptLength();
                    byte[] data = stream.readNBytes(maxBytes);
                    // One extra byte distinguishes a complete object from a silently truncated prefix.
                    if (stream.read() != -1) throw new BlobStore.BlobReadLimitException(maxBytes);
                    if (length != null && length != data.length) throw corruptLength();
                    return new BlobStore.GetResult(data, response.contentType(), response.eTag(), response.versionId());
                } catch (IOException | RuntimeException | Error failure) {
                    try { stream.abort(); }
                    catch (RuntimeException | Error abortFailure) { if (abortFailure != failure) failure.addSuppressed(abortFailure); }
                    throw failure;
                }
            }
        } catch (NoSuchKeyException missing) {
            throw new BlobStore.BlobNotFoundException("blob not found: s3://" + bucket + "/" + key
                    + (versionId != null ? "@" + versionId : ""), missing);
        } catch (S3Exception failure) {
            if (failure.statusCode() == 404 && failure.awsErrorDetails() != null
                    && "NoSuchVersion".equals(failure.awsErrorDetails().errorCode()))
                throw new BlobStore.BlobNotFoundException("blob version not found: s3://" + bucket + "/" + key + "@" + versionId, failure);
            String error = failure.awsErrorDetails() == null ? "" : failure.awsErrorDetails().errorCode();
            var code = "RequestTimeout".equals(error) ? BlobStoreException.Code.DEADLINE_EXCEEDED : switch (failure.statusCode()) {
                case 400 -> BlobStoreException.Code.INVALID_ARGUMENT;
                case 401 -> BlobStoreException.Code.UNAUTHENTICATED;
                case 403 -> BlobStoreException.Code.PERMISSION_DENIED;
                case 301, 307, 404 -> BlobStoreException.Code.FAILED_PRECONDITION;
                case 408, 504 -> BlobStoreException.Code.DEADLINE_EXCEEDED;
                case 429 -> BlobStoreException.Code.RESOURCE_EXHAUSTED;
                default -> failure.statusCode() >= 500 ? BlobStoreException.Code.UNAVAILABLE : BlobStoreException.Code.UNKNOWN;
            };
            throw new BlobStoreException(code, "Object provider rejected the read", failure);
        } catch (software.amazon.awssdk.core.exception.ApiCallTimeoutException
                | software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException timeout) {
            throw new BlobStoreException(BlobStoreException.Code.DEADLINE_EXCEEDED, "Object provider read timed out", timeout);
        } catch (software.amazon.awssdk.core.exception.SdkClientException | IOException failure) {
            var code = Thread.currentThread().isInterrupted() ? BlobStoreException.Code.CANCELLED : BlobStoreException.Code.UNAVAILABLE;
            if (code != BlobStoreException.Code.CANCELLED) {
                for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                    if (cause instanceof java.net.SocketTimeoutException) { code = BlobStoreException.Code.DEADLINE_EXCEEDED; break; }
                }
            }
            throw new BlobStoreException(code, "Object provider read could not complete", failure);
        }
    }

    private static BlobStoreException corruptLength() {
        return new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Object length disagrees with provider metadata", null);
    }
}

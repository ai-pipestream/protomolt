package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Arrays;
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
                    byte[] data = readDeclared(stream, maxBytes, length);
                    // One extra byte distinguishes a complete object from a silently truncated prefix. A body that
                    // runs past a declared length is drained to the caller's limit before it is classified: past the
                    // limit it is an oversized object, within it the provider's framing disagrees with itself.
                    if (stream.read() != -1) {
                        if (length == null || exceeds(stream, maxBytes - data.length - 1)) throw new BlobStore.BlobReadLimitException(maxBytes);
                        throw corruptLength();
                    }
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
                    if (cause instanceof SocketTimeoutException) { code = BlobStoreException.Code.DEADLINE_EXCEEDED; break; }
                }
            }
            throw new BlobStoreException(code, "Object provider read could not complete", failure);
        }
    }

    /**
     * Reads the streamed body once, up to the declared length when the provider declared one and
     * otherwise up to the caller's limit. A body that ends before its declared length, whether the
     * HTTP client raises the early end or returns end-of-stream, is reported as UNAVAILABLE with
     * the byte counts: on the wire it is identical to a connection dropped mid-body, so it is
     * never promoted to DATA_LOSS. The SDK does not retry a streamed body, so the counts describe
     * the only read that happened.
     */
    private static byte[] readDeclared(InputStream stream, int limit, Long declared) throws IOException {
        int expected = declared == null ? limit : (int) (long) declared;
        byte[] data = new byte[expected];
        int received = 0;
        while (received < expected) {
            int count;
            try { count = stream.read(data, received, expected - received); }
            catch (IOException ended) {
                // A stalled read and an interrupted thread keep their own classifications (DEADLINE_EXCEEDED, CANCELLED).
                if (Thread.currentThread().isInterrupted() || timedOut(ended)) throw ended;
                throw shortBody(received, declared, ended);
            }
            if (count < 0) break;
            received += count;
        }
        if (declared != null && received < declared) throw shortBody(received, declared, null);
        return received == data.length ? data : Arrays.copyOf(data, received);
    }

    /** Drains up to {@code remaining} further bytes without keeping them; true when the body continues past them. */
    private static boolean exceeds(InputStream stream, int remaining) throws IOException {
        byte[] drain = new byte[8192];
        while (remaining > 0) {
            int count = stream.read(drain, 0, Math.min(drain.length, remaining));
            if (count < 0) return false;
            remaining -= count;
        }
        return stream.read() != -1;
    }

    private static boolean timedOut(IOException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof SocketTimeoutException) return true;
        return false;
    }

    private static BlobStoreException shortBody(int received, Long declared, IOException cause) {
        String declaredText = declared == null ? "an undeclared length" : declared + " declared bytes";
        return new BlobStoreException(BlobStoreException.Code.UNAVAILABLE, "Object provider ended the response body early: received "
                + received + " of " + declaredText + " in the single body read the SDK does not retry", cause);
    }

    private static BlobStoreException corruptLength() {
        return new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Object length disagrees with provider metadata", null);
    }
}

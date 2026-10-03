package ai.protomolt.proto.repo.blob.grpc;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import ai.protomolt.proto.validate.ProtoValidator;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Objects;

/**
 * The dogfood {@link BlobStore}: the object-storage port served by a
 * repo-service's own {@code DocumentService} blob RPCs. Any protomolt
 * consumer that speaks the {@link BlobStore} port can therefore use a
 * repo-service as its byte store instead of talking S3 directly — one
 * implementation, no provider SDK on the consumer's classpath.
 *
 * <p>Coordinate mapping: the port's {@code bucket} parameter is IGNORED.
 * The repo API addresses objects by drive, and the drive resolves to its
 * bucket server-side, so every operation here lands on the single default
 * drive this store was constructed with.
 *
 * <p>Size bound: unary gRPC carries the whole payload in one message, so
 * every byte of a put/get transits BOTH the channel and this process in
 * memory (and the streaming {@link #put(PutSpec, InputStream, long)} variant
 * accepts at most 9 MiB and verifies the declared length before making an RPC). Huge payloads
 * belong on the repo-service's streaming HTTP upload route
 * ({@code POST /v1/documents:upload}), which never buffers.
 *
 * <p>Deliberate gaps in the repo blob API:
 * <ul>
 *   <li>{@link #headObject} is a full {@code GetBlob} whose bytes are
 *   discarded — there is no cheap existence probe across the v1 API, and
 *   inventing one here would just hide the same fetch;</li>
 *   <li>{@link #copy} is a client-side get+put through the stub — the bytes
 *   transit this process; there is no server-side copy across the API yet;</li>
 *   <li>{@link #list}, {@link #deleteAll} and {@link #headBucket} throw
 *   {@link UnsupportedOperationException} — enumerate/purge/admin operations
 *   have no repo-API counterpart and stay S3-only.</li>
 * </ul>
 */
public final class RemoteBlobStore implements BlobStore {

    /** Payload bound leaves room for protobuf framing within the service's 10 MiB RPC limit. */
    public static final int MAX_UNARY_BYTES = 9 * 1024 * 1024;
    private static final int MAX_RPC_BYTES = 10 * 1024 * 1024;

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static final String UNSUPPORTED =
            "not supported by the repo-backed store: the repo blob API has no operation for this";

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String driveName;
    private final long timeoutNanos;

    /**
     * @param documents the blocking stub of the repo-service to store through
     * @param driveName the drive every operation addresses (resolves to its
     *        backing bucket on the server)
     */
    public RemoteBlobStore(DocumentServiceGrpc.DocumentServiceBlockingStub documents, String driveName) {
        this(documents, driveName, Duration.ofSeconds(30));
    }

    /**
     * Borrows a stub and applies a fresh timeout to each RPC. A shorter deadline
     * already on the stub or current gRPC context still wins. Timeout does not
     * establish whether a remote write committed; this adapter does not retry it.
     */
    public RemoteBlobStore(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
                           String driveName, Duration timeout) {
        this.documents = Objects.requireNonNull(documents, "documents");
        Objects.requireNonNull(timeout, "timeout");
        try { this.timeoutNanos = timeout.toNanos(); }
        catch (ArithmeticException overflow) { throw new IllegalArgumentException("RPC timeout is too large", overflow); }
        if (timeoutNanos <= 0) throw new IllegalArgumentException("RPC timeout must be positive");

        if (driveName == null || driveName.isBlank()) {
            throw new IllegalArgumentException("driveName cannot be null or blank");
        }
        this.driveName = driveName;
    }

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(body, "body");
        requireLength(body.length);
        if (spec.sha256Hex() != null && !spec.sha256Hex().equals(DocumentPartCodec.sha256Hex(body))) {
            throw new IllegalArgumentException("blob digest differs from body");
        }
        PutBlobRequest.Builder request = PutBlobRequest.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(spec.key())
                .setData(ByteString.copyFrom(body));
        if (spec.contentType() != null && !spec.contentType().isBlank()) {
            request.setMimeType(spec.contentType());
        }
        var outgoing = request.build();
        if (outgoing.getSerializedSize() > MAX_RPC_BYTES) {
            throw new IllegalArgumentException("blob request exceeds 10 MiB RPC limit");
        }
        PutBlobResponse response = callStub().putBlob(outgoing);
        // Verified write: the server computed the SHA-256 and made its store
        // verify the landed bytes against it, so a returned response is proof.
        return new PutResult(null, versionOf(response.getStorageRef()));
    }

    /**
     * Reads at most the declared length plus one byte, then delegates to {@link #put(PutSpec, byte[])}.
     * The whole payload sits in memory (see the class Javadoc): this variant
     * exists for port compatibility, not for large bodies.
     */
    @Override
    public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(body, "body");
        requireLength(contentLength);
        try {
            byte[] bytes = body.readNBytes((int) contentLength + 1);
            if (bytes.length != contentLength) {
                throw new IllegalArgumentException("blob body length differs from declared content length");
            }
            return put(spec, bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read blob body stream for key " + spec.key(), e);
        }
    }

    @Override
    public GetResult get(String bucket, String key, String versionId) {
        try {
            GetBlobResponse response = callStub().getBlob(GetBlobRequest.newBuilder()
                    .setStorageRef(storageRef(key, versionId))
                    .build());
            return new GetResult(response.getData().toByteArray(),
                    response.hasMimeType() ? response.getMimeType() : null, null, versionId);
        } catch (StatusRuntimeException e) {
            throw mapNotFound(e, key);
        }
    }

    @Override
    public GetResult getForUpdate(String bucket, String key) {
        ConditionalBlobKey address = conditionalKey(key);
        var request = GetBlobForUpdateRequest.newBuilder().setKey(address).build();
        if (!valid(request)) {
            throw new IllegalArgumentException("authoritative blob read request is invalid");
        }
        try {
            var response = callStub().withMaxInboundMessageSize(10 * 1024 * 1024)
                    .getBlobForUpdate(request);
            requireResponse(response);
            var version = response.getVersion();
            byte[] data = response.getData().toByteArray();
            if (!version.getKey().equals(address) || version.getSizeBytes() != data.length
                    || !version.getSha256().equals(DocumentPartCodec.sha256Hex(data))) {
                throw new IllegalStateException("authoritative blob response differs from requested bytes");
            }
            return new GetResult(data, response.hasMimeType() ? response.getMimeType() : null,
                    BlobStore.requireStrongEtag(version.getEtag()), null);
        } catch (StatusRuntimeException failure) {
            if (failure.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                throw new UnsupportedOperationException("authoritative blob read is unsupported", failure);
            }
            throw mapNotFound(failure, key);
        }
    }

    @Override
    public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(body, "body");
        Objects.requireNonNull(condition, "condition");
        if (body.length > MAX_CONDITIONAL_BYTES) {
            throw new IllegalArgumentException("conditional blob exceeds 9 MiB");
        }
        if (spec.sha256Hex() != null
                && !spec.sha256Hex().equals(DocumentPartCodec.sha256Hex(body))) {
            throw new IllegalArgumentException("conditional blob digest differs from body");
        }
        ConditionalBlobKey address = conditionalKey(spec.key());
        var request = CompareAndPutBlobRequest.newBuilder().setKey(address)
                .setData(ByteString.copyFrom(body));
        if (spec.contentType() != null && !spec.contentType().isBlank()) {
            request.setMimeType(spec.contentType());
        }
        if (condition.ifAbsent()) request.setIfAbsent(true);
        else request.setExpectedEtag(BlobStore.requireStrongEtag(condition.expectedEtag()));
        var outgoing = request.build();
        if (!valid(outgoing)) {
            throw new IllegalArgumentException("conditional blob request is invalid");
        }
        try {
            var response = callStub().withMaxInboundMessageSize(10 * 1024 * 1024)
                    .compareAndPutBlob(outgoing);
            requireResponse(response);
            var version = response.getVersion();
            if (!version.getKey().equals(address) || version.getSizeBytes() != body.length
                    || !version.getSha256().equals(DocumentPartCodec.sha256Hex(body))) {
                throw new IllegalStateException("conditional blob response differs from submitted bytes");
            }
            return new PutResult(BlobStore.requireStrongEtag(version.getEtag()), null);
        } catch (StatusRuntimeException failure) {
            if (failure.getStatus().getCode() == Status.Code.ABORTED) {
                throw new BlobConflictException("conditional blob precondition failed", failure);
            }
            if (failure.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                throw new UnsupportedOperationException("conditional blob write is unsupported", failure);
            }
            throw failure;
        }
    }

    private DocumentServiceGrpc.DocumentServiceBlockingStub callStub() {
        Deadline deadline = Deadline.after(timeoutNanos, TimeUnit.NANOSECONDS);
        Deadline supplied = documents.getCallOptions().getDeadline();
        if (supplied != null) deadline = deadline.minimum(supplied);
        return documents.withDeadline(deadline);
    }

    private static void requireLength(long length) {
        if (length < 0 || length > MAX_UNARY_BYTES) {
            throw new IllegalArgumentException("unary blob length must be between 0 and 9 MiB");
        }
    }

    private ConditionalBlobKey conditionalKey(String key) {
        var address = ConditionalBlobKey.newBuilder().setDriveName(driveName)
                .setObjectKey(Objects.requireNonNull(key, "key")).build();
        if (!VALIDATOR.validate(address).valid()) {
            throw new IllegalArgumentException("conditional blob key is invalid");
        }
        return address;
    }

    private static void requireResponse(Message value) {
        if (!valid(value)) {
            throw new IllegalStateException("conditional blob response is invalid");
        }
    }

    private static boolean valid(Message value) {
        if (!value.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(value).valid()) {
            return false;
        }
        for (var field : value.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) {
                    if (!valid((Message) nested)) return false;
                }
            } else if (!valid((Message) field.getValue())) return false;
        }
        return true;
    }

    /**
     * Existence probe implemented as a {@code GetBlob} whose bytes are
     * discarded — the v1 API has no cheaper probe (see the class Javadoc).
     */
    @Override
    public void headObject(String bucket, String key) {
        get(bucket, key, null);
    }

    @Override
    public boolean delete(String bucket, String key) {
        try {
            return callStub().deleteBlob(DeleteBlobRequest.newBuilder()
                    .setStorageRef(storageRef(key, null))
                    .build()).getDeleted();
        } catch (StatusRuntimeException e) {
            throw mapNotFound(e, key);
        }
    }

    /**
     * Client-side copy: get through the stub, then put under the destination
     * key. The bytes transit this process — there is no server-side copy
     * across the repo API yet.
     */
    @Override
    public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
        GetResult source = get(srcBucket, srcKey, null);
        put(new PutSpec(dstBucket, dstKey, source.contentType(), null, null), source.data());
    }

    @Override
    public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public List<ListedObject> list(String bucket, String prefix) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public void headBucket(String bucket) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    private FileStorageReference storageRef(String key, String versionId) {
        FileStorageReference.Builder ref = FileStorageReference.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(key);
        if (versionId != null && !versionId.isBlank()) {
            ref.setVersionId(versionId);
        }
        return ref.build();
    }

    private static RuntimeException mapNotFound(StatusRuntimeException e, String key) {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
            return new BlobNotFoundException("blob not found at key " + key, e);
        }
        return e;
    }

    private static String versionOf(FileStorageReference ref) {
        return ref.hasVersionId() && !ref.getVersionId().isBlank() ? ref.getVersionId() : null;
    }
}

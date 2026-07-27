package ai.pipestream.proto.repo.service.client;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.v1.DeleteBlobRequest;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetBlobResponse;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
 * reads its stream fully, bounded by the gRPC message limit). Huge payloads
 * belong on the repo-service's streaming HTTP upload route
 * ({@code POST /v1/documents:upload}), which never buffers.
 *
 * <p>Deliberate gaps (the repo blob API exposes get/put/delete only):
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

    private static final String UNSUPPORTED =
            "not supported by the repo-backed store: the repo blob API exposes get/put/delete only";

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String driveName;

    /**
     * @param documents the blocking stub of the repo-service to store through
     * @param driveName the drive every operation addresses (resolves to its
     *        backing bucket on the server)
     */
    public RemoteBlobStore(DocumentServiceGrpc.DocumentServiceBlockingStub documents, String driveName) {
        this.documents = Objects.requireNonNull(documents, "documents");
        if (driveName == null || driveName.isBlank()) {
            throw new IllegalArgumentException("driveName cannot be null or blank");
        }
        this.driveName = driveName;
    }

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        PutBlobRequest.Builder request = PutBlobRequest.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(spec.key())
                .setData(ByteString.copyFrom(body));
        if (spec.contentType() != null && !spec.contentType().isBlank()) {
            request.setMimeType(spec.contentType());
        }
        PutBlobResponse response = documents.putBlob(request.build());
        // Verified write: the server computed the SHA-256 and made its store
        // verify the landed bytes against it, so a returned response is proof.
        return new PutResult(null, versionOf(response.getStorageRef()));
    }

    /**
     * Reads the stream fully, then delegates to {@link #put(PutSpec, byte[])}.
     * The whole payload sits in memory (see the class Javadoc): this variant
     * exists for port compatibility, not for large bodies.
     */
    @Override
    public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        try {
            return put(spec, body.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read blob body stream for key " + spec.key(), e);
        }
    }

    @Override
    public GetResult get(String bucket, String key, String versionId) {
        try {
            GetBlobResponse response = documents.getBlob(GetBlobRequest.newBuilder()
                    .setStorageRef(storageRef(key, versionId))
                    .build());
            return new GetResult(response.getData().toByteArray(),
                    response.hasMimeType() ? response.getMimeType() : null, null, versionId);
        } catch (StatusRuntimeException e) {
            throw mapNotFound(e, key);
        }
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
            return documents.deleteBlob(DeleteBlobRequest.newBuilder()
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

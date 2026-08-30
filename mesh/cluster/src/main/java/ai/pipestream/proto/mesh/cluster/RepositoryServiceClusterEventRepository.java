package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.delegation.EncryptedRepositoryStateCodec;
import ai.pipestream.proto.delegation.RepositoryStateKeyResolver;
import ai.pipestream.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.pipestream.proto.mesh.cluster.storage.v1.ClusterEventLog;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetBlobResponse;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Stores an encrypted cluster directory event log through repository-service blob RPCs. */
public final class RepositoryServiceClusterEventRepository
        implements ClusterEventRepository {

    /** Plaintext content type authenticated inside the encrypted envelope. */
    public static final String CONTENT_TYPE =
            "application/vnd.protomolt.cluster-event-log+protobuf";

    /**
     * How long one blob round trip may take before the directory gives up on it. A
     * blocking stub with no deadline waits forever, so an unreachable or wedged
     * repository-service turned every mesh mutation into an unbounded stall that logged
     * nothing. The bound sits well inside the shortest presence TTL the directory serves,
     * so a node that cannot be persisted is reported long before its liveness lapses.
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String driveName;
    private final String objectKey;
    private final String keyReference;
    private final EncryptedRepositoryStateCodec codec;
    private final Duration timeout;

    /** Creates a repository using the default encrypted-state codec limits and timeout. */
    public RepositoryServiceClusterEventRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            RepositoryStateKeyResolver keys) {
        this(documents, driveName, objectKey, keyReference,
                new EncryptedRepositoryStateCodec(keys));
    }

    /** Creates a repository with an explicit codec for embedding and tests. */
    public RepositoryServiceClusterEventRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            EncryptedRepositoryStateCodec codec) {
        this(documents, driveName, objectKey, keyReference, codec, DEFAULT_TIMEOUT);
    }

    /** Creates a repository with an explicit per-call blob timeout. */
    public RepositoryServiceClusterEventRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            EncryptedRepositoryStateCodec codec, Duration timeout) {
        this.documents = Objects.requireNonNull(documents, "documents");
        this.driveName = requireText(driveName, "driveName", 256);
        this.objectKey = requireText(objectKey, "objectKey", 1024);
        this.keyReference = requireText(keyReference, "keyReference", 256);
        this.codec = Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        this.timeout = timeout;
    }

    /**
     * The stub to call one RPC on. The deadline is applied per call rather than once at
     * construction, because a stub-level deadline starts counting when the stub is made
     * and would expire for the life of the process shortly after startup.
     */
    private DocumentServiceGrpc.DocumentServiceBlockingStub within() {
        return documents.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static IllegalStateException unreachable(String operation,
                                                     StatusRuntimeException cause) {
        return new IllegalStateException(
                "repository service did not " + operation + " the cluster event log within "
                        + "the deadline; the mesh directory is not durable and refuses to "
                        + "report membership it cannot persist", cause);
    }

    @Override
    public Optional<StoredDirectory> load(ClusterDescriptor cluster) {
        ClusterValidation.validate(cluster);
        GetBlobResponse response;
        try {
            response = within().getBlob(GetBlobRequest.newBuilder()
                    .setStorageRef(storageReference()).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw unreachable("return", e);
            }
            throw e;
        }
        byte[] stored = response.getData().toByteArray();
        if (response.getSizeBytes() != stored.length) {
            throw corrupt("stored cluster event log size does not match repository metadata");
        }
        if (response.hasMimeType() && !EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE
                .equals(response.getMimeType())) {
            throw corrupt("stored cluster event log has an unexpected media type");
        }
        EncryptedRepositoryState envelope;
        try {
            envelope = EncryptedRepositoryState.parseFrom(stored);
        } catch (InvalidProtocolBufferException e) {
            throw corrupt("stored cluster event log envelope is not valid protobuf", e);
        }
        byte[] plaintext = codec.decrypt(envelope, CONTENT_TYPE, storageContext());
        ClusterEventLog eventLog;
        try {
            eventLog = ClusterEventLog.parseFrom(plaintext);
        } catch (InvalidProtocolBufferException e) {
            throw corrupt("decrypted cluster event log is not valid protobuf", e);
        }
        StoredDirectory directory = eventLog.hasCheckpoint()
                ? new StoredDirectory(eventLog.getCheckpoint(), eventLog.getEventsList())
                : StoredDirectory.of(eventLog.getEventsList());
        try {
            ValidationResult.validate(eventLog).throwIfInvalid();
            if (!cluster.getClusterId().equals(eventLog.getClusterId())
                    || !cluster.getFingerprint().equals(eventLog.getClusterFingerprint())) {
                throw new IllegalArgumentException("cluster identity does not match");
            }
            if (envelope.getRecordCount() != eventLog.getEventsCount()) {
                throw new IllegalArgumentException("event count does not match");
            }
            // A log written before compaction existed carries no checkpoint and begins at
            // one, which is exactly what an uncompacted StoredDirectory reports.
            ClusterValidation.validateEventLog(directory.events(), directory.firstSeq());
        } catch (RuntimeException e) {
            throw corrupt("stored cluster event log failed validation", e);
        }
        return Optional.of(directory);
    }

    @Override
    public void save(ClusterDescriptor cluster, StoredDirectory directory) {
        ClusterValidation.validate(cluster);
        ClusterValidation.validateEventLog(directory.events(), directory.firstSeq());
        ClusterEventLog.Builder builder = ClusterEventLog.newBuilder()
                .setClusterId(cluster.getClusterId())
                .setClusterFingerprint(cluster.getFingerprint())
                .addAllEvents(directory.events());
        if (directory.compacted()) {
            builder.setCheckpoint(directory.checkpoint());
        }
        ClusterEventLog eventLog = builder.build();
        ValidationResult.validate(eventLog).throwIfInvalid();
        EncryptedRepositoryState envelope = codec.encrypt(eventLog.toByteArray(),
                CONTENT_TYPE, directory.events().size(), keyReference, storageContext());
        byte[] stored = envelope.toByteArray();
        PutBlobResponse response;
        try {
            response = within().putBlob(PutBlobRequest.newBuilder()
                    .setDriveName(driveName)
                    .setObjectKey(objectKey)
                    .setData(ByteString.copyFrom(stored))
                    .setMimeType(EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                // The write may still land server-side. That is safe to leave: the caller
                // does not install the candidate, and the next mutation rewrites the whole
                // log from the state this process still considers current.
                throw unreachable("accept", e);
            }
            throw e;
        }
        if (response.getSizeBytes() != stored.length
                || !EncryptedRepositoryStateCodec.sha256Hex(stored)
                .equals(response.getSha256())
                || !driveName.equals(response.getStorageRef().getDriveName())
                || !objectKey.equals(response.getStorageRef().getObjectKey())) {
            throw new IllegalStateException(
                    "repository service did not confirm the persisted cluster event log");
        }
    }

    private FileStorageReference storageReference() {
        return FileStorageReference.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey)
                .build();
    }

    private String storageContext() {
        return driveName + "\n" + objectKey;
    }

    private static String requireText(String value, String name, int maxLength) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(name + " is invalid");
        }
        return value;
    }

    private static IllegalStateException corrupt(String message) {
        return new IllegalStateException(message);
    }

    private static IllegalStateException corrupt(String message, Throwable cause) {
        return new IllegalStateException(message, cause);
    }
}

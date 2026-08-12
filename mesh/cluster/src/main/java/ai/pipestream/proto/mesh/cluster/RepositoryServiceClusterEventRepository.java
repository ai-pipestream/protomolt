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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Stores an encrypted cluster directory event log through repository-service blob RPCs. */
public final class RepositoryServiceClusterEventRepository
        implements ClusterEventRepository {

    /** Plaintext content type authenticated inside the encrypted envelope. */
    public static final String CONTENT_TYPE =
            "application/vnd.protomolt.cluster-event-log+protobuf";

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String driveName;
    private final String objectKey;
    private final String keyReference;
    private final EncryptedRepositoryStateCodec codec;

    /** Creates a repository using the default encrypted-state codec limits. */
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
        this.documents = Objects.requireNonNull(documents, "documents");
        this.driveName = requireText(driveName, "driveName", 256);
        this.objectKey = requireText(objectKey, "objectKey", 1024);
        this.keyReference = requireText(keyReference, "keyReference", 256);
        this.codec = Objects.requireNonNull(codec, "codec");
    }

    @Override
    public Optional<List<ClusterEvent>> load(ClusterDescriptor cluster) {
        ClusterValidation.validate(cluster);
        GetBlobResponse response;
        try {
            response = documents.getBlob(GetBlobRequest.newBuilder()
                    .setStorageRef(storageReference()).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
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
        try {
            ValidationResult.validate(eventLog).throwIfInvalid();
            if (!cluster.getClusterId().equals(eventLog.getClusterId())
                    || !cluster.getFingerprint().equals(eventLog.getClusterFingerprint())) {
                throw new IllegalArgumentException("cluster identity does not match");
            }
            if (envelope.getRecordCount() != eventLog.getEventsCount()) {
                throw new IllegalArgumentException("event count does not match");
            }
            ClusterValidation.validateEventLog(eventLog.getEventsList());
        } catch (RuntimeException e) {
            throw corrupt("stored cluster event log failed validation", e);
        }
        return Optional.of(List.copyOf(eventLog.getEventsList()));
    }

    @Override
    public void save(ClusterDescriptor cluster, List<ClusterEvent> events) {
        ClusterValidation.validate(cluster);
        ClusterValidation.validateEventLog(events);
        ClusterEventLog eventLog = ClusterEventLog.newBuilder()
                .setClusterId(cluster.getClusterId())
                .setClusterFingerprint(cluster.getFingerprint())
                .addAllEvents(events)
                .build();
        ValidationResult.validate(eventLog).throwIfInvalid();
        EncryptedRepositoryState envelope = codec.encrypt(eventLog.toByteArray(),
                CONTENT_TYPE, events.size(), keyReference, storageContext());
        byte[] stored = envelope.toByteArray();
        PutBlobResponse response = documents.putBlob(PutBlobRequest.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey)
                .setData(ByteString.copyFrom(stored))
                .setMimeType(EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE)
                .build());
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

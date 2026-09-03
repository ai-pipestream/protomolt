package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Stores a complete delegation transcript through repository-service raw blob RPCs.
 * Encryption and integrity verification happen in the ProtoMolt process, so repository
 * service and every storage or cache layer below it only receive ciphertext.
 */
public final class RepositoryServiceTranscriptRepository implements TranscriptRepository {

    /** MIME type used for encrypted repository-state envelopes. */
    public static final String MIME_TYPE =
            EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE;
    /** Plaintext content type authenticated inside the encrypted envelope. */
    public static final String CONTENT_TYPE =
            "application/vnd.protomolt.delegation-transcript+protobuf";
    /** Default maximum serialized plaintext carried by the unary repository RPC. */
    public static final int DEFAULT_MAX_PLAINTEXT_BYTES =
            EncryptedRepositoryStateCodec.DEFAULT_MAX_PLAINTEXT_BYTES;
    /** Default deadline applied independently to every repository RPC. */
    public static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofSeconds(30);

    private static final Pattern KEY_REFERENCE = Pattern.compile(
            "[A-Za-z][A-Za-z0-9+.-]{0,31}:[A-Za-z_][A-Za-z0-9_]{0,127}");

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String driveName;
    private final String objectKey;
    private final String writeKeyReference;
    private final EncryptedRepositoryStateCodec codec;
    private final Duration rpcTimeout;
    private final DelegationReducer reducer = new DelegationReducer();

    /** Creates a repository using the default 8 MiB unary plaintext limit. */
    public RepositoryServiceTranscriptRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            RepositoryStateKeyResolver keys) {
        this(documents, driveName, objectKey, keyReference,
                new EncryptedRepositoryStateCodec(keys), DEFAULT_RPC_TIMEOUT);
    }

    /** Creates a repository with an explicit per-call RPC deadline. */
    public RepositoryServiceTranscriptRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            RepositoryStateKeyResolver keys, Duration rpcTimeout) {
        this(documents, driveName, objectKey, keyReference,
                new EncryptedRepositoryStateCodec(keys), rpcTimeout);
    }

    /** Creates a fully configurable repository for embedding and deterministic tests. */
    public RepositoryServiceTranscriptRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            RepositoryStateKeyResolver keys, Clock clock, SecureRandom random,
            int maxPlaintextBytes) {
        this(documents, driveName, objectKey, keyReference,
                new EncryptedRepositoryStateCodec(keys, clock, random, maxPlaintextBytes),
                DEFAULT_RPC_TIMEOUT);
    }

    private RepositoryServiceTranscriptRepository(
            DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            String driveName, String objectKey, String keyReference,
            EncryptedRepositoryStateCodec codec, Duration rpcTimeout) {
        this.documents = Objects.requireNonNull(documents, "documents");
        this.driveName = requireCoordinate(driveName, "driveName");
        this.objectKey = requireCoordinate(objectKey, "objectKey");
        this.writeKeyReference = requireCoordinate(keyReference, "keyReference");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.rpcTimeout = requireRpcTimeout(rpcTimeout);
        if (driveName.length() > 256 || objectKey.length() > 1024
                || keyReference.length() > 256
                || !KEY_REFERENCE.matcher(keyReference).matches()) {
            throw new IllegalArgumentException("transcript repository configuration is invalid");
        }
    }

    @Override
    public Optional<Transcript> load() {
        GetBlobResponse response;
        try {
            response = callStub().getBlob(GetBlobRequest.newBuilder()
                    .setStorageRef(storageReference()).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
        byte[] stored = response.getData().toByteArray();
        if (response.getSizeBytes() != stored.length) {
            throw corrupt("stored transcript size does not match repository metadata");
        }
        if (response.hasMimeType() && !MIME_TYPE.equals(response.getMimeType())) {
            throw corrupt("stored transcript has an unexpected media type");
        }
        EncryptedRepositoryState envelope;
        try {
            envelope = EncryptedRepositoryState.parseFrom(stored);
        } catch (InvalidProtocolBufferException e) {
            throw corrupt("stored transcript envelope is not valid protobuf", e);
        }
        byte[] plaintext = codec.decrypt(envelope, CONTENT_TYPE, storageContext());
        Transcript transcript;
        try {
            transcript = Transcript.parseFrom(plaintext);
        } catch (InvalidProtocolBufferException e) {
            throw corrupt("decrypted transcript is not valid protobuf", e);
        }
        if (transcript.getEntriesCount() != envelope.getRecordCount()) {
            throw corrupt("decrypted transcript entry count does not match");
        }
        validateTranscript(transcript, "stored transcript is invalid");
        return Optional.of(transcript);
    }

    @Override
    public void save(Transcript transcript) {
        Objects.requireNonNull(transcript, "transcript");
        validateTranscript(transcript, "transcript is invalid");
        EncryptedRepositoryState envelope = codec.encrypt(transcript.toByteArray(),
                CONTENT_TYPE, transcript.getEntriesCount(), writeKeyReference,
                storageContext());
        byte[] stored = envelope.toByteArray();
        PutBlobResponse response = callStub().putBlob(PutBlobRequest.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey)
                .setData(ByteString.copyFrom(stored))
                .setMimeType(MIME_TYPE)
                .build());
        if (response.getSizeBytes() != stored.length
                || !EncryptedRepositoryStateCodec.sha256Hex(stored)
                .equals(response.getSha256())
                || !driveName.equals(response.getStorageRef().getDriveName())
                || !objectKey.equals(response.getStorageRef().getObjectKey())) {
            throw new IllegalStateException(
                    "repository service did not confirm the persisted transcript bytes");
        }
    }

    private void validateTranscript(Transcript transcript, String prefix) {
        try {
            DelegationValidation.validate(transcript);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(prefix + ": " + e.getMessage(), e);
        }
        DelegationReducer.Result result = reducer.reduce(transcript);
        if (!result.clean()) {
            DelegationReducer.Finding finding = result.findings().getFirst();
            throw new IllegalArgumentException(prefix + ": " + finding.kind()
                    + ": " + finding.error());
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

    private DocumentServiceGrpc.DocumentServiceBlockingStub callStub() {
        return documents.withDeadlineAfter(rpcTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static Duration requireRpcTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "rpcTimeout");
        if (timeout.isZero() || timeout.isNegative()
                || timeout.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException(
                    "rpcTimeout must be positive and no greater than one hour");
        }
        return timeout;
    }

    private static String requireCoordinate(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
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

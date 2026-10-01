package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Stores a complete delegation transcript through repository-service raw blob RPCs.
 * Encryption and integrity verification happen in the ProtoMolt process, so repository
 * service and every storage or cache layer below it only receive ciphertext.
 * Writes use the authoritative backing ETag. A failed or unconfirmed write makes
 * this instance unusable; recovery requires a new instance and a fresh load.
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
    private Transcript confirmed = Transcript.getDefaultInstance();
    private String etag;
    private boolean failed;

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
    public synchronized Optional<Transcript> load() {
        requireActive();
        try {
            return loadCurrent();
        } catch (RuntimeException e) {
            failed = true;
            throw e;
        }
    }

    private Optional<Transcript> loadCurrent() {
        GetBlobForUpdateRequest request = GetBlobForUpdateRequest.newBuilder()
                .setKey(storageKey()).build();
        validateWire(request);
        GetBlobForUpdateResponse response;
        try {
            response = callStub().getBlobForUpdate(request);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                if (etag != null) throw corrupt("confirmed transcript disappeared from storage");
                return Optional.empty();
            }
            throw e;
        }
        validateWire(response);
        byte[] stored = response.getData().toByteArray();
        verifyVersion(response.getVersion(), stored);
        if (response.hasMimeType() && !MIME_TYPE.equals(response.getMimeType())) {
            throw corrupt("stored transcript has an unexpected media type");
        }
        EncryptedRepositoryState envelope;
        try {
            envelope = EncryptedRepositoryState.parseFrom(stored);
        } catch (InvalidProtocolBufferException e) {
            throw corrupt("stored transcript envelope is not valid protobuf", e);
        }
        rejectUnknown(envelope);
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
        requireExtension(transcript);
        confirmed = transcript;
        etag = response.getVersion().getEtag();
        return Optional.of(transcript);
    }

    @Override
    public synchronized void save(Transcript transcript) {
        requireActive();
        Objects.requireNonNull(transcript, "transcript");
        validateTranscript(transcript, "transcript is invalid");
        requireExtension(transcript);
        EncryptedRepositoryState envelope = codec.encrypt(transcript.toByteArray(),
                CONTENT_TYPE, transcript.getEntriesCount(), writeKeyReference,
                storageContext());
        byte[] stored = envelope.toByteArray();
        CompareAndPutBlobRequest.Builder request = CompareAndPutBlobRequest.newBuilder()
                .setKey(storageKey())
                .setData(ByteString.copyFrom(stored))
                .setMimeType(MIME_TYPE);
        if (etag == null) request.setIfAbsent(true);
        else request.setExpectedEtag(etag);
        CompareAndPutBlobRequest write = request.build();
        validateWire(write);
        // Mark uncertain before dispatch. Only a fully verified acknowledgement
        // makes this instance writable again, even if dispatch exits abnormally.
        failed = true;
        CompareAndPutBlobResponse response = callStub().compareAndPutBlob(write);
        validateWire(response);
        verifyVersion(response.getVersion(), stored);
        confirmed = transcript;
        etag = response.getVersion().getEtag();
        failed = false;
    }

    private void verifyVersion(ConditionalBlobVersion version, byte[] stored) {
        if (version.getSizeBytes() != stored.length
                || !EncryptedRepositoryStateCodec.sha256Hex(stored)
                .equals(version.getSha256())
                || !storageKey().equals(version.getKey())) {
            throw new IllegalStateException(
                    "repository service did not confirm the persisted transcript bytes");
        }
    }

    private void requireActive() {
        if (failed) throw new IllegalStateException(
                "transcript repository requires recovery through a new instance");
    }

    private void requireExtension(Transcript transcript) {
        if (transcript.getEntriesCount() < confirmed.getEntriesCount()
                || !transcript.getEntriesList().subList(0, confirmed.getEntriesCount())
                .equals(confirmed.getEntriesList())) {
            throw new IllegalArgumentException("transcript must preserve confirmed history");
        }
    }

    private static void validateWire(Message message) {
        rejectUnknown(message);
        var result = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!result.valid()) throw corrupt("repository protocol validation failed: " + result.violations());
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) throw corrupt("unknown repository protocol fields");
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object value : (List<?>) field.getValue()) rejectUnknown((Message) value);
            } else rejectUnknown((Message) field.getValue());
        }
    }

    private void validateTranscript(Transcript transcript, String prefix) {
        rejectUnknown(transcript);
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

    private ConditionalBlobKey storageKey() {
        return ConditionalBlobKey.newBuilder()
                .setDriveName(driveName)
                .setObjectKey(objectKey)
                .build();
    }

    private String storageContext() {
        return driveName + "\n" + objectKey;
    }

    private DocumentServiceGrpc.DocumentServiceBlockingStub callStub() {
        return documents.withMaxInboundMessageSize(16 * 1024 * 1024)
                .withDeadlineAfter(rpcTimeout.toNanos(), TimeUnit.NANOSECONDS);
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

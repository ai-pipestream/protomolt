package ai.protomolt.proto.delegation.repository;

import ai.protomolt.proto.delegation.lifecycle.TranscriptRepository;

import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;

import ai.protomolt.proto.delegation.contract.DelegationValidation;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Context;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Stores a complete delegation transcript as one v2 repository document whose CORE
 * part carries the encrypted envelope in {@code structured_data}. Encryption and
 * integrity verification happen in the ProtoMolt process, so the repository service
 * and every storage or cache layer below it only receive ciphertext. Writes compare
 * and set on the confirmed document revision (ExpectedRevision none for the first
 * write, then the confirmed revision), where the legacy blob flow used ETags. A
 * failed or unconfirmed write makes this instance unusable; recovery requires a new
 * instance and a fresh load.
 */
public final class RepositoryServiceTranscriptRepository implements TranscriptRepository {

    /** MIME type of the encrypted envelope bytes, retained for envelope identification. */
    public static final String MIME_TYPE =
            EncryptedRepositoryStateCodec.ENVELOPE_MIME_TYPE;
    /** Plaintext content type authenticated inside the encrypted envelope. */
    public static final String CONTENT_TYPE =
            "application/vnd.protomolt.delegation-transcript+protobuf";
    /** Default maximum serialized plaintext carried inside the state document. */
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
    private long confirmedRevision;
    private boolean failed;

    /** Creates a repository using the default 8 MiB plaintext limit. */
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
        if (driveName.length() > 200 || objectKey.length() > 1024
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
        var request = RepositoryStateDocuments.get(driveName, objectKey);
        validateWire(request);
        GetDocumentResponse response;
        try {
            response = callInRepositoryContext(() -> callStub().getDocument(request));
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                if (confirmedRevision != 0) throw corrupt("confirmed transcript disappeared from storage");
                return Optional.empty();
            }
            throw e;
        }
        rejectUnknown(response);
        long revision = RepositoryStateDocuments.confirmLoaded(request, response, "transcript");
        EncryptedRepositoryState envelope = RepositoryStateDocuments.envelopeOf(response.getDocument(), "transcript");
        byte[] plaintext = codec.decrypt(envelope, CONTENT_TYPE, storageContext());
        Transcript transcript;
        try {
            transcript = Transcript.parseFrom(plaintext);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw corrupt("decrypted transcript is not valid protobuf", e);
        }
        if (transcript.getEntriesCount() != envelope.getRecordCount()) {
            throw corrupt("decrypted transcript entry count does not match");
        }
        validateTranscript(transcript, "stored transcript is invalid");
        requireExtension(transcript);
        confirmed = transcript;
        confirmedRevision = revision;
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
        var request = RepositoryStateDocuments.save(driveName, objectKey, envelope, confirmedRevision);
        validateWire(request);
        // Mark uncertain before dispatch. Only a fully verified acknowledgement
        // makes this instance writable again, even if dispatch exits abnormally.
        failed = true;
        SaveDocumentResponse response = callInRepositoryContext(() -> callStub().saveDocument(request));
        rejectUnknown(response);
        confirmedRevision = RepositoryStateDocuments.confirmSaved(request, response, "transcript");
        confirmed = transcript;
        failed = false;
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

    private String storageContext() {
        return driveName + "\n" + objectKey;
    }

    private DocumentServiceGrpc.DocumentServiceBlockingStub callStub() {
        return documents.withMaxInboundMessageSize(16 * 1024 * 1024)
                .withDeadlineAfter(rpcTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Repository state belongs to this process, even when an initiating caller disconnects. */
    private static <T> T callInRepositoryContext(Supplier<T> call) {
        Context detached = Context.current().fork();
        Context previous = detached.attach();
        try {
            return call.get();
        } finally {
            detached.detach(previous);
        }
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

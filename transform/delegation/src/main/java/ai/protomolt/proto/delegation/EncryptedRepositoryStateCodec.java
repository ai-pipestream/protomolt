package ai.protomolt.proto.delegation;

import ai.protomolt.proto.delegation.storage.v1.EncryptedRepositoryState;
import ai.protomolt.proto.delegation.storage.v1.RepositoryStateEncryptionAlgorithm;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;

/** Encrypts and authenticates bounded protobuf state for repository-service storage. */
public final class EncryptedRepositoryStateCodec {

    /** Media type stored in repository service for every encrypted state envelope. */
    public static final String ENVELOPE_MIME_TYPE =
            "application/vnd.protomolt.encrypted-state+protobuf";
    /** Default maximum plaintext carried by a unary repository-service blob RPC. */
    public static final int DEFAULT_MAX_PLAINTEXT_BYTES = 8 * 1024 * 1024;

    private static final int NONCE_BYTES = 12;
    private static final int GCM_TAG_BITS = 128;
    private static final byte[] AAD_VERSION =
            "protomolt-encrypted-repository-state-v1".getBytes(StandardCharsets.UTF_8);

    private final RepositoryStateKeyResolver keys;
    private final Clock clock;
    private final SecureRandom random;
    private final int maxPlaintextBytes;

    /** Creates a codec with the default 8 MiB plaintext limit. */
    public EncryptedRepositoryStateCodec(RepositoryStateKeyResolver keys) {
        this(keys, Clock.systemUTC(), new SecureRandom(), DEFAULT_MAX_PLAINTEXT_BYTES);
    }

    /** Creates a fully configurable codec for embedding and deterministic tests. */
    public EncryptedRepositoryStateCodec(RepositoryStateKeyResolver keys, Clock clock,
                                         SecureRandom random, int maxPlaintextBytes) {
        this.keys = Objects.requireNonNull(keys, "keys");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        if (maxPlaintextBytes < 1 || maxPlaintextBytes > DEFAULT_MAX_PLAINTEXT_BYTES) {
            throw new IllegalArgumentException("maxPlaintextBytes is outside the supported range");
        }
        this.maxPlaintextBytes = maxPlaintextBytes;
    }

    /** Encrypts one serialized protobuf state message. */
    public EncryptedRepositoryState encrypt(byte[] plaintext, String contentType,
                                            int recordCount, String keyReference,
                                            String storageContext) {
        Objects.requireNonNull(plaintext, "plaintext");
        requireText(contentType, "contentType");
        requireText(keyReference, "keyReference");
        requireText(storageContext, "storageContext");
        if (plaintext.length > maxPlaintextBytes) {
            throw new IllegalArgumentException(
                    "serialized state exceeds the configured plaintext limit");
        }
        if (recordCount < 0 || recordCount > 100_000) {
            throw new IllegalArgumentException("recordCount is outside the supported range");
        }
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(nonce);
        byte[] ciphertext;
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, requireAes256(keys.resolve(keyReference)),
                    new GCMParameterSpec(GCM_TAG_BITS, nonce));
            cipher.updateAAD(aad(contentType, keyReference, storageContext));
            ciphertext = cipher.doFinal(plaintext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to encrypt repository state", e);
        }
        EncryptedRepositoryState envelope = EncryptedRepositoryState.newBuilder()
                .setFormatVersion(1)
                .setAlgorithm(RepositoryStateEncryptionAlgorithm
                        .REPOSITORY_STATE_ENCRYPTION_ALGORITHM_AES_256_GCM)
                .setKeyRef(keyReference)
                .setNonce(ByteString.copyFrom(nonce))
                .setCiphertext(ByteString.copyFrom(ciphertext))
                .setPlaintextSha256(sha256Hex(plaintext))
                .setRecordCount(recordCount)
                .setSavedAt(Timestamps.fromMillis(clock.millis()))
                .setContentType(contentType)
                .build();
        validateEnvelope(envelope, contentType);
        return envelope;
    }

    /** Authenticates and decrypts one state envelope for its expected use and location. */
    public byte[] decrypt(EncryptedRepositoryState envelope, String expectedContentType,
                          String storageContext) {
        Objects.requireNonNull(envelope, "envelope");
        validateEnvelope(envelope, expectedContentType);
        byte[] plaintext;
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE,
                    requireAes256(keys.resolve(envelope.getKeyRef())),
                    new GCMParameterSpec(GCM_TAG_BITS,
                            envelope.getNonce().toByteArray()));
            cipher.updateAAD(aad(expectedContentType, envelope.getKeyRef(), storageContext));
            plaintext = cipher.doFinal(envelope.getCiphertext().toByteArray());
        } catch (AEADBadTagException e) {
            throw new IllegalStateException("stored repository state authentication failed", e);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt stored repository state", e);
        }
        if (plaintext.length > maxPlaintextBytes) {
            throw new IllegalStateException("stored repository state exceeds the plaintext limit");
        }
        byte[] recordedDigest = HexFormat.of().parseHex(envelope.getPlaintextSha256());
        if (!MessageDigest.isEqual(sha256(plaintext), recordedDigest)) {
            throw new IllegalStateException("stored repository state digest does not match");
        }
        return plaintext;
    }

    /** Returns the lowercase SHA-256 digest of {@code value}. */
    public static String sha256Hex(byte[] value) {
        return HexFormat.of().formatHex(sha256(value));
    }

    private static void validateEnvelope(EncryptedRepositoryState envelope,
                                         String expectedContentType) {
        if (!envelope.hasSavedAt() || !Timestamps.isValid(envelope.getSavedAt())) {
            throw new IllegalStateException("stored repository state has an invalid timestamp");
        }
        try {
            ValidationResult.validate(envelope).throwIfInvalid();
        } catch (RuntimeException e) {
            throw new IllegalStateException("stored repository state failed validation", e);
        }
        if (envelope.getAlgorithm() != RepositoryStateEncryptionAlgorithm
                .REPOSITORY_STATE_ENCRYPTION_ALGORITHM_AES_256_GCM) {
            throw new IllegalStateException("stored repository state uses an unsupported algorithm");
        }
        if (!expectedContentType.equals(envelope.getContentType())) {
            throw new IllegalStateException("stored repository state has an unexpected content type");
        }
    }

    private static byte[] aad(String contentType, String keyReference,
                              String storageContext) {
        MessageDigest digest = digest();
        digest.update(AAD_VERSION);
        updateLengthPrefixed(digest, contentType);
        updateLengthPrefixed(digest, keyReference);
        updateLengthPrefixed(digest, storageContext);
        return digest.digest();
    }

    private static void updateLengthPrefixed(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static SecretKey requireAes256(SecretKey key) {
        if (key == null || key.getEncoded() == null || key.getEncoded().length != 32
                || !"AES".equalsIgnoreCase(key.getAlgorithm())) {
            throw new IllegalStateException("key resolver did not return an AES-256 key");
        }
        return key;
    }

    private static byte[] sha256(byte[] value) {
        return digest().digest(value);
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}

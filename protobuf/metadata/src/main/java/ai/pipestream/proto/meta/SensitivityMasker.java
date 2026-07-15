package ai.pipestream.proto.meta;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Masks fields by their schema-declared sensitivity class
 * ({@code ai.pipestream.proto.meta.v1.field.sensitivity}) — declare once in the contract,
 * mask on every surface. Two strategies:
 *
 * <ul>
 *   <li>{@code REMOVE} — the field is cleared; absent from output entirely;</li>
 *   <li>{@code REDACT} — strings become {@code ***} (visibly masked), everything else is
 *       cleared: a redacted number or bool would still leak by being plausible.</li>
 * </ul>
 *
 * <p>Recursion covers singular and repeated message fields, so a {@code pii}-classed field
 * three levels down is found. Requires descriptors whose options were parsed with the
 * metadata extensions registered ({@link DescriptorMetadata#registerExtensions}); options
 * left as unknown fields mask nothing.</p>
 */
public final class SensitivityMasker {

    public enum Strategy {
        REMOVE, REDACT,
        /** String/bytes values become AES-GCM ciphertext; other types clear. Needs a key. */
        ENCRYPT,
        /** Reverses ENCRYPT with the same key; a wrong key fails loudly, never silently. */
        DECRYPT;

        public static Strategy of(String name) {
            return valueOf(name.toUpperCase(Locale.ROOT));
        }
    }

    private static final String REDACTED = "***";
    private static final int GCM_NONCE_BYTES = 12;
    private static final int GCM_TAG_BITS = 128;

    /** The masked message plus which field paths were touched. */
    public record MaskResult(Message message, List<String> maskedPaths) {
    }

    private SensitivityMasker() {
    }

    public static MaskResult mask(Message message, Set<String> classes, Strategy strategy) {
        if (strategy == Strategy.ENCRYPT || strategy == Strategy.DECRYPT) {
            throw new IllegalArgumentException(strategy + " needs a key");
        }
        return mask(message, classes, strategy, null);
    }

    /**
     * Masks with a key (required for ENCRYPT/DECRYPT; 16, 24, or 32 bytes for AES). The
     * key is the caller's, never the schema's: the contract declares what is sensitive,
     * the operator holds the means.
     */
    public static MaskResult mask(Message message, Set<String> classes, Strategy strategy,
                                  byte[] key) {
        List<String> masked = new ArrayList<>();
        javax.crypto.SecretKey secret = null;
        if (strategy == Strategy.ENCRYPT || strategy == Strategy.DECRYPT) {
            if (key == null || (key.length != 16 && key.length != 24 && key.length != 32)) {
                throw new IllegalArgumentException(
                        strategy + " needs an AES key of 16, 24, or 32 bytes");
            }
            secret = new javax.crypto.spec.SecretKeySpec(key, "AES");
        }
        Message result = maskMessage(message, classes, strategy, secret, "", masked);
        return new MaskResult(result, List.copyOf(masked));
    }

    private static Message maskMessage(Message message, Set<String> classes,
                                       Strategy strategy, javax.crypto.SecretKey key,
                                       String prefix, List<String> masked) {
        Message.Builder builder = message.toBuilder();
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            String sensitivity = DescriptorMetadata.field(field)
                    .map(meta -> meta.getSensitivity())
                    .orElse("");
            if (!sensitivity.isEmpty() && classes.contains(sensitivity)) {
                apply(builder, field, strategy, key);
                masked.add(path);
                continue;
            }
            if (field.isMapField() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.isRepeated()) {
                int count = message.getRepeatedFieldCount(field);
                for (int i = 0; i < count; i++) {
                    builder.setRepeatedField(field, i, maskMessage(
                            (Message) message.getRepeatedField(field, i),
                            classes, strategy, key, path, masked));
                }
            } else if (message.hasField(field)) {
                builder.setField(field, maskMessage((Message) message.getField(field),
                        classes, strategy, key, path, masked));
            }
        }
        return builder.build();
    }

    private static void apply(Message.Builder builder, FieldDescriptor field,
                              Strategy strategy, javax.crypto.SecretKey key) {
        boolean transformable = field.getJavaType() == FieldDescriptor.JavaType.STRING
                || (field.getJavaType() == FieldDescriptor.JavaType.BYTE_STRING
                        && (strategy == Strategy.ENCRYPT || strategy == Strategy.DECRYPT));
        if (strategy == Strategy.REMOVE || !transformable) {
            // Ciphertext cannot live in an int64 and a redacted number would still look
            // plausible: everything non-transformable clears.
            builder.clearField(field);
            return;
        }
        if (field.isRepeated()) {
            int count = builder.getRepeatedFieldCount(field);
            for (int i = 0; i < count; i++) {
                builder.setRepeatedField(field, i, transform(
                        builder.getRepeatedField(field, i), field, strategy, key));
            }
        } else {
            builder.setField(field, transform(
                    builder.getField(field), field, strategy, key));
        }
    }

    private static Object transform(Object value, FieldDescriptor field, Strategy strategy,
                                    javax.crypto.SecretKey key) {
        if (strategy == Strategy.REDACT) {
            return REDACTED;
        }
        boolean isString = field.getJavaType() == FieldDescriptor.JavaType.STRING;
        byte[] plain = isString
                ? ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8)
                : ((com.google.protobuf.ByteString) value).toByteArray();
        if (strategy == Strategy.ENCRYPT) {
            byte[] boxed = seal(plain, key);
            return isString
                    ? java.util.Base64.getEncoder().encodeToString(boxed)
                    : com.google.protobuf.ByteString.copyFrom(boxed);
        }
        byte[] boxed = isString
                ? java.util.Base64.getDecoder().decode((String) value)
                : plain;
        byte[] opened = open(boxed, key);
        return isString
                ? new String(opened, java.nio.charset.StandardCharsets.UTF_8)
                : com.google.protobuf.ByteString.copyFrom(opened);
    }

    /** AES-GCM: a fresh random nonce prefixed to the ciphertext. */
    private static byte[] seal(byte[] plain, javax.crypto.SecretKey key) {
        try {
            byte[] nonce = new byte[GCM_NONCE_BYTES];
            java.security.SecureRandom.getInstanceStrong().nextBytes(nonce);
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key,
                    new javax.crypto.spec.GCMParameterSpec(GCM_TAG_BITS, nonce));
            byte[] sealed = cipher.doFinal(plain);
            byte[] out = new byte[nonce.length + sealed.length];
            System.arraycopy(nonce, 0, out, 0, nonce.length);
            System.arraycopy(sealed, 0, out, nonce.length, sealed.length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("Encryption failed: " + e.getMessage(), e);
        }
    }

    private static byte[] open(byte[] boxed, javax.crypto.SecretKey key) {
        try {
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(javax.crypto.Cipher.DECRYPT_MODE, key,
                    new javax.crypto.spec.GCMParameterSpec(GCM_TAG_BITS, boxed, 0,
                            GCM_NONCE_BYTES));
            return cipher.doFinal(boxed, GCM_NONCE_BYTES, boxed.length - GCM_NONCE_BYTES);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Decryption failed: wrong key or tampered value", e);
        }
    }
}

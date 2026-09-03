package ai.protomolt.proto.meta;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Masks fields by their schema-declared sensitivity class
 * ({@code ai.protomolt.proto.meta.v1.field.sensitivity}) — declare once in the contract,
 * mask on every surface. Two strategies:
 *
 * <ul>
 *   <li>{@code REMOVE} — the field is cleared; absent from output entirely;</li>
 *   <li>{@code REDACT} — strings become {@code ***} (visibly masked), everything else is
 *       cleared: a redacted number or bool would still leak by being plausible.</li>
 * </ul>
 *
 * <p>Recursion covers singular and repeated message fields <em>and message-valued map
 * entries</em>, so a {@code pii}-classed field three levels down — or inside a map value —
 * is found; map entries are reported as {@code field[key].nested}. A sensitivity class on a
 * map field itself applies to every entry's value. Requires descriptors whose options were
 * parsed with the metadata extensions registered
 * ({@link DescriptorMetadata#registerExtensions}); options left as unknown fields mask
 * nothing.</p>
 *
 * <h2>Packed payloads</h2>
 *
 * <p>A {@code google.protobuf.Any} hides its payload's fields in opaque bytes, so the walk
 * cannot see a classed field inside one without first resolving the payload's type. Given a
 * {@link PayloadResolver} that answers the packed type name, the payload is unpacked, masked
 * as any other message, and repacked under the same type URL; the bytes are left byte-for-byte
 * alone when nothing inside was classed. The default resolver searches the message's own
 * descriptor and its transitive imports, which is enough when the payload type travels with
 * the schema; callers holding a wider view (a descriptor set, a registry) should pass their
 * own.</p>
 *
 * <p>A payload whose type cannot be resolved <em>cannot be masked</em>, and this is the one
 * case where the masker cannot honour the contract. It never fails silently: the field path
 * is reported in {@link MaskResult#unresolvedPaths()}, and a caller treating masking as a
 * security boundary must decide what an unresolved payload means rather than assume the
 * output is clean.</p>
 *
 * <h2>Encrypted values</h2>
 *
 * <p>{@code ENCRYPT} seals string/bytes values as AES-GCM in a versioned envelope:
 * a format-version byte, a 12-byte random nonce, then ciphertext with a 128-bit tag. The
 * value's identity — its containing message's full name and its field number — is bound as
 * additional authenticated data, so ciphertext moved to a different field (or a different
 * message type) refuses to decrypt. Ciphertext is therefore deliberately <em>not</em>
 * portable across fields; re-encrypt under the destination field to move data. The version
 * byte leaves room for future algorithms and key rotation without guessing.</p>
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

    /** Resolves the type name packed in an {@code Any}'s type URL to its descriptor. */
    @FunctionalInterface
    public interface PayloadResolver {
        /** @return the descriptor for a fully qualified type name, or null when unknown. */
        Descriptor find(String typeName);
    }

    private static final String ANY_TYPE = "google.protobuf.Any";
    private static final String REDACTED = "***";
    private static final byte ENVELOPE_VERSION = 1;
    private static final int GCM_NONCE_BYTES = 12;
    private static final int GCM_TAG_BITS = 128;
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    /**
     * The masked message, which field paths were touched, and which packed payloads the
     * walk could not open. A non-empty {@link #unresolvedPaths()} means the output is not
     * provably free of the requested classes.
     */
    public record MaskResult(Message message, List<String> maskedPaths,
                             List<String> unresolvedPaths) {
    }

    /** One traversal's inputs and findings, so the walk's own signature stays readable. */
    private record Pass(Set<String> classes, Strategy strategy, javax.crypto.SecretKey key,
                        PayloadResolver resolver, List<String> masked,
                        List<String> unresolved) {
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
        return mask(message, classes, strategy, key, importedTypes(message));
    }

    /**
     * Masks with an explicit resolver for {@code Any} payloads. Callers that hold the whole
     * schema (a descriptor set, a registry) can see packed types the message's own imports
     * do not reach.
     */
    public static MaskResult mask(Message message, Set<String> classes, Strategy strategy,
                                  byte[] key, PayloadResolver resolver) {
        javax.crypto.SecretKey secret = null;
        if (strategy == Strategy.ENCRYPT || strategy == Strategy.DECRYPT) {
            if (key == null || (key.length != 16 && key.length != 24 && key.length != 32)) {
                throw new IllegalArgumentException(
                        strategy + " needs an AES key of 16, 24, or 32 bytes");
            }
            secret = new javax.crypto.spec.SecretKeySpec(key, "AES");
        }
        Pass pass = new Pass(classes, strategy, secret, resolver,
                new ArrayList<>(), new ArrayList<>());
        Message result = maskNested(message, "", pass);
        return new MaskResult(result, List.copyOf(pass.masked()),
                List.copyOf(pass.unresolved()));
    }

    private static Message maskMessage(Message message, String prefix, Pass pass) {
        Message.Builder builder = message.toBuilder();
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            String sensitivity = DescriptorMetadata.field(field)
                    .map(meta -> meta.getSensitivity())
                    .orElse("");
            if (!sensitivity.isEmpty() && pass.classes().contains(sensitivity)) {
                apply(builder, field, pass.strategy(), pass.key());
                pass.masked().add(path);
                continue;
            }
            if (field.isMapField()) {
                maskMapValues(message, builder, field, path, pass);
                continue;
            }
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.isRepeated()) {
                int count = message.getRepeatedFieldCount(field);
                for (int i = 0; i < count; i++) {
                    builder.setRepeatedField(field, i, maskNested(
                            (Message) message.getRepeatedField(field, i), path, pass));
                }
            } else if (message.hasField(field)) {
                builder.setField(field,
                        maskNested((Message) message.getField(field), path, pass));
            }
        }
        return builder.build();
    }

    /**
     * A nested message is either an {@code Any}, whose fields are packed out of sight, or an
     * ordinary message the walk can read straight through.
     */
    private static Message maskNested(Message message, String path, Pass pass) {
        return ANY_TYPE.equals(message.getDescriptorForType().getFullName())
                ? maskAny(message, path, pass)
                : maskMessage(message, path, pass);
    }

    /**
     * Unpacks, masks, and repacks an {@code Any}. The packed bytes are returned untouched
     * when nothing inside was classed, so masking never rewrites a payload it did not change.
     * An unopenable payload is recorded rather than passed over in silence: its fields are
     * exactly the ones nobody can see.
     */
    private static Message maskAny(Message any, String path, Pass pass) {
        FieldDescriptor urlField = any.getDescriptorForType().findFieldByName("type_url");
        FieldDescriptor valueField = any.getDescriptorForType().findFieldByName("value");
        if (urlField == null || valueField == null) {
            return any;
        }
        String typeUrl = (String) any.getField(urlField);
        ByteString packed = (ByteString) any.getField(valueField);
        if (typeUrl.isEmpty() || packed.isEmpty()) {
            return any;
        }
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        Descriptor payloadType = pass.resolver() == null
                ? null
                : pass.resolver().find(typeName);
        if (payloadType == null) {
            pass.unresolved().add(path);
            return any;
        }
        Message payload;
        try {
            payload = DynamicMessage.parseFrom(payloadType, packed);
        } catch (InvalidProtocolBufferException e) {
            // The type URL and the bytes disagree; masking it would corrupt a payload we
            // cannot even read.
            pass.unresolved().add(path);
            return any;
        }
        int before = pass.masked().size();
        Message maskedPayload = maskMessage(payload, path, pass);
        if (pass.masked().size() == before) {
            return any;
        }
        return any.toBuilder().setField(valueField, maskedPayload.toByteString()).build();
    }

    /** Message values inside an (unannotated) map can still hold sensitive fields. */
    private static void maskMapValues(Message message, Message.Builder builder,
                                      FieldDescriptor field, String path, Pass pass) {
        FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
        if (valueField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return;
        }
        FieldDescriptor keyField = field.getMessageType().findFieldByName("key");
        int count = message.getRepeatedFieldCount(field);
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            String entryPath = path + "[" + entry.getField(keyField) + "]";
            Message maskedValue =
                    maskNested((Message) entry.getField(valueField), entryPath, pass);
            builder.setRepeatedField(field, i,
                    entry.toBuilder().setField(valueField, maskedValue).build());
        }
    }

    /**
     * The default resolver: the message's own file and everything it imports. Enough when the
     * packed type travels with the schema that declares the {@code Any} field, and honestly
     * empty-handed when the payload is a type this schema never heard of.
     */
    public static PayloadResolver importedTypes(Message message) {
        FileDescriptor root = message.getDescriptorForType().getFile();
        return typeName -> {
            Deque<FileDescriptor> queue = new ArrayDeque<>(List.of(root));
            Set<String> seen = new HashSet<>();
            while (!queue.isEmpty()) {
                FileDescriptor file = queue.poll();
                if (!seen.add(file.getFullName())) {
                    continue;
                }
                queue.addAll(file.getDependencies());
                String pkg = file.getPackage();
                if (!pkg.isEmpty() && !typeName.startsWith(pkg + ".")) {
                    continue;
                }
                Descriptor found = file.findMessageTypeByName(
                        pkg.isEmpty() ? typeName : typeName.substring(pkg.length() + 1));
                if (found != null) {
                    return found;
                }
            }
            return null;
        };
    }

    private static void apply(Message.Builder builder, FieldDescriptor field,
                              Strategy strategy, javax.crypto.SecretKey key) {
        if (strategy == Strategy.REMOVE) {
            builder.clearField(field);
            return;
        }
        if (field.isMapField()) {
            applyToMapEntries(builder, field, strategy, key);
            return;
        }
        if (!transformable(field, strategy)) {
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

    /** A sensitivity class on the map field itself masks every entry's value. */
    private static void applyToMapEntries(Message.Builder builder, FieldDescriptor field,
                                          Strategy strategy, javax.crypto.SecretKey key) {
        FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
        if (!transformable(valueField, strategy)) {
            builder.clearField(field);
            return;
        }
        int count = builder.getRepeatedFieldCount(field);
        for (int i = 0; i < count; i++) {
            Message entry = (Message) builder.getRepeatedField(field, i);
            builder.setRepeatedField(field, i, entry.toBuilder()
                    .setField(valueField, transform(entry.getField(valueField),
                            valueField, strategy, key))
                    .build());
        }
    }

    private static boolean transformable(FieldDescriptor field, Strategy strategy) {
        return field.getJavaType() == FieldDescriptor.JavaType.STRING
                || (field.getJavaType() == FieldDescriptor.JavaType.BYTE_STRING
                        && (strategy == Strategy.ENCRYPT || strategy == Strategy.DECRYPT));
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
            byte[] boxed = seal(plain, field, key);
            return isString
                    ? java.util.Base64.getEncoder().encodeToString(boxed)
                    : com.google.protobuf.ByteString.copyFrom(boxed);
        }
        byte[] boxed = isString
                ? java.util.Base64.getDecoder().decode((String) value)
                : plain;
        byte[] opened = open(boxed, field, key);
        return isString
                ? new String(opened, java.nio.charset.StandardCharsets.UTF_8)
                : com.google.protobuf.ByteString.copyFrom(opened);
    }

    /**
     * The AES-GCM additional authenticated data: the envelope version plus the value's
     * identity. Binding the identity means a ciphertext pasted into another field — even a
     * type-compatible one — fails to open instead of silently decrypting.
     */
    private static byte[] aad(FieldDescriptor field) {
        String identity = field.getContainingType().getFullName() + "#" + field.getNumber();
        byte[] name = identity.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] out = new byte[name.length + 1];
        out[0] = ENVELOPE_VERSION;
        System.arraycopy(name, 0, out, 1, name.length);
        return out;
    }

    /** AES-GCM in the versioned envelope: version byte, fresh random nonce, ciphertext. */
    private static byte[] seal(byte[] plain, FieldDescriptor field, javax.crypto.SecretKey key) {
        try {
            byte[] nonce = new byte[GCM_NONCE_BYTES];
            RANDOM.nextBytes(nonce);
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, key,
                    new javax.crypto.spec.GCMParameterSpec(GCM_TAG_BITS, nonce));
            cipher.updateAAD(aad(field));
            byte[] sealed = cipher.doFinal(plain);
            byte[] out = new byte[1 + nonce.length + sealed.length];
            out[0] = ENVELOPE_VERSION;
            System.arraycopy(nonce, 0, out, 1, nonce.length);
            System.arraycopy(sealed, 0, out, 1 + nonce.length, sealed.length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("Encryption failed: " + e.getMessage(), e);
        }
    }

    private static byte[] open(byte[] boxed, FieldDescriptor field, javax.crypto.SecretKey key) {
        if (boxed.length < 1 + GCM_NONCE_BYTES + GCM_TAG_BITS / 8
                || boxed[0] != ENVELOPE_VERSION) {
            throw new IllegalArgumentException(
                    "Decryption failed: not a recognized encrypted-value envelope");
        }
        try {
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(javax.crypto.Cipher.DECRYPT_MODE, key,
                    new javax.crypto.spec.GCMParameterSpec(GCM_TAG_BITS, boxed, 1,
                            GCM_NONCE_BYTES));
            cipher.updateAAD(aad(field));
            return cipher.doFinal(boxed, 1 + GCM_NONCE_BYTES,
                    boxed.length - 1 - GCM_NONCE_BYTES);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Decryption failed: wrong key, wrong field, or tampered value", e);
        }
    }
}

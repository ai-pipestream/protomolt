package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.repo.external.v1.ExternalPartDigest;
import ai.protomolt.proto.repo.external.v1.ExternalProcessingIdentity;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.protovalidate.ProtovalidateRuleSource;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Canonical deterministic intent encoding, SHA-256 intent digesting, and RFC 9562
 * UUIDv5 operation-key derivation for {@link ExternalPublicationIntent}.
 *
 * <p>Every semantic field of {@link ExternalPublicationIntent} is encoded with
 * explicit length prefixes and presence markers so no field boundary or optional
 * omission can collide.
 */
public final class ExternalOperationKeys {

    /** Fixed namespace UUID for deterministic external publication operation IDs. */
    public static final UUID EXTERNAL_OPERATION_NAMESPACE =
            UUID.fromString("91c7d840-5c11-4f88-a761-9b3008140001");

    private static final byte CANONICAL_FORMAT_VERSION = 1;

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            List.of(new ProtomoltRuleSource(), new ProtovalidateRuleSource()));

    private static final Comparator<ExternalPartDigest> PART_DIGEST_ORDER =
            Comparator.comparingInt(ExternalPartDigest::getPartValue)
                    .thenComparing(ExternalPartDigest::getSubKey)
                    .thenComparing(ExternalPartDigest::getSha256)
                    .thenComparingLong(ExternalPartDigest::getSizeBytes);

    private ExternalOperationKeys() {
    }

    /**
     * Validates {@code intent} against both validation dialects and display URL rules,
     * then returns the lowercase 64-character hex SHA-256 of its canonical binary encoding.
     *
     * @param intent the semantic publication or deletion intent
     * @return lowercase hex SHA-256 of the canonical intent bytes
     */
    public static String intentSha256(ExternalPublicationIntent intent) {
        byte[] canonical = canonicalBytes(intent);
        return sha256Hex(canonical);
    }

    /**
     * Derives a deterministic RFC 9562 UUIDv5 operation identifier from {@code intent}.
     *
     * @param intent the semantic publication or deletion intent
     * @return lowercase UUIDv5 string bound to the exact intent
     */
    public static String deriveOperationId(ExternalPublicationIntent intent) {
        byte[] canonical = canonicalBytes(intent);
        return uuidV5(EXTERNAL_OPERATION_NAMESPACE, canonical).toString();
    }

    /**
     * Validates {@code intent} and returns its canonical length-delimited binary encoding.
     *
     * @param intent the intent to validate and encode
     * @return canonical deterministic byte representation
     */
    public static byte[] canonicalBytes(ExternalPublicationIntent intent) {
        validateIntent(intent);
        try {
            ByteArrayOutputStream raw = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(raw);
            out.writeByte(CANONICAL_FORMAT_VERSION);
            out.writeInt(intent.getEncodingVersion());
            writeUtf8(out, intent.getAccountId());
            byte[] sourceKey = ExternalPublicationCoordinates.logicalSourceKey(
                    intent.getSourceAddress());
            out.writeInt(sourceKey.length);
            out.write(sourceKey);
            out.writeLong(intent.getSourceRevision());
            writeOptionalUtf8(out, intent.hasPurgeId(), intent.getPurgeId());
            writeUtf8(out, intent.getBindingId());
            out.writeLong(intent.getBindingRevision());
            out.writeInt(intent.getActionValue());
            writeUtf8(out, intent.getExternalObjectId());
            writeOptionalUtf8(out, intent.hasDisplayUrl(), intent.getDisplayUrl());
            if (intent.hasProcessing()) {
                out.writeBoolean(true);
                encodeProcessing(out, intent.getProcessing());
            } else {
                out.writeBoolean(false);
            }
            out.flush();
            return raw.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory canonical encoding failed", impossible);
        }
    }

    /**
     * Validates {@code intent} structurally, semantically, and for safe non-secret URLs.
     *
     * @param intent the intent to validate
     */
    public static void validateIntent(ExternalPublicationIntent intent) {
        Objects.requireNonNull(intent, "intent");
        ValidationResult result = VALIDATOR.validate(intent);
        if (!result.valid()) {
            throw new IllegalArgumentException(
                    "Invalid ExternalPublicationIntent: " + result.violations());
        }
        ExternalPublicationCoordinates.requireSourceAddress(intent.getSourceAddress());
        if (intent.hasDisplayUrl()) {
            ExternalPublicationCoordinates.validateDisplayUrl(intent.getDisplayUrl());
        }
    }

    static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void encodeProcessing(
            DataOutputStream out, ExternalProcessingIdentity processing) throws IOException {
        List<ExternalPartDigest> parts = new ArrayList<>(processing.getSourcePartDigestsList());
        parts.sort(PART_DIGEST_ORDER);
        out.writeInt(parts.size());
        for (ExternalPartDigest part : parts) {
            out.writeInt(part.getPartValue());
            writeUtf8(out, part.getSubKey());
            writeUtf8(out, part.getSha256());
            out.writeLong(part.getSizeBytes());
        }

        List<String> schemas = new ArrayList<>(processing.getRetainedSchemaSha256SList());
        schemas.sort(Comparator.naturalOrder());
        out.writeInt(schemas.size());
        for (String schemaSha : schemas) {
            writeUtf8(out, schemaSha);
        }

        writeOptionalUtf8(out, processing.hasMappingId(), processing.getMappingId());
        writeOptionalUtf8(out, processing.hasGenerationDigest(), processing.getGenerationDigest());
        writeOptionalUtf8(out, processing.hasNlpInputSha256(), processing.getNlpInputSha256());
        writeOptionalUtf8(out, processing.hasNlpResultSha256(), processing.getNlpResultSha256());
        writeOptionalUtf8(out, processing.hasNlpReceiptSha256(), processing.getNlpReceiptSha256());
    }

    private static void writeOptionalUtf8(
            DataOutputStream out, boolean present, String value) throws IOException {
        out.writeBoolean(present);
        if (present) {
            writeUtf8(out, value);
        }
    }

    private static void writeUtf8(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static UUID uuidV5(UUID namespace, byte[] name) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            ByteBuffer nsBytes = ByteBuffer.allocate(16);
            nsBytes.putLong(namespace.getMostSignificantBits());
            nsBytes.putLong(namespace.getLeastSignificantBits());
            sha1.update(nsBytes.array());
            byte[] hash = sha1.digest(name);
            hash[6] = (byte) ((hash[6] & 0x0F) | 0x50);
            hash[8] = (byte) ((hash[8] & 0x3F) | 0x80);
            ByteBuffer out = ByteBuffer.wrap(hash, 0, 16);
            return new UUID(out.getLong(), out.getLong());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-1 unavailable for UUIDv5", impossible);
        }
    }
}

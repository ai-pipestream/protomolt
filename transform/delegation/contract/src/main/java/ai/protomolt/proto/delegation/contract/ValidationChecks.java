package ai.protomolt.proto.delegation.contract;

import ai.protomolt.proto.formats.Formats;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.List;

/**
 * The shared validation atoms of the delegation contract: refusal primitive, bounded text,
 * identity names, attempt/revision ranges, timestamps, frame envelopes, digests, canonical
 * UUIDs, and the unknown-field sweep. Every {@link DelegationValidation} entry point and
 * the review-identity checks in {@link ReviewValidation} are built from these, so a bound
 * or an error message exists in exactly one place.
 */
final class ValidationChecks {

    static final int MAX_ATTEMPT = 1_024;
    static final int MAX_NAME_LENGTH = 128;

    private ValidationChecks() {
    }

    /** Refuses with {@link IllegalArgumentException} carrying {@code message} when false. */
    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /** A text field must not exceed {@code max} characters. */
    static void bounded(String value, int max, String field) {
        require(value.length() <= max,
                field + " exceeds the maximum of " + max + " characters");
    }

    /** A participant or check name: a bounded lowercase slug. */
    static void validateName(String value, String field) {
        require(value.length() <= MAX_NAME_LENGTH && Formats.isSlug(value),
                field + " must be a lowercase slug name: " + value);
    }

    /** An attempt number in 1..{@link #MAX_ATTEMPT}. */
    static void validateAttempt(int attempt, String field) {
        require(attempt >= 1 && attempt <= MAX_ATTEMPT,
                field + " must be between 1 and " + MAX_ATTEMPT);
    }

    /** A revision number in 1..{@link #MAX_ATTEMPT}. */
    static void validateRevision(int revision, String field) {
        require(revision >= 1 && revision <= MAX_ATTEMPT,
                field + " must be between 1 and " + MAX_ATTEMPT);
    }

    /** A protobuf timestamp that is valid per {@link Timestamps#isValid}. */
    static void validateTimestamp(Timestamp value, String field) {
        require(Timestamps.isValid(value), field + " must be a valid timestamp");
    }

    /** The envelope fields every frame shares: ids, sequence, and send time. */
    static void validateEnvelope(String frameId, String taskId, long seq,
                                 boolean hasSentAt, Timestamp sentAt) {
        require(Formats.isUuid(frameId),
                "frame.frame_id must be a uuid: " + frameId);
        require(taskId.isEmpty() || Formats.isUuid(taskId),
                "frame.task_id must be a uuid or empty: " + taskId);
        require(seq >= 1, "frame.seq must be at least 1");
        require(hasSentAt, "frame.sent_at must be set");
        validateTimestamp(sentAt, "frame.sent_at");
    }

    /** A frame that belongs to a task must name it. */
    static void requireTaskScoped(String taskId) {
        require(!taskId.isEmpty(), "frame.task_id must name the task on task frames");
    }

    /** A lowercase SHA-256 hex digest. */
    static void validateDigest(String value, String field) {
        require(Formats.isSha256Hex(value), field + " must be a lowercase SHA-256 digest");
    }

    /** A canonical lowercase dashed UUID. */
    static void validateCanonicalUuid(String value, String field) {
        require(Formats.isUuid(value) && isLowercaseUuidAlphabet(value),
                field + " must be a canonical lowercase uuid");
    }

    /** A canonical lowercase dashed UUID, or empty when the field is optional. */
    static void validateOptionalCanonicalUuid(String value, String field) {
        if (!value.isEmpty()) validateCanonicalUuid(value, field);
    }

    /** A message tree must carry no unknown fields, recursively. */
    static void requireNoUnknown(Message value, String field) {
        require(value.getUnknownFields().asMap().isEmpty(), field + " contains unknown fields");
        for (var entry : value.getAllFields().entrySet()) {
            if (entry.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (entry.getKey().isRepeated()) {
                for (Object child : (List<?>) entry.getValue()) requireNoUnknown((Message) child, field);
            } else requireNoUnknown((Message) entry.getValue(), field);
        }
    }

    /**
     * Lowercase hex or dash with the canonical dashed-UUID length. Combined with
     * {@link Formats#isUuid} (whose hex alphabet is case-insensitive) this pins the
     * canonical lowercase form; together they accept exactly the set the former
     * {@code [0-9a-f-]{36}} check accepted.
     */
    private static boolean isLowercaseUuidAlphabet(String value) {
        if (value.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char c = value.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f') && c != '-') {
                return false;
            }
        }
        return true;
    }
}

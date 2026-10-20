package ai.protomolt.proto.grpc.workflow;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import java.util.stream.Collectors;

/**
 * The primitive checks every workflow contract validation is built from: the
 * {@code validate.v1} annotation pass, timestamp presence and ordering, and the single
 * refusal form. Every failure is an {@link IllegalArgumentException} naming the field.
 */
final class ContractChecks {

    private ContractChecks() {
    }

    /** Runs the message's own {@code validate.v1} annotations, recursing into nested messages. */
    static void annotations(Message message) {
        ValidationResult result = ProtoValidator.forMessageType(message.getDescriptorForType())
                .validate(message);
        if (!result.valid()) {
            throw new IllegalArgumentException("message fails the workflow contract annotations: "
                    + result.violations().stream()
                    .map(v -> "[" + v.path() + "] " + v.ruleId() + ": " + v.message())
                    .collect(Collectors.joining("; ")));
        }
    }

    /** A timestamp is present when either component is set; a present one must be well formed. */
    static void timestamp(Timestamp value, String field, boolean required) {
        boolean present = value.getSeconds() != 0 || value.getNanos() != 0;
        require(!required || present, field + " must be present");
        if (present) {
            require(value.getNanos() >= 0 && value.getNanos() < 1_000_000_000,
                    field + " must be a valid timestamp");
        }
    }

    /** An absent end is open; a present end must not precede the start. */
    static void order(Timestamp start, Timestamp end, String field) {
        boolean hasEnd = end.getSeconds() != 0 || end.getNanos() != 0;
        require(!hasEnd || compare(start, end) <= 0,
                field + ".completed_at must not precede started_at");
    }

    static int compare(Timestamp left, Timestamp right) {
        int seconds = Long.compare(left.getSeconds(), right.getSeconds());
        return seconds != 0 ? seconds : Integer.compare(left.getNanos(), right.getNanos());
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}

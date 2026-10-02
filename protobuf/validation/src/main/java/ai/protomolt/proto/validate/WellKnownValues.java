package ai.protomolt.proto.validate;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * The well-known types the validator treats as values rather than as messages to walk into, and
 * the conversions onto their Java counterparts.
 *
 * <p>Both the rule checks and the CEL bindings need these: a {@code Timestamp} field compares
 * against {@code timestamp.gt} as an {@link Instant}, and binds to {@code this} in a CEL rule as
 * the same {@link Instant}. Reading the fields by name rather than through the generated classes
 * keeps the validator working on {@code DynamicMessage}.</p>
 */
final class WellKnownValues {

    static final String TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    static final String DURATION_TYPE = "google.protobuf.Duration";

    /** Well-known wrapper message types mapped to the scalar family that validates their value. */
    static final Map<String, FieldDescriptor.JavaType> WRAPPER_TYPES = Map.of(
            "google.protobuf.Int32Value", FieldDescriptor.JavaType.INT,
            "google.protobuf.Int64Value", FieldDescriptor.JavaType.LONG,
            "google.protobuf.UInt32Value", FieldDescriptor.JavaType.INT,
            "google.protobuf.UInt64Value", FieldDescriptor.JavaType.LONG,
            "google.protobuf.FloatValue", FieldDescriptor.JavaType.FLOAT,
            "google.protobuf.DoubleValue", FieldDescriptor.JavaType.DOUBLE,
            "google.protobuf.BoolValue", FieldDescriptor.JavaType.BOOLEAN,
            "google.protobuf.StringValue", FieldDescriptor.JavaType.STRING,
            "google.protobuf.BytesValue", FieldDescriptor.JavaType.BYTE_STRING);

    private WellKnownValues() {
    }

    static Instant toInstant(Message timestamp) {
        Descriptor d = timestamp.getDescriptorForType();
        long seconds = (Long) timestamp.getField(d.findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(d.findFieldByName("nanos"));
        try {
            return Instant.ofEpochSecond(seconds, nanos);
        } catch (DateTimeException | ArithmeticException e) {
            // Out-of-range seconds/nanos are a runtime failure, not a raw unchecked leak.
            throw new RuleEvaluationException("timestamp value out of range: " + e.getMessage(), e);
        }
    }

    static Duration toJavaDuration(Message duration) {
        Descriptor d = duration.getDescriptorForType();
        long seconds = (Long) duration.getField(d.findFieldByName("seconds"));
        int nanos = (Integer) duration.getField(d.findFieldByName("nanos"));
        try {
            return Duration.ofSeconds(seconds, nanos);
        } catch (DateTimeException | ArithmeticException e) {
            throw new RuleEvaluationException("duration value out of range: " + e.getMessage(), e);
        }
    }
}

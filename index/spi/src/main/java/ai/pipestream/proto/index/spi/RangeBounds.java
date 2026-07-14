package ai.pipestream.proto.index.spi;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Optional;

/**
 * Bound fields of a range message. A field hinted with a range kind must be a singular
 * message whose type declares a matching bound pair named {@code (gte, lte)} or
 * {@code (min, max)} — {@code (gte, lte)} wins when both pairs exist. Numeric ranges
 * require the exact matching scalar type; DATE_RANGE accepts {@code google.protobuf.Timestamp}
 * or {@code int64} (epoch) bounds.
 *
 * <p>{@link IndexingPlanFactory} validates resolution at planning time; engine mappers use
 * the same resolution to extract bound values at document time.
 */
public record RangeBounds(FieldDescriptor lower, FieldDescriptor upper) {

    public static Optional<RangeBounds> resolve(Descriptor message, IndexFieldKind rangeKind) {
        Optional<RangeBounds> gteLte = pair(message, rangeKind, "gte", "lte");
        return gteLte.isPresent() ? gteLte : pair(message, rangeKind, "min", "max");
    }

    private static Optional<RangeBounds> pair(
            Descriptor message, IndexFieldKind rangeKind, String lowerName, String upperName) {
        FieldDescriptor lower = message.findFieldByName(lowerName);
        FieldDescriptor upper = message.findFieldByName(upperName);
        if (lower == null || upper == null
                || !matches(lower, rangeKind) || !matches(upper, rangeKind)) {
            return Optional.empty();
        }
        return Optional.of(new RangeBounds(lower, upper));
    }

    private static boolean matches(FieldDescriptor field, IndexFieldKind rangeKind) {
        if (field.isRepeated()) {
            return false;
        }
        return switch (rangeKind) {
            case INT_RANGE -> field.getJavaType() == FieldDescriptor.JavaType.INT;
            case LONG_RANGE -> field.getJavaType() == FieldDescriptor.JavaType.LONG;
            case FLOAT_RANGE -> field.getJavaType() == FieldDescriptor.JavaType.FLOAT;
            case DOUBLE_RANGE -> field.getJavaType() == FieldDescriptor.JavaType.DOUBLE;
            case DATE_RANGE -> field.getJavaType() == FieldDescriptor.JavaType.LONG
                    || (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                            && "google.protobuf.Timestamp".equals(field.getMessageType().getFullName()));
            default -> false;
        };
    }
}

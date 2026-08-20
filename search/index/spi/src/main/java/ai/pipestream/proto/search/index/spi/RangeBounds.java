package ai.pipestream.proto.search.index.spi;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * Bound fields of a range message.
 *
 * <p>The canonical range types in {@code ai.pipestream.proto.types.v1} are recognized
 * by name: {@code LongRange} for LONG_RANGE (and DATE_RANGE with epoch bounds),
 * {@code DoubleRange} for DOUBLE_RANGE, and {@code DateRange} for DATE_RANGE with
 * ISO calendar-day bounds ({@link #dayGrain()}). Their {@code (begin, end)} bounds are
 * optional (an unset bound is an open end) and carry per-end inclusivity flags
 * (absent = included).
 *
 * <p>Any other singular message duck-types by declaring a matching bound pair named
 * {@code (gte, lte)} or {@code (min, max)} — {@code (gte, lte)} wins when both pairs
 * exist. Numeric ranges require the exact matching scalar type; DATE_RANGE accepts
 * {@code google.protobuf.Timestamp} or {@code int64} (epoch) bounds. Duck-typed bounds
 * with explicit presence ({@code optional}) are also open ends when unset.
 *
 * <p>{@link IndexMappingFactory} validates resolution at mapping time; engine mappers use
 * the same resolution to extract bound values at document time.
 */
public record RangeBounds(
        FieldDescriptor lower,
        FieldDescriptor upper,
        FieldDescriptor includeHead,
        FieldDescriptor includeTail,
        boolean dayGrain) {

    private static final String CANONICAL_DATE = "ai.pipestream.proto.types.v1.DateRange";
    private static final String CANONICAL_LONG = "ai.pipestream.proto.types.v1.LongRange";
    private static final String CANONICAL_DOUBLE = "ai.pipestream.proto.types.v1.DoubleRange";

    public static Optional<RangeBounds> resolve(Descriptor message, IndexFieldKind rangeKind) {
        Optional<RangeBounds> canonical = canonical(message, rangeKind);
        if (canonical.isPresent()) {
            return canonical;
        }
        Optional<RangeBounds> gteLte = pair(message, rangeKind, "gte", "lte");
        return gteLte.isPresent() ? gteLte : pair(message, rangeKind, "min", "max");
    }

    /** The range kind a canonical types.v1 message maps to by name, if it is one. */
    public static Optional<IndexFieldKind> canonicalKind(Descriptor message) {
        return switch (message.getFullName()) {
            case CANONICAL_DATE -> Optional.of(IndexFieldKind.DATE_RANGE);
            case CANONICAL_LONG -> Optional.of(IndexFieldKind.LONG_RANGE);
            case CANONICAL_DOUBLE -> Optional.of(IndexFieldKind.DOUBLE_RANGE);
            default -> Optional.empty();
        };
    }

    /** Whether the lower bound is set on this range value. */
    public boolean hasLower(MessageOrBuilder range) {
        return !lower.hasPresence() || range.hasField(lower);
    }

    /** Whether the upper bound is set on this range value. */
    public boolean hasUpper(MessageOrBuilder range) {
        return !upper.hasPresence() || range.hasField(upper);
    }

    /** Whether the lower bound itself is in the range (absent flag = included). */
    public boolean lowerIncluded(MessageOrBuilder range) {
        return includeHead == null
                || !range.hasField(includeHead)
                || (Boolean) range.getField(includeHead);
    }

    /** Whether the upper bound itself is in the range (absent flag = included). */
    public boolean upperIncluded(MessageOrBuilder range) {
        return includeTail == null
                || !range.hasField(includeTail)
                || (Boolean) range.getField(includeTail);
    }

    /** Epoch milliseconds of the first instant (UTC) of an ISO calendar day. */
    public static long dayFirstMillis(String isoDate) {
        return LocalDate.parse(isoDate).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    /** Milliseconds in one calendar day; the last included millisecond is first + this - 1. */
    public static final long DAY_MILLIS = 24L * 60 * 60 * 1000;

    private static Optional<RangeBounds> canonical(Descriptor message, IndexFieldKind rangeKind) {
        boolean matches = switch (message.getFullName()) {
            case CANONICAL_DATE -> rangeKind == IndexFieldKind.DATE_RANGE;
            case CANONICAL_LONG -> rangeKind == IndexFieldKind.LONG_RANGE
                    || rangeKind == IndexFieldKind.DATE_RANGE;
            case CANONICAL_DOUBLE -> rangeKind == IndexFieldKind.DOUBLE_RANGE;
            default -> false;
        };
        if (!matches) {
            return Optional.empty();
        }
        return Optional.of(new RangeBounds(
                message.findFieldByName("begin"),
                message.findFieldByName("end"),
                message.findFieldByName("include_head"),
                message.findFieldByName("include_tail"),
                CANONICAL_DATE.equals(message.getFullName())));
    }

    private static Optional<RangeBounds> pair(
            Descriptor message, IndexFieldKind rangeKind, String lowerName, String upperName) {
        FieldDescriptor lower = message.findFieldByName(lowerName);
        FieldDescriptor upper = message.findFieldByName(upperName);
        if (lower == null || upper == null
                || !matches(lower, rangeKind) || !matches(upper, rangeKind)) {
            return Optional.empty();
        }
        return Optional.of(new RangeBounds(lower, upper, null, null, false));
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

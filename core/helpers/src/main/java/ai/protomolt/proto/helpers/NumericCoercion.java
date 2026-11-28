package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.FieldDescriptor;

import java.math.BigInteger;

/**
 * Exact signed and unsigned integral coercions keyed by protobuf {@link FieldDescriptor.Type}.
 *
 * <p>{@code protobuf-java} stores {@code uint32}/{@code fixed32} in two's-complement {@link Integer}
 * and {@code uint64}/{@code fixed64} in two's-complement {@link Long}, while CEL represents unsigned
 * integers as {@code com.google.common.primitives.UnsignedLong} (a {@link Number} subclass whose
 * {@link Number#longValue()} returns the raw 64-bit unsigned bit pattern). Dispatching on
 * {@link FieldDescriptor#getType()} preserves full unsigned ranges ({@code [0, 2^32 - 1]} and
 * {@code [0, 2^64 - 1]}) without adding a Guava compile dependency to {@code core/helpers}.</p>
 */
final class NumericCoercion {

    private static final String GUAVA_UNSIGNED_LONG_CLASS =
            "com.google.common.primitives.UnsignedLong";
    private static final long MAX_UINT32 = 0xFFFF_FFFFL;
    private static final BigInteger MAX_UINT32_BIG = BigInteger.valueOf(MAX_UINT32);
    private static final BigInteger MAX_UINT64_BIG =
            BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final double TWO_POW_63 = 0x1.0p63;
    private static final double TWO_POW_64 = 0x1.0p64;

    private NumericCoercion() {
    }

    static boolean isUnsigned32(FieldDescriptor field) {
        FieldDescriptor.Type type = field.getType();
        return type == FieldDescriptor.Type.UINT32 || type == FieldDescriptor.Type.FIXED32;
    }

    static boolean isUnsigned64(FieldDescriptor field) {
        FieldDescriptor.Type type = field.getType();
        return type == FieldDescriptor.Type.UINT64 || type == FieldDescriptor.Type.FIXED64;
    }

    static int toFieldInt(Object value, FieldDescriptor field) {
        return isUnsigned32(field) ? toUint32Bits(value, field) : toSignedInt32(value, field);
    }

    static long toFieldLong(Object value, FieldDescriptor field) {
        return isUnsigned64(field) ? toUint64Bits(value, field) : toSignedInt64(value, field);
    }

    static int toSignedInt32(Object value, FieldDescriptor field) {
        if (value instanceof String text) {
            return Integer.parseInt(text);
        }
        if (isGuavaUnsignedLong(value)) {
            long bits = ((Number) value).longValue();
            if (Long.compareUnsigned(bits, Integer.MAX_VALUE) > 0) {
                throw new IllegalArgumentException(
                        "Value " + value + " is out of int32 range for field " + field.getFullName());
            }
            return (int) bits;
        }
        if (value instanceof Number number) {
            long asLong = signedNumberToLong(number, field, "int32");
            int result = (int) asLong;
            if (result != asLong) {
                throw new IllegalArgumentException(
                        "Value " + number + " is out of int32 range for field " + field.getFullName());
            }
            return result;
        }
        throw incompatible(value, field);
    }

    static long toSignedInt64(Object value, FieldDescriptor field) {
        if (value instanceof String text) {
            return Long.parseLong(text);
        }
        if (isGuavaUnsignedLong(value)) {
            long bits = ((Number) value).longValue();
            if (bits < 0L) {
                throw new IllegalArgumentException(
                        "Value " + value + " is out of int64 range for field " + field.getFullName());
            }
            return bits;
        }
        if (value instanceof Number number) {
            return signedNumberToLong(number, field, "int64");
        }
        throw incompatible(value, field);
    }

    private static int toUint32Bits(Object value, FieldDescriptor field) {
        if (value instanceof String text) {
            return Integer.parseUnsignedInt(text);
        }
        if (isGuavaUnsignedLong(value)) {
            long bits = ((Number) value).longValue();
            if (Long.compareUnsigned(bits, MAX_UINT32) > 0) {
                throw new IllegalArgumentException(
                        "Value " + value + " is out of uint32 range for field " + field.getFullName());
            }
            return (int) bits;
        }
        if (value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) {
            long v = ((Number) value).longValue();
            if (v < 0L || v > MAX_UINT32) {
                throw new IllegalArgumentException(
                        "Value " + value + " is out of uint32 range for field " + field.getFullName());
            }
            return (int) v;
        }
        if (value instanceof BigInteger big) {
            if (big.signum() < 0 || big.compareTo(MAX_UINT32_BIG) > 0) {
                throw new IllegalArgumentException(
                        "Value " + big + " is out of uint32 range for field " + field.getFullName());
            }
            return (int) big.longValue();
        }
        if (value instanceof Number number) {
            double d = number.doubleValue();
            if (!Double.isFinite(d) || d < 0.0 || d > (double) MAX_UINT32) {
                throw new IllegalArgumentException(
                        "Value " + number + " is out of uint32 range for field " + field.getFullName());
            }
            long asLong = (long) d;
            if ((double) asLong != d) {
                throw new IllegalArgumentException(
                        "Value " + number + " cannot be represented exactly as uint32 for field "
                                + field.getFullName());
            }
            return (int) asLong;
        }
        throw incompatible(value, field);
    }

    private static long toUint64Bits(Object value, FieldDescriptor field) {
        if (value instanceof String text) {
            return Long.parseUnsignedLong(text);
        }
        if (isGuavaUnsignedLong(value)) {
            return ((Number) value).longValue();
        }
        if (value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) {
            long v = ((Number) value).longValue();
            if (v < 0L) {
                throw new IllegalArgumentException(
                        "Value " + value + " is out of uint64 range for field " + field.getFullName());
            }
            return v;
        }
        if (value instanceof BigInteger big) {
            if (big.signum() < 0 || big.compareTo(MAX_UINT64_BIG) > 0) {
                throw new IllegalArgumentException(
                        "Value " + big + " is out of uint64 range for field " + field.getFullName());
            }
            return big.longValue();
        }
        if (value instanceof Number number) {
            return doubleToUint64Bits(number.doubleValue(), field);
        }
        throw incompatible(value, field);
    }

    private static boolean isGuavaUnsignedLong(Object value) {
        return value instanceof Number
                && GUAVA_UNSIGNED_LONG_CLASS.equals(value.getClass().getName());
    }

    private static long signedNumberToLong(Number number, FieldDescriptor field, String targetType) {
        if (number instanceof Long || number instanceof Integer
                || number instanceof Short || number instanceof Byte) {
            return number.longValue();
        }
        if (number instanceof BigInteger big) {
            try {
                return big.longValueExact();
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException(
                        "Value " + number + " is out of " + targetType + " range for field "
                                + field.getFullName(), e);
            }
        }
        double d = number.doubleValue();
        if (!Double.isFinite(d) || d < -TWO_POW_63 || d >= TWO_POW_63) {
            throw new IllegalArgumentException(
                    "Value " + d + " cannot be represented exactly as " + targetType + " for field "
                            + field.getFullName());
        }
        long result = (long) d;
        if ((double) result != d) {
            throw new IllegalArgumentException(
                    "Value " + d + " cannot be represented exactly as " + targetType + " for field "
                            + field.getFullName());
        }
        return result;
    }

    private static long doubleToUint64Bits(double d, FieldDescriptor field) {
        if (!Double.isFinite(d) || d < 0.0 || d >= TWO_POW_64) {
            throw new IllegalArgumentException(
                    "Value " + d + " is out of uint64 range for field " + field.getFullName());
        }
        if (d < TWO_POW_63) {
            long result = (long) d;
            if ((double) result != d) {
                throw new IllegalArgumentException(
                        "Value " + d + " cannot be represented exactly as uint64 for field "
                                + field.getFullName());
            }
            return result;
        }
        double shifted = d - TWO_POW_63;
        long low = (long) shifted;
        if ((double) low != shifted) {
            throw new IllegalArgumentException(
                    "Value " + d + " cannot be represented exactly as uint64 for field "
                            + field.getFullName());
        }
        return low | Long.MIN_VALUE;
    }

    private static IllegalArgumentException incompatible(Object value, FieldDescriptor field) {
        return new IllegalArgumentException(
                "Cannot convert value of type " + value.getClass().getName()
                        + " to field type " + field.getJavaType());
    }
}

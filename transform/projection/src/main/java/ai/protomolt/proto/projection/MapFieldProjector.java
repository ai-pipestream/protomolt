package ai.protomolt.proto.projection;

import ai.protomolt.proto.helpers.TypeConverter;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Projects map-shaped inputs ({@link MapInput.ProtoEntries}, CEL {@link Map}, and
 * {@link MapInput.StructLiteral}) onto a protobuf {@code map<K, V>} target field.
 *
 * <p>{@code DynamicMessage} in {@code protobuf-java} serializes map entries in list insertion
 * order even when {@code CodedOutputStream.useDeterministicSerialization()} is active. Emitting
 * projected entries sorted by canonical protobuf map-key order makes {@code DynamicMessage} wire
 * output deterministic and byte-identical across Java ({@code GeneratedMessage} and
 * {@code DynamicMessage}) and {@code protomolt-rs} ({@code BTreeMap}).</p>
 */
final class MapFieldProjector {

    private static final int MAP_KEY_FIELD_NUMBER = 1;
    private static final int MAP_VALUE_FIELD_NUMBER = 2;
    private static final String VALUE_FULL_NAME = Value.getDescriptor().getFullName();
    private static final String NULL_VALUE_ENUM_FULL_NAME = NullValue.getDescriptor().getFullName();

    @FunctionalInterface
    interface ValueElementConverter {
        Object convert(FieldDescriptor ownerField, FieldDescriptor valueField, Object rawValue, boolean fromLiteral);
    }

    private MapFieldProjector() {
    }

    static FieldDescriptor valueFieldOf(FieldDescriptor mapField) {
        return mapField.getMessageType().findFieldByNumber(MAP_VALUE_FIELD_NUMBER);
    }

    static void assignMap(
            DynamicMessage.Builder out,
            FieldDescriptor mapField,
            Object sourceValue,
            TypeConverter typeConverter,
            ValueElementConverter valueConverter) {
        var entryType = mapField.getMessageType();
        FieldDescriptor keyField = entryType.findFieldByNumber(MAP_KEY_FIELD_NUMBER);
        FieldDescriptor valueField = entryType.findFieldByNumber(MAP_VALUE_FIELD_NUMBER);
        TreeMap<Object, Object> sortedEntries = new TreeMap<>(canonicalKeyComparator(keyField));

        switch (sourceValue) {
            case MapInput.ProtoEntries protoEntries -> {
                for (MapInput.RawEntry pair : protoEntries.entries()) {
                    collectProjectedEntry(
                            mapField, keyField, valueField,
                            pair.key(), pair.value(), false,
                            sortedEntries, typeConverter, valueConverter);
                }
            }
            case Map<?, ?> celMap -> {
                for (Map.Entry<?, ?> pair : celMap.entrySet()) {
                    collectProjectedEntry(
                            mapField, keyField, valueField,
                            pair.getKey(), pair.getValue(), false,
                            sortedEntries, typeConverter, valueConverter);
                }
            }
            case MapInput.StructLiteral literal -> {
                for (Map.Entry<String, Value> pair : literal.struct().getFieldsMap().entrySet()) {
                    Object rawVal = unwrapStructLiteralValue(pair.getValue(), valueField, typeConverter);
                    collectProjectedEntry(
                            mapField, keyField, valueField,
                            pair.getKey(), rawVal, true,
                            sortedEntries, typeConverter, valueConverter);
                }
            }
            default -> throw new ProjectionException("Value for map projection field "
                    + mapField.getFullName() + " is not a map: " + sourceValue.getClass().getName());
        }

        for (Map.Entry<Object, Object> entry : sortedEntries.entrySet()) {
            DynamicMessage entryMsg = DynamicMessage.newBuilder(entryType)
                    .setField(keyField, entry.getKey())
                    .setField(valueField, entry.getValue())
                    .build();
            out.addRepeatedField(mapField, entryMsg);
        }
    }

    private static void collectProjectedEntry(
            FieldDescriptor mapField,
            FieldDescriptor keyField,
            FieldDescriptor valueField,
            Object rawKey,
            Object rawValue,
            boolean fromLiteral,
            TreeMap<Object, Object> sortedEntries,
            TypeConverter typeConverter,
            ValueElementConverter valueConverter) {
        if (isNullLike(rawKey)) {
            throw new ProjectionException(
                    "Null key for map projection field " + mapField.getFullName());
        }
        rejectStructuredScalar(rawKey, keyField, mapField);
        final Object convertedKey;
        try {
            convertedKey = typeConverter.convertToFieldType(rawKey, keyField);
        } catch (IllegalArgumentException e) {
            throw new ProjectionException(
                    "Cannot coerce map key for projection field " + mapField.getFullName(), e);
        }
        if (sortedEntries.containsKey(convertedKey)) {
            throw new ProjectionException("Duplicate coerced map key for projection field "
                    + mapField.getFullName() + ": " + formatKey(convertedKey, keyField));
        }

        Object convertedValue = coerceEntryValue(
                mapField, valueField, rawValue, fromLiteral, typeConverter, valueConverter);
        sortedEntries.put(convertedKey, convertedValue);
    }

    private static Comparator<Object> canonicalKeyComparator(FieldDescriptor keyField) {
        return switch (keyField.getType()) {
            case BOOL -> (a, b) -> Boolean.compare((Boolean) a, (Boolean) b);
            case INT32, SINT32, SFIXED32 -> (a, b) -> Integer.compare((Integer) a, (Integer) b);
            case UINT32, FIXED32 -> (a, b) -> Integer.compareUnsigned((Integer) a, (Integer) b);
            case INT64, SINT64, SFIXED64 -> (a, b) -> Long.compare((Long) a, (Long) b);
            case UINT64, FIXED64 -> (a, b) -> Long.compareUnsigned((Long) a, (Long) b);
            case STRING -> (a, b) -> Arrays.compareUnsigned(
                    ((String) a).getBytes(StandardCharsets.UTF_8),
                    ((String) b).getBytes(StandardCharsets.UTF_8));
            default -> throw new IllegalStateException(
                    "Unsupported protobuf map key type: " + keyField.getType());
        };
    }

    private static Object coerceEntryValue(
            FieldDescriptor mapField,
            FieldDescriptor valueField,
            Object rawValue,
            boolean fromLiteral,
            TypeConverter typeConverter,
            ValueElementConverter valueConverter) {
        if (isNullLike(rawValue)) {
            if (valueField.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && VALUE_FULL_NAME.equals(valueField.getMessageType().getFullName())) {
                Value nullVal = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
                return typeConverter.convertToFieldType(nullVal, valueField);
            }
            if (valueField.getJavaType() == FieldDescriptor.JavaType.ENUM
                    && NULL_VALUE_ENUM_FULL_NAME.equals(valueField.getEnumType().getFullName())) {
                return valueField.getEnumType().findValueByNumber(0);
            }
            throw new ProjectionException(
                    "Null value for map projection field " + mapField.getFullName());
        }
        try {
            return valueConverter.convert(mapField, valueField, rawValue, fromLiteral);
        } catch (IllegalArgumentException e) {
            throw new ProjectionException(
                    "Cannot coerce map value for projection field " + mapField.getFullName(), e);
        }
    }

    private static Object unwrapStructLiteralValue(
            Value literalValue, FieldDescriptor valueField, TypeConverter typeConverter) {
        if (valueField.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && VALUE_FULL_NAME.equals(valueField.getMessageType().getFullName())) {
            return literalValue;
        }
        if (literalValue.getKindCase() == Value.KindCase.NULL_VALUE) {
            return null;
        }
        return typeConverter.fromValue(literalValue);
    }

    static boolean isNullLike(Object value) {
        return value == null
                || value instanceof NullValue
                || value == dev.cel.common.values.NullValue.NULL_VALUE;
    }

    static void rejectStructuredScalar(
            Object value, FieldDescriptor targetField, FieldDescriptor ownerField) {
        if (targetField.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return;
        }
        if (value instanceof Message
                || value instanceof List<?>
                || value instanceof Map<?, ?>
                || value instanceof MapInput
                || (value instanceof ByteString
                        && targetField.getJavaType() != FieldDescriptor.JavaType.BYTE_STRING)) {
            throw new ProjectionException("Cannot coerce value of type " + value.getClass().getName()
                    + " for projection field " + ownerField.getFullName());
        }
    }

    private static String formatKey(Object convertedKey, FieldDescriptor keyField) {
        return switch (keyField.getType()) {
            case UINT32, FIXED32 -> Integer.toUnsignedString((Integer) convertedKey);
            case UINT64, FIXED64 -> Long.toUnsignedString((Long) convertedKey);
            default -> String.valueOf(convertedKey);
        };
    }

    static Object normalizeCelResult(Object celResult) {
        if (celResult == dev.cel.common.values.NullValue.NULL_VALUE) {
            return null;
        }
        if (celResult instanceof UnsignedLong) {
            return celResult;
        }
        return celResult;
    }
}

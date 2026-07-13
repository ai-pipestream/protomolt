package ai.pipestream.proto.helpers;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for converting between different Protocol Buffer types.
 * Handles conversions between messages, primitives, Struct, Value, and common Java types.
 */
public class TypeConverter {

    /**
     * Creates a type converter.
     */
    public TypeConverter() {}


    /**
     * Converts a Java object to a protobuf Value.
     * Supports null, String, Number, Boolean, Struct, List, and Message types.
     *
     * @param value The value to convert
     * @return A protobuf Value representation
     * @throws IllegalArgumentException if the type cannot be converted
     */
    public Value toValue(Object value) {
        switch (value) {
            case null -> {
                return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
            }
            case String s -> {
                return Value.newBuilder().setStringValue(s).build();
            }
            case Double v -> {
                return Value.newBuilder().setNumberValue(v).build();
            }
            case Float v -> {
                return Value.newBuilder().setNumberValue(v.doubleValue()).build();
            }
            case Number number -> {
                return Value.newBuilder().setNumberValue(number.doubleValue()).build();
            }
            case Boolean b -> {
                return Value.newBuilder().setBoolValue(b).build();
            }
            case Struct struct -> {
                return Value.newBuilder().setStructValue(struct).build();
            }
            //noinspection rawtypes
            case List list -> {
                ListValue.Builder listBuilder = ListValue.newBuilder();
                for (Object item : list) {
                    listBuilder.addValues(toValue(item));
                }
                return Value.newBuilder().setListValue(listBuilder).build();
            }
            case Message message -> {
                // Convert message to Struct representation
                return Value.newBuilder().setStructValue(messageToStruct(message)).build();
                // Convert message to Struct representation
            }
            default -> {
            }
        }
        throw new IllegalArgumentException("Cannot convert type to Value: " + value.getClass().getName());
    }

    /**
     * Converts a protobuf Value to a Java object.
     *
     * @param value The Value to convert
     * @return The unwrapped Java object (String, Double, Boolean, Struct, List, or null)
     */
    public Object fromValue(Value value) {
        if (value == null || value.getKindCase() == Value.KindCase.NULL_VALUE) {
            return null;
        }
        return switch (value.getKindCase()) {
            case NUMBER_VALUE -> value.getNumberValue();
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case STRUCT_VALUE -> value.getStructValue();
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                    .map(this::fromValue)
                    .collect(Collectors.toList());
            default -> null;
        };
    }

    /**
     * Converts a protobuf Message to a Struct.
     * Recursively converts all fields to Value representations.
     *
     * @param message The message to convert
     * @return A Struct representation of the message
     */
    public Struct messageToStruct(Message message) {
        Struct.Builder structBuilder = Struct.newBuilder();
        Descriptor descriptor = message.getDescriptorForType();

        for (FieldDescriptor field : descriptor.getFields()) {
            // Skip non-repeated fields that are not set
            if (!field.isRepeated() && !message.hasField(field)) {
                continue;
            }

            String fieldName = field.getName();
            Object fieldValue = message.getField(field);

            if (field.isRepeated()) {
                List<?> values = (List<?>) fieldValue;
                if (!values.isEmpty()) {
                    ListValue.Builder listBuilder = ListValue.newBuilder();
                    for (Object item : values) {
                        listBuilder.addValues(fieldToValue(field, item));
                    }
                    structBuilder.putFields(fieldName, Value.newBuilder().setListValue(listBuilder).build());
                }
            } else {
                structBuilder.putFields(fieldName, fieldToValue(field, fieldValue));
            }
        }

        return structBuilder.build();
    }

    /**
     * Converts a Struct to a DynamicMessage of the specified type.
     *
     * @param struct The Struct to convert
     * @param descriptor The descriptor of the target message type
     * @return A DynamicMessage built from the Struct
     * @throws IllegalArgumentException if conversion fails
     */
    public DynamicMessage structToMessage(Struct struct, Descriptor descriptor) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

        for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
            FieldDescriptor field = descriptor.findFieldByName(entry.getKey());
            if (field == null) {
                // Skip unknown fields
                continue;
            }

            Object convertedValue = valueToField(entry.getValue(), field);
            if (convertedValue != null) {
                if (field.isRepeated() && convertedValue instanceof List) {
                    for (Object item : (List<?>) convertedValue) {
                        builder.addRepeatedField(field, item);
                    }
                } else {
                    builder.setField(field, convertedValue);
                }
            }
        }

        return builder.build();
    }

    /**
     * Converts a field value to a protobuf Value based on the field descriptor.
     *
     * @param field The field descriptor
     * @param value The field value
     * @return A Value representation
     */
    private Value fieldToValue(FieldDescriptor field, Object value) {
        if (value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        }

        return switch (field.getJavaType()) {
            case INT, FLOAT, DOUBLE -> Value.newBuilder().setNumberValue(((Number) value).doubleValue()).build();
            // int64 encodes as a string per the proto3 JSON convention (doubles lose precision above 2^53)
            case LONG -> Value.newBuilder().setStringValue(Long.toString(((Number) value).longValue())).build();
            case BOOLEAN -> Value.newBuilder().setBoolValue((Boolean) value).build();
            case STRING -> Value.newBuilder().setStringValue((String) value).build();
            case ENUM -> {
                EnumValueDescriptor enumValue = (EnumValueDescriptor) value;
                yield Value.newBuilder().setStringValue(enumValue.getName()).build();
            }
            case MESSAGE -> {
                if (value instanceof Struct) {
                    yield Value.newBuilder().setStructValue((Struct) value).build();
                }
                yield Value.newBuilder().setStructValue(messageToStruct((Message) value)).build();
            }
            case BYTE_STRING -> {
                // Encode as base64 string
                ByteString bytes = (ByteString) value;
                yield Value.newBuilder()
                        .setStringValue(Base64.getEncoder().encodeToString(bytes.toByteArray()))
                        .build();
            }
            default -> Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        };
    }

    /**
     * Converts a protobuf Value to a field value based on the field descriptor.
     *
     * @param value The Value to convert
     * @param field The target field descriptor
     * @return The converted value appropriate for the field type
     */
    private Object valueToField(Value value, FieldDescriptor field) {
        if (value.getKindCase() == Value.KindCase.NULL_VALUE) {
            return null;
        }

        if (field.isRepeated()) {
            List<Value> elements = value.getKindCase() == Value.KindCase.LIST_VALUE
                    ? value.getListValue().getValuesList()
                    : List.of(value);
            List<Object> converted = new ArrayList<>(elements.size());
            for (Value element : elements) {
                Object item = singleValueToField(element, field);
                if (item != null) {
                    converted.add(item);
                }
            }
            return converted;
        }

        return singleValueToField(value, field);
    }

    /**
     * Converts a single (non-list) protobuf Value to a field element value based on the field descriptor.
     *
     * @param value The Value to convert
     * @param field The target field descriptor
     * @return The converted value appropriate for the field's element type
     */
    private Object singleValueToField(Value value, FieldDescriptor field) {
        if (value.getKindCase() == Value.KindCase.NULL_VALUE) {
            return null;
        }

        switch (field.getJavaType()) {
            case INT:
                // Accept the string encoding and reject non-integral numbers, mirroring LONG.
                if (value.getKindCase() == Value.KindCase.STRING_VALUE) {
                    try {
                        return Integer.parseInt(value.getStringValue());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Value \"" + value.getStringValue() + "\" is not a valid int32 for field "
                                        + field.getFullName(), e);
                    }
                }
                return numberToInt(value.getNumberValue(), field);
            case LONG:
                // Accept the string encoding (proto3 JSON convention) and, for backward
                // compatibility, numbers that are exactly representable as int64.
                if (value.getKindCase() == Value.KindCase.STRING_VALUE) {
                    return Long.parseLong(value.getStringValue());
                }
                return numberToLong(value.getNumberValue(), field);
            case FLOAT:
                return (float) value.getNumberValue();
            case DOUBLE:
                return value.getNumberValue();
            case BOOLEAN:
                if (value.getKindCase() != Value.KindCase.BOOL_VALUE) {
                    throw new IllegalArgumentException(
                            "Expected a bool value for field " + field.getFullName()
                                    + " but got " + value.getKindCase());
                }
                return value.getBoolValue();
            case STRING:
                if (value.getKindCase() != Value.KindCase.STRING_VALUE) {
                    throw new IllegalArgumentException(
                            "Expected a string value for field " + field.getFullName()
                                    + " but got " + value.getKindCase());
                }
                return value.getStringValue();
            case ENUM:
                EnumDescriptor enumDescriptor = field.getEnumType();
                EnumValueDescriptor enumValue = enumDescriptor.findValueByName(value.getStringValue());
                if (enumValue == null) {
                    throw new IllegalArgumentException(
                            "Unknown value \"" + value.getStringValue() + "\" for enum "
                                    + enumDescriptor.getFullName() + " on field " + field.getFullName());
                }
                return enumValue;
            case MESSAGE:
                if (field.getMessageType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                    return value.getStructValue();
                }
                if (value.getKindCase() == Value.KindCase.STRUCT_VALUE) {
                    return structToMessage(value.getStructValue(), field.getMessageType());
                }
                return null;
            case BYTE_STRING:
                // Decode from base64 string (matches the encoding in fieldToValue)
                return ByteString.copyFrom(Base64.getDecoder().decode(value.getStringValue()));
            default:
                return null;
        }
    }

    /**
     * Converts a JSON number to a long, rejecting values that cannot be represented exactly.
     *
     * @param number The number to convert
     * @param field The target field descriptor (used for error reporting)
     * @return The exact long value
     * @throws IllegalArgumentException if the number is non-integral or outside long range
     */
    private static long numberToLong(double number, FieldDescriptor field) {
        long result = (long) number;
        if ((double) result != number) {
            throw new IllegalArgumentException(
                    "Value " + number + " cannot be represented exactly as int64 for field "
                            + field.getFullName());
        }
        return result;
    }

    /**
     * Converts a JSON number to an int, rejecting NaN, non-integral values, and values outside
     * int32 range.
     *
     * @param number The number to convert
     * @param field The target field descriptor (used for error reporting)
     * @return The exact int value
     * @throws IllegalArgumentException if the number is non-integral or outside int32 range
     */
    private static int numberToInt(double number, FieldDescriptor field) {
        long asLong = (long) number;
        if ((double) asLong != number) {
            throw new IllegalArgumentException(
                    "Value " + number + " cannot be represented exactly as int32 for field "
                            + field.getFullName());
        }
        int result = (int) asLong;
        if (result != asLong) {
            throw new IllegalArgumentException(
                    "Value " + number + " is out of int32 range for field " + field.getFullName());
        }
        return result;
    }

    /**
     * Attempts to convert a value to match the expected type of a field.
     * Provides intelligent type coercion where possible.
     *
     * @param value The value to convert
     * @param field The target field descriptor
     * @return The converted value, or the original if no conversion is needed
     * @throws IllegalArgumentException if conversion is not possible
     */
    public Object convertToFieldType(Object value, FieldDescriptor field) {
        if (value == null) {
            return null;
        }

        // If value is already compatible, return as-is
        if (isCompatibleType(value, field)) {
            return value;
        }

        // Attempt type conversions
        switch (field.getJavaType()) {
            case INT:
                if (value instanceof Number) return ((Number) value).intValue();
                if (value instanceof String) return Integer.parseInt((String) value);
                break;
            case LONG:
                if (value instanceof Number) return ((Number) value).longValue();
                if (value instanceof String) return Long.parseLong((String) value);
                break;
            case FLOAT:
                if (value instanceof Number) return ((Number) value).floatValue();
                if (value instanceof String) return Float.parseFloat((String) value);
                break;
            case DOUBLE:
                if (value instanceof Number) return ((Number) value).doubleValue();
                if (value instanceof String) return Double.parseDouble((String) value);
                break;
            case BOOLEAN:
                if (value instanceof String) return Boolean.parseBoolean((String) value);
                break;
            case STRING:
                return value.toString();
            case MESSAGE:
                if (value instanceof Struct &&
                    !field.getMessageType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                    return structToMessage((Struct) value, field.getMessageType());
                }
                break;
        }

        throw new IllegalArgumentException(
            "Cannot convert value of type " + value.getClass().getName() +
            " to field type " + field.getJavaType());
    }

    /**
     * Checks if a value is compatible with a field type.
     *
     * @param value The value to check
     * @param field The field descriptor
     * @return true if the value can be assigned to the field without conversion
     */
    private boolean isCompatibleType(Object value, FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case INT -> value instanceof Integer;
            case LONG -> value instanceof Long;
            case FLOAT -> value instanceof Float;
            case DOUBLE -> value instanceof Double;
            case BOOLEAN -> value instanceof Boolean;
            case STRING -> value instanceof String;
            case BYTE_STRING -> value instanceof ByteString;
            case ENUM -> value instanceof EnumValueDescriptor;
            case MESSAGE -> value instanceof Message;
            default -> false;
        };
    }
}

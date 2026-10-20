package ai.protomolt.proto.mapper;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.List;

/**
 * Package-private {@link Struct} and {@link Value} codec and key-chain mutator for
 * {@link ProtoFieldMapperImpl}.
 */
final class StructValueCodec {

    private static final String STRUCT_FULL_NAME = Struct.getDescriptor().getFullName();

    private StructValueCodec() {}

    /**
     * Reports whether {@code descriptor} represents {@code google.protobuf.Struct}.
     */
    static boolean isStructDescriptor(Descriptor descriptor) {
        return descriptor.getFullName().equals(STRUCT_FULL_NAME);
    }

    /**
     * Reports whether the final path part addresses a key inside a {@code google.protobuf.Struct},
     * determined from the descriptors alone so it holds for paths whose parents are unset.
     */
    static boolean pathEntersStruct(Descriptor root, String[] parts) {
        Descriptor current = root;
        for (int i = 0; i < parts.length - 1; i++) {
            if (isStructDescriptor(current)) {
                return true;
            }
            FieldDescriptor fd = current.findFieldByName(parts[i]);
            if (fd == null || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                return false;
            }
            current = fd.getMessageType();
        }
        return isStructDescriptor(current);
    }

    /**
     * Reads {@code key} from a concrete {@link Struct}, throwing {@link MappingException} when an
     * intermediate key is missing.
     */
    static Object readStructKey(
            Struct struct,
            String key,
            boolean isIntermediate,
            String path,
            String rule) throws MappingException {
        Value value = struct.getFieldsMap().get(key);
        if (value == null && isIntermediate) {
            throw new MappingException(
                    "Cannot resolve path '" + path + "': key '" + key + "' not found in struct",
                    rule);
        }
        return unwrapValue(value);
    }

    /**
     * Reads {@code key} from a {@link MessageOrBuilder} whose descriptor is
     * {@code google.protobuf.Struct} (including {@code DynamicMessage} instances).
     */
    static Object readDynamicStructKey(
            MessageOrBuilder currentMsg,
            String key,
            boolean isIntermediate,
            String path,
            String rule) throws MappingException {
        try {
            Message structMessage = currentMsg instanceof Message.Builder builder
                    ? builder.build()
                    : (Message) currentMsg;
            Struct parsed = Struct.parseFrom(structMessage.toByteString());
            return readStructKey(parsed, key, isIntermediate, path, rule);
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to read struct path '" + path + "'", e, rule);
        }
    }

    /**
     * Sets {@code value} at the nested {@code structKeys} chain inside {@code containerBuilder}.
     */
    static void setStructValue(
            Message.Builder containerBuilder,
            String[] structKeys,
            Object value,
            String rule) throws MappingException {
        try {
            // Round-trip through bytes: the builder's Struct descriptor may be a
            // runtime-compiled copy, so typed merges would reject it; the wire form
            // is identical either way.
            Struct currentStruct = Struct.parseFrom(containerBuilder.build().toByteString());
            Struct modifiedStruct = putStructKey(currentStruct, structKeys, 0, wrapValue(value));
            containerBuilder.clear().mergeFrom(modifiedStruct.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to rebuild struct for setting value", e, rule);
        }
    }

    /**
     * Appends {@code value} to the list at the nested {@code structKeys} chain inside
     * {@code containerBuilder}, creating the list if needed.
     */
    static void appendStructValue(
            Message.Builder containerBuilder,
            String[] structKeys,
            Object value,
            String rule) throws MappingException {
        try {
            Struct currentStruct = Struct.parseFrom(containerBuilder.build().toByteString());
            Struct modifiedStruct = appendStructKey(currentStruct, structKeys, 0, value);
            containerBuilder.clear().mergeFrom(modifiedStruct.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to rebuild struct for appending value", e, rule);
        }
    }

    /**
     * Removes the key at the nested {@code structKeys} chain inside {@code containerBuilder}.
     */
    static void clearStructKey(
            Message.Builder containerBuilder,
            String[] structKeys,
            String rule) throws MappingException {
        try {
            Struct currentStruct = Struct.parseFrom(containerBuilder.build().toByteString());
            Struct modifiedStruct = removeStructKey(currentStruct, structKeys, 0);
            containerBuilder.clear().mergeFrom(modifiedStruct.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to rebuild struct for clearing field", e, rule);
        }
    }

    /**
     * Wraps a Java value into a {@link Value}.
     */
    static Value wrapValue(Object value) {
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
            case List<?> list -> {
                ListValue.Builder listBuilder = ListValue.newBuilder();
                for (Object item : list) {
                    listBuilder.addValues(wrapValue(item));
                }
                return Value.newBuilder().setListValue(listBuilder).build();
            }
            default -> {
            }
        }
        throw new IllegalArgumentException(
                "Cannot wrap unsupported type to Protobuf Value: " + value.getClass().getName());
    }

    /**
     * Unwraps a {@link Value} into its Java representation.
     */
    static Object unwrapValue(Value value) {
        if (value == null || value.getKindCase() == Value.KindCase.NULL_VALUE) {
            return null;
        }
        return switch (value.getKindCase()) {
            case NUMBER_VALUE -> value.getNumberValue();
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case STRUCT_VALUE -> value.getStructValue();
            case LIST_VALUE ->
                    value.getListValue().getValuesList().stream().map(StructValueCodec::unwrapValue).toList();
            default -> null;
        };
    }

    /** Sets {@code leaf} at the nested Struct key chain, creating intermediate structs as needed. */
    private static Struct putStructKey(Struct struct, String[] keys, int index, Value leaf) {
        Struct.Builder builder = struct.toBuilder();
        String key = keys[index];
        if (index == keys.length - 1) {
            builder.putFields(key, leaf);
        } else {
            builder.putFields(key, Value.newBuilder()
                    .setStructValue(putStructKey(childStruct(struct, key), keys, index + 1, leaf))
                    .build());
        }
        return builder.build();
    }

    /** Appends {@code value} to the list at the nested Struct key chain, preserving an existing scalar. */
    private static Struct appendStructKey(Struct struct, String[] keys, int index, Object value) {
        Struct.Builder builder = struct.toBuilder();
        String key = keys[index];
        if (index == keys.length - 1) {
            Value existing = struct.getFieldsMap().get(key);
            ListValue.Builder listBuilder;
            if (existing != null && existing.getKindCase() == Value.KindCase.LIST_VALUE) {
                listBuilder = existing.getListValue().toBuilder();
            } else {
                listBuilder = ListValue.newBuilder();
                // An existing non-list value becomes the first element rather than being discarded.
                if (existing != null && existing.getKindCase() != Value.KindCase.NULL_VALUE
                        && existing.getKindCase() != Value.KindCase.KIND_NOT_SET) {
                    listBuilder.addValues(existing);
                }
            }
            if (value instanceof List<?>) {
                for (Object item : (List<?>) value) {
                    listBuilder.addValues(wrapValue(item));
                }
            } else {
                listBuilder.addValues(wrapValue(value));
            }
            builder.putFields(key, Value.newBuilder().setListValue(listBuilder).build());
        } else {
            builder.putFields(key, Value.newBuilder()
                    .setStructValue(appendStructKey(childStruct(struct, key), keys, index + 1, value))
                    .build());
        }
        return builder.build();
    }

    /** Removes the nested Struct key; a missing or non-struct intermediate key makes this a no-op. */
    private static Struct removeStructKey(Struct struct, String[] keys, int index) {
        Struct.Builder builder = struct.toBuilder();
        String key = keys[index];
        if (index == keys.length - 1) {
            builder.removeFields(key);
            return builder.build();
        }
        Value existing = struct.getFieldsMap().get(key);
        if (existing == null || existing.getKindCase() != Value.KindCase.STRUCT_VALUE) {
            return struct;
        }
        builder.putFields(key, Value.newBuilder()
                .setStructValue(removeStructKey(existing.getStructValue(), keys, index + 1))
                .build());
        return builder.build();
    }

    /** Existing struct value at {@code key}, or an empty struct when absent or not a struct. */
    private static Struct childStruct(Struct struct, String key) {
        Value existing = struct.getFieldsMap().get(key);
        return existing != null && existing.getKindCase() == Value.KindCase.STRUCT_VALUE
                ? existing.getStructValue()
                : Struct.getDefaultInstance();
    }
}

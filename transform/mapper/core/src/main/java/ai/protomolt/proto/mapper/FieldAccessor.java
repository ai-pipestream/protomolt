package ai.protomolt.proto.mapper;

import ai.protomolt.proto.helpers.AnyHandler;
import ai.protomolt.proto.helpers.TypeConverter;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;

import java.util.List;

/**
 * Package-private field reader and writer for {@link ProtoFieldMapperImpl}, delegating path and
 * literal resolution to {@link FieldPathResolver} and {@link Struct} operations to
 * {@link StructValueCodec}.
 */
final class FieldAccessor {

    private final AnyHandler anyHandler;
    private final TypeConverter typeConverter;

    FieldAccessor(AnyHandler anyHandler, TypeConverter typeConverter) {
        this.anyHandler = anyHandler;
        this.typeConverter = typeConverter;
    }

    Object getValue(MessageOrBuilder root, String path, String rule) throws MappingException {
        return getValue(root, path, rule, false);
    }

    Object getValue(MessageOrBuilder root, String path, String rule, boolean includeDefaults)
            throws MappingException {
        String trimmedPath = path.trim();
        FieldPathResolver.LiteralResult literal = FieldPathResolver.tryParseLiteral(trimmedPath, rule);
        if (literal.matched()) {
            return literal.value();
        }

        String[] parts = FieldPathResolver.splitPath(trimmedPath);
        Object current = root;

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            boolean isIntermediate = i < parts.length - 1;
            switch (current) {
                case null -> throw new MappingException(
                        "Cannot resolve path '" + path + "': intermediate value is null at segment '" + part + "'",
                        rule);
                case Struct struct ->
                        current = StructValueCodec.readStructKey(struct, part, isIntermediate, path, rule);
                case MessageOrBuilder currentMsg -> {
                    if (StructValueCodec.isStructDescriptor(currentMsg.getDescriptorForType())) {
                        current = StructValueCodec.readDynamicStructKey(
                                currentMsg, part, isIntermediate, path, rule);
                        continue;
                    }
                    FieldDescriptor fd = FieldPathResolver.findField(
                            currentMsg.getDescriptorForType(), part, rule);

                    if (!isIntermediate) {
                        if (!fd.isRepeated() && !currentMsg.hasField(fd)) {
                            // Implicit-presence proto3 fields at default (false/0/"") have no
                            // presence; surface the default only when the caller asked for it.
                            if (includeDefaults && !fd.hasPresence()) {
                                return currentMsg.getField(fd);
                            }
                            return null;
                        }
                        Object fieldValue = currentMsg.getField(fd);
                        if (FieldPathResolver.isAnyMessage(fieldValue)) {
                            Any any = FieldPathResolver.toAny(fieldValue, rule);
                            try {
                                return anyHandler.unpack(any);
                            } catch (InvalidProtocolBufferException e) {
                                throw new MappingException("Failed to unpack Any field '" + part
                                        + "' (type url '" + any.getTypeUrl()
                                        + "'); register the type's descriptor with the DescriptorRegistry to unpack it",
                                        e, rule);
                            }
                        }
                        return fieldValue;
                    } else {
                        if (fd.isRepeated() || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                            throw new MappingException(
                                    "Path '" + path + "' attempts to traverse through non-message or repeated field '" + part + "'",
                                    rule);
                        }
                        if (!currentMsg.hasField(fd)) {
                            throw MappingException.absentIntermediate(
                                    "Path '" + path + "' is invalid because intermediate field '"
                                            + part + "' is not set.",
                                    rule);
                        }
                        Object fieldValue = currentMsg.getField(fd);
                        if (FieldPathResolver.isAnyMessage(fieldValue)) {
                            try {
                                current = anyHandler.unpack(FieldPathResolver.toAny(fieldValue, rule));
                            } catch (InvalidProtocolBufferException e) {
                                throw new MappingException(
                                        "Failed to unpack Any field '" + part + "' during path traversal",
                                        e, rule);
                            }
                        } else {
                            current = fieldValue;
                        }
                    }
                }
                default -> throw new MappingException(
                        "Cannot resolve path '" + path + "': tried to traverse through a non-message, non-struct value at '" + part + "'",
                        rule);
            }
        }
        return current;
    }

    void setValue(Message.Builder root, String path, Object value, String rule)
            throws MappingException {
        // Assigning null clears the target field, so absent parents must not be materialised:
        // that would flip hasX() on those parents while clearing something that was never set.
        // Struct paths are excluded because a null there is a stored NullValue, not a clear.
        boolean materializeParents = value != null
                || StructValueCodec.pathEntersStruct(
                        root.getDescriptorForType(), FieldPathResolver.splitPath(path));
        FieldPathResolver.PathResolutionResult result =
                FieldPathResolver.resolvePathToFinalContainer(root, path, rule, materializeParents);
        if (result == null) {
            return;
        }
        String fieldName = result.finalPathPart;
        Message.Builder containerBuilder = (Message.Builder) result.container;

        if (StructValueCodec.isStructDescriptor(containerBuilder.getDescriptorForType())) {
            StructValueCodec.setStructValue(containerBuilder, result.structKeys, value, rule);
        } else {
            FieldDescriptor fd = FieldPathResolver.findField(
                    containerBuilder.getDescriptorForType(), fieldName, rule);

            // A null value (null literal or unset source field) clears the target field,
            // mirroring the null semantics already implemented for Struct paths.
            if (value == null) {
                containerBuilder.clearField(fd);
                result.commit();
                return;
            }

            if (FieldPathResolver.isAnyField(fd)) {
                if (FieldPathResolver.isAnyMessage(value)) {
                    containerBuilder.setField(fd, FieldPathResolver.toAny(value, rule));
                } else if (value instanceof Message message) {
                    Any packedAny = anyHandler.pack(message);
                    containerBuilder.setField(fd, packedAny);
                } else {
                    throw new MappingException(
                            "Cannot pack non-Message value into Any field: " + fieldName, rule);
                }
            } else {
                Object convertedValue = typeConverter.convertToFieldType(value, fd);
                containerBuilder.setField(fd, convertedValue);
            }
        }
        result.commit();
    }

    void appendValue(Message.Builder root, String path, Object value, String rule)
            throws MappingException {
        FieldPathResolver.PathResolutionResult result =
                FieldPathResolver.resolvePathToFinalContainer(root, path, rule, true);
        if (!(result.container instanceof Message.Builder finalBuilder)) {
            throw new MappingException("Cannot append to a non-message field", rule);
        }
        if (StructValueCodec.isStructDescriptor(finalBuilder.getDescriptorForType())) {
            StructValueCodec.appendStructValue(finalBuilder, result.structKeys, value, rule);
            result.commit();
            return;
        }

        FieldDescriptor fd = FieldPathResolver.findField(
                finalBuilder.getDescriptorForType(), result.finalPathPart, rule);

        if (!fd.isRepeated()) {
            throw new MappingException("Cannot append: field '" + fd.getName() + "' is not repeated", rule);
        }

        // Convert each appended element to the field's element type, mirroring setValue.
        // A repeated field has no representation for a null element, so nulls are rejected
        // rather than reaching addRepeatedField, which would throw a bare NullPointerException.
        if (value instanceof List<?>) {
            for (Object item : (List<?>) value) {
                if (item == null) {
                    throw new MappingException("Cannot append null to field '" + fd.getName() + "'", rule);
                }
                finalBuilder.addRepeatedField(fd, typeConverter.convertToFieldType(item, fd));
            }
        } else {
            if (value == null) {
                throw new MappingException("Cannot append null to field '" + fd.getName() + "'", rule);
            }
            finalBuilder.addRepeatedField(fd, typeConverter.convertToFieldType(value, fd));
        }
        result.commit();
    }

    void clearField(Message.Builder root, String path, String rule) throws MappingException {
        FieldPathResolver.PathResolutionResult result =
                FieldPathResolver.resolvePathToFinalContainer(root, path, rule, false);
        if (result == null) {
            return; // Clearing through an unset parent is a no-op; do not materialise parents.
        }
        String fieldName = result.finalPathPart;
        Message.Builder containerBuilder = (Message.Builder) result.container;

        if (StructValueCodec.isStructDescriptor(containerBuilder.getDescriptorForType())) {
            StructValueCodec.clearStructKey(containerBuilder, result.structKeys, rule);
        } else {
            FieldDescriptor fd = FieldPathResolver.findField(
                    containerBuilder.getDescriptorForType(), fieldName, rule);
            containerBuilder.clearField(fd);
        }
        result.commit();
    }
}

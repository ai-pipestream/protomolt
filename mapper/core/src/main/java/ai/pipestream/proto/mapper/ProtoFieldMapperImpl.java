package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.helpers.AnyHandler;
import ai.pipestream.proto.helpers.TypeConverter;

import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A streamlined utility for dynamically mapping fields between Protocol Buffer messages.
 *
 * <p>This class refactors the original concept of a multi-component mapping system
 * (parser, resolver, executor) into a more consolidated design. The core logic is
 * encapsulated within the `FieldAccessor`, which handles both reading and writing
 * field values based on a dot-notation path, similar to a simplified object property selector.
 *
 * <p><b>Features:</b>
 * <ul>
 * <li>Handles literal value assignments (strings, booleans, numbers, null).</li>
 * <li>Correctly reads from and writes to `google.protobuf.Struct` fields, handling type conversions from DynamicMessage.</li>
 * <li>Supports `google.protobuf.Any` field packing and unpacking.</li>
 * <li>Provides type conversion between different protobuf types.</li>
 * <li>Manages proto descriptors for dynamic message handling.</li>
 * <li>Provides precise error messages for invalid paths.</li>
 * </ul>
 */
public class ProtoFieldMapperImpl implements ProtoFieldMapper {

    private final RuleParser ruleParser = new RuleParser();
    private final FieldAccessor fieldAccessor;
    private final DescriptorRegistry descriptorRegistry;
    private final AnyHandler anyHandler;
    private final TypeConverter typeConverter;

    /**
     * CDI constructor — receives the application-scoped DescriptorRegistry
     * which has CDI-injected loaders (Apicurio, Google, etc.) already wired.
     *
     * @param descriptorRegistry The descriptor registry to use
     */
    public ProtoFieldMapperImpl(DescriptorRegistry descriptorRegistry) {
        this.descriptorRegistry = descriptorRegistry;
        this.anyHandler = new AnyHandler(descriptorRegistry);
        this.typeConverter = new TypeConverter();
        this.fieldAccessor = new FieldAccessor(anyHandler, typeConverter);
    }

    /**
     * Gets the descriptor registry for this mapper.
     * Use this to register descriptors for Any field handling.
     *
     * @return The descriptor registry
     */
    @Override
    public DescriptorRegistry getDescriptorRegistry() {
        return descriptorRegistry;
    }

    /**
     * Gets the AnyHandler for this mapper.
     * Use this for manual Any field operations.
     *
     * @return The AnyHandler
     */
    @Override
    public AnyHandler getAnyHandler() {
        return anyHandler;
    }

    /**
     * Reads a value from a protobuf message/builder using a dot-notation path.
     *
     * <p>This exposes the same path semantics as the rule engine, but without needing to build
     * a string rule (useful for services that already have a mapping model).
     * @param source the source
     * @param path the path
     * @return the value
     * @throws MappingException if the operation fails
     */
    @Override
    public Object getValue(MessageOrBuilder source, String path) throws MappingException {
        return fieldAccessor.getValue(source, path, path);
    }

    /**
     * Reads a value like {@link #getValue(MessageOrBuilder, String)}, optionally surfacing the
     * default value ({@code false} / {@code 0} / {@code ""}) of proto3 implicit-presence leaf
     * fields instead of {@code null}.
     * @param source the source
     * @param path the path
     * @param includeDefaults when true, implicit-presence leaves at default return the default
     * @return the value
     * @throws MappingException if the operation fails
     */
    @Override
    public Object getValue(MessageOrBuilder source, String path, boolean includeDefaults)
            throws MappingException {
        return fieldAccessor.getValue(source, path, path, includeDefaults);
    }

    /**
     * Writes a value to a protobuf builder using a dot-notation path.
     * @param targetBuilder the target builder
     * @param path the path
     * @param value the value
     * @throws MappingException if the operation fails
     */
    @Override
    public void setValue(Message.Builder targetBuilder, String path, Object value) throws MappingException {
        fieldAccessor.setValue(targetBuilder, path, value, path);
    }

    /**
     * Appends a value to a repeated field using a dot-notation path.
     * @param targetBuilder the target builder
     * @param path the path
     * @param value the value
     * @throws MappingException if the operation fails
     */
    @Override
    public void appendValue(Message.Builder targetBuilder, String path, Object value) throws MappingException {
        fieldAccessor.appendValue(targetBuilder, path, value, path);
    }

    /**
     * Clears a field using a dot-notation path.
     * @param targetBuilder the target builder
     * @param path the path
     * @throws MappingException if the operation fails
     */
    @Override
    public void clearField(Message.Builder targetBuilder, String path) throws MappingException {
        fieldAccessor.clearField(targetBuilder, path, path);
    }

    /**
     * Executes the rule syntax directly against a single mutable builder.
     *
     * <p>Unlike {@link #map(Message, Message.Builder, List)}, this reads from the same builder
     * as it is being mutated, so later rules can depend on earlier rules without repeatedly
     * building intermediate immutable messages.</p>
     * @param builder the builder
     * @param rules the rules
     * @throws MappingException if the operation fails
     */
    @Override
    public void mapInPlace(Message.Builder builder, List<String> rules) throws MappingException {
        List<MappingRule> parsedRules = ruleParser.parse(rules);

        for (MappingRule rule : parsedRules) {
            try {
                Object value = null;
                if (rule.sourcePath != null) {
                    value = fieldAccessor.getValue(builder, rule.sourcePath, rule.originalRule);
                }

                switch (rule.operation) {
                    case ASSIGN:
                        fieldAccessor.setValue(builder, rule.targetPath, value, rule.originalRule);
                        break;
                    case APPEND:
                        fieldAccessor.appendValue(builder, rule.targetPath, value, rule.originalRule);
                        break;
                    case CLEAR:
                        fieldAccessor.clearField(builder, rule.targetPath, rule.originalRule);
                        break;
                }
            } catch (Exception e) {
                if (e instanceof MappingException) {
                    throw e;
                }
                throw new MappingException("Failed to execute rule: " + rule.originalRule, e, rule.originalRule);
            }
        }
    }

    /**
     * Maps fields from a source message to a target message builder based on a list of rules.
     *
     * @param source The source protobuf message to read values from
     * @param targetBuilder The target message builder to write values to
     * @param rules List of mapping rules in string format
     * @throws MappingException If any rule cannot be applied due to invalid paths or type mismatches
     */
    @Override
    public void map(Message source, Message.Builder targetBuilder, List<String> rules) throws MappingException {
        List<MappingRule> parsedRules = ruleParser.parse(rules);

        for (MappingRule rule : parsedRules) {
            try {
                Object value = null;
                // For assign/append, we need a value from the source path.
                // For clear, the source path is null and so is the value.
                if (rule.sourcePath != null) {
                    value = fieldAccessor.getValue(source, rule.sourcePath, rule.originalRule);
                }

                switch (rule.operation) {
                    case ASSIGN:
                        fieldAccessor.setValue(targetBuilder, rule.targetPath, value, rule.originalRule);
                        break;
                    case APPEND:
                        fieldAccessor.appendValue(targetBuilder, rule.targetPath, value, rule.originalRule);
                        break;
                    case CLEAR:
                        fieldAccessor.clearField(targetBuilder, rule.targetPath, rule.originalRule);
                        break;
                }
            } catch (Exception e) {
                if (e instanceof MappingException) {
                    throw e; // Re-throw if it's already our specific exception
                }
                // Wrap other exceptions with the rule that caused them.
                throw new MappingException("Failed to execute rule: " + rule.originalRule, e, rule.originalRule);
            }
        }
    }

    // --- Nested Helper Classes ---

    private static class MappingRule {
        final String targetPath;
        final String sourcePath;
        final Operation operation;
        final String originalRule;

        MappingRule(String targetPath, String sourcePath, Operation operation, String originalRule) {
            this.targetPath = targetPath;
            this.sourcePath = sourcePath;
            this.operation = operation;
            this.originalRule = originalRule;
        }
    }

    private enum Operation { ASSIGN, APPEND, CLEAR }

    private static class RuleParser {
        private static final Pattern ASSIGN_PATTERN = Pattern.compile("^\\s*([^=+\\s]+)\\s*=\\s*(.+)\\s*$");
        private static final Pattern APPEND_PATTERN = Pattern.compile("^\\s*([^+\\s]+)\\s*\\+=\\s*(.+)\\s*$");
        private static final Pattern CLEAR_PATTERN = Pattern.compile("^\\s*-\\s*(\\S+)\\s*$");

        public List<MappingRule> parse(List<String> ruleStrings) throws MappingException {
            List<MappingRule> rules = new ArrayList<>();
            for (String ruleString : ruleStrings) {
                if (ruleString == null || ruleString.trim().isEmpty()) continue;
                Matcher assignMatcher = ASSIGN_PATTERN.matcher(ruleString);
                if (assignMatcher.matches()) {
                    rules.add(new MappingRule(assignMatcher.group(1), assignMatcher.group(2).trim(), Operation.ASSIGN, ruleString));
                    continue;
                }
                Matcher appendMatcher = APPEND_PATTERN.matcher(ruleString);
                if (appendMatcher.matches()) {
                    rules.add(new MappingRule(appendMatcher.group(1), appendMatcher.group(2).trim(), Operation.APPEND, ruleString));
                    continue;
                }
                Matcher clearMatcher = CLEAR_PATTERN.matcher(ruleString);
                if (clearMatcher.matches()) {
                    rules.add(new MappingRule(clearMatcher.group(1), null, Operation.CLEAR, ruleString));
                    continue;
                }
                throw new MappingException("Invalid rule syntax", ruleString);
            }
            return rules;
        }
    }

    static class FieldAccessor {
        private static final String PATH_SEPARATOR_REGEX = "\\.";
        private final AnyHandler anyHandler;
        private final TypeConverter typeConverter;

        public FieldAccessor(AnyHandler anyHandler, TypeConverter typeConverter) {
            this.anyHandler = anyHandler;
            this.typeConverter = typeConverter;
        }

        public Object getValue(MessageOrBuilder root, String path, String rule) throws MappingException {
            return getValue(root, path, rule, false);
        }

        public Object getValue(MessageOrBuilder root, String path, String rule, boolean includeDefaults)
                throws MappingException {
            String trimmedPath = path.trim();
            // Check for literals first.
            switch (trimmedPath) {
                case "null" -> {
                    return null;
                }
                case "true" -> {
                    return true;
                }
                case "false" -> {
                    return false;
                }
            }
            if (trimmedPath.startsWith("\"") && trimmedPath.endsWith("\"")) {
                if (trimmedPath.length() == 1) throw new MappingException("Invalid empty quoted string literal", rule);
                return trimmedPath.substring(1, trimmedPath.length() - 1);
            }
            try {
                if (!trimmedPath.contains(" ") && (trimmedPath.matches("-?\\d+\\.\\d+") || trimmedPath.matches("-?\\d+"))) {
                    if (trimmedPath.contains(".")) {
                        return Double.parseDouble(trimmedPath);
                    }
                    return Long.parseLong(trimmedPath);
                }
            } catch (NumberFormatException e) {
                // Not a number, so it must be a path. Proceed.
            }

            // It's a path, so resolve it.
            String[] parts = trimmedPath.split(PATH_SEPARATOR_REGEX);
            Object current = root;

            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                switch (current) {
                    case null ->
                            throw new MappingException("Cannot resolve path '" + path + "': intermediate value is null at segment '" + part + "'", rule);
                    case Struct struct -> {
                        Value value = struct.getFieldsMap().get(part);
                        if (value == null && i < parts.length - 1) {
                            throw new MappingException("Cannot resolve path '" + path + "': key '" + part + "' not found in struct", rule);
                        }
                        current = unwrapValue(value);
                    }
                    case MessageOrBuilder currentMsg -> {
                        // DynamicMessage instances for google.protobuf.Struct are not concrete
                        // Struct objects, but retain the same wire representation.
                        if (currentMsg.getDescriptorForType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                            try {
                                Message structMessage = currentMsg instanceof Message.Builder builder
                                        ? builder.build() : (Message) currentMsg;
                                Value value = Struct.parseFrom(structMessage.toByteString()).getFieldsMap().get(part);
                                if (value == null && i < parts.length - 1) {
                                    throw new MappingException("Cannot resolve path '" + path + "': key '" + part + "' not found in struct", rule);
                                }
                                current = unwrapValue(value);
                                continue;
                            } catch (InvalidProtocolBufferException e) {
                                throw new MappingException("Failed to read struct path '" + path + "'", e, rule);
                            }
                        }
                        FieldDescriptor fd = findField(currentMsg.getDescriptorForType(), part, rule);

                        if (i == parts.length - 1) {
                            if (!fd.isRepeated() && !currentMsg.hasField(fd)) {
                                // Implicit-presence proto3 fields at default (false/0/"") have no
                                // presence; surface the default only when the caller asked for it.
                                if (includeDefaults && !fd.hasPresence()) {
                                    return currentMsg.getField(fd);
                                }
                                return null;
                            }
                            Object fieldValue = currentMsg.getField(fd);

                            // If the final field is an Any and we want to return the unpacked value
                            if (isAnyMessage(fieldValue)) {
                                Any any = toAny(fieldValue, rule);
                                try {
                                    return anyHandler.unpack(any);
                                } catch (InvalidProtocolBufferException e) {
                                    throw new MappingException("Failed to unpack Any field '" + part
                                            + "' (type url '" + any.getTypeUrl()
                                            + "'); register the type's descriptor with the DescriptorRegistry to unpack it", e, rule);
                                }
                            }
                            return fieldValue;
                        } else {
                            if (fd.isRepeated() || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                                throw new MappingException("Path '" + path + "' attempts to traverse through non-message or repeated field '" + part + "'", rule);
                            }
                            if (!currentMsg.hasField(fd)) {
                                throw new MappingException("Path '" + path + "' is invalid because intermediate field '" + part + "' is not set.", rule);
                            }
                            Object fieldValue = currentMsg.getField(fd);

                            // If field is an Any, unpack it before continuing traversal
                            if (isAnyMessage(fieldValue)) {
                                try {
                                    current = anyHandler.unpack(toAny(fieldValue, rule));
                                } catch (InvalidProtocolBufferException e) {
                                    throw new MappingException("Failed to unpack Any field '" + part + "' during path traversal", e, rule);
                                }
                            } else {
                                current = fieldValue;
                            }
                        }
                    }
                    default ->
                            throw new MappingException("Cannot resolve path '" + path + "': tried to traverse through a non-message, non-struct value at '" + part + "'", rule);
                }

            }
            return current;
        }

        public void setValue(Message.Builder root, String path, Object value, String rule) throws MappingException {
            PathResolutionResult result = resolvePathToFinalContainer(root, path, rule, true);
            String fieldName = result.finalPathPart;
            Message.Builder containerBuilder = (Message.Builder) result.container;

            if (containerBuilder.getDescriptorForType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                try {
                    // This is the key insight: build the generic message, then parse it back into a concrete Struct to get a proper builder.
                    Struct currentStruct = Struct.parseFrom(containerBuilder.build().toByteString());
                    Struct modifiedStruct = putStructKey(currentStruct, result.structKeys, 0, wrapValue(value));
                    containerBuilder.clear().mergeFrom(modifiedStruct);
                } catch(InvalidProtocolBufferException e) {
                    throw new MappingException("Failed to rebuild struct for setting value", e, rule);
                }
            } else {
                FieldDescriptor fd = findField(containerBuilder.getDescriptorForType(), fieldName, rule);

                // A null value (null literal or unset source field) clears the target field,
                // mirroring the null semantics already implemented for Struct paths.
                if (value == null) {
                    containerBuilder.clearField(fd);
                    return;
                }

                // Special handling for Any fields
                if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE &&
                    fd.getMessageType().getFullName().equals(Any.getDescriptor().getFullName())) {
                    // The target field is an Any - pack the value
                    if (isAnyMessage(value)) {
                        // Already an Any (possibly in DynamicMessage form), use it directly
                        containerBuilder.setField(fd, toAny(value, rule));
                    } else if (value instanceof Message) {
                        // Pack the message into an Any
                        Any packedAny = anyHandler.pack((Message) value);
                        containerBuilder.setField(fd, packedAny);
                    } else {
                        throw new MappingException("Cannot pack non-Message value into Any field: " + fieldName, rule);
                    }
                } else {
                    // Try to convert the value to the appropriate type if needed
                    Object convertedValue = typeConverter.convertToFieldType(value, fd);
                    containerBuilder.setField(fd, convertedValue);
                }
            }
        }

        public void appendValue(Message.Builder root, String path, Object value, String rule) throws MappingException {
            PathResolutionResult result = resolvePathToFinalContainer(root, path, rule, true);
            if (!(result.container instanceof Message.Builder finalBuilder)) {
                throw new MappingException("Cannot append to a non-message field", rule);
            }
            // Special handling for google.protobuf.Struct: treat "append" as "append to list value",
            // creating the list if needed.
            if (finalBuilder.getDescriptorForType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                try {
                    Struct currentStruct = Struct.parseFrom(finalBuilder.build().toByteString());
                    Struct modifiedStruct = appendStructKey(currentStruct, result.structKeys, 0, value);
                    finalBuilder.clear().mergeFrom(modifiedStruct);
                    return;
                } catch (InvalidProtocolBufferException e) {
                    throw new MappingException("Failed to rebuild struct for appending value", e, rule);
                }
            }

            FieldDescriptor fd = findField(finalBuilder.getDescriptorForType(), result.finalPathPart, rule);

            if (!fd.isRepeated()) {
                throw new MappingException("Cannot append: field '" + fd.getName() + "' is not repeated", rule);
            }

            // Convert each appended element to the field's element type, mirroring setValue.
            if (value instanceof List<?>) {
                for (Object item : (List<?>) value) finalBuilder.addRepeatedField(fd, typeConverter.convertToFieldType(item, fd));
            } else {
                finalBuilder.addRepeatedField(fd, typeConverter.convertToFieldType(value, fd));
            }
        }

        public void clearField(Message.Builder root, String path, String rule) throws MappingException {
            PathResolutionResult result = resolvePathToFinalContainer(root, path, rule, false);
            if (result == null) {
                return; // Clearing through an unset parent is a no-op; do not materialise parents.
            }
            String fieldName = result.finalPathPart;
            Message.Builder containerBuilder = (Message.Builder) result.container;

            if (containerBuilder.getDescriptorForType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                try {
                    Struct currentStruct = Struct.parseFrom(containerBuilder.build().toByteString());
                    Struct modifiedStruct = removeStructKey(currentStruct, result.structKeys, 0);
                    containerBuilder.clear().mergeFrom(modifiedStruct);
                } catch(InvalidProtocolBufferException e) {
                    throw new MappingException("Failed to rebuild struct for clearing field", e, rule);
                }
            } else {
                FieldDescriptor fd = findField(containerBuilder.getDescriptorForType(), fieldName, rule);
                containerBuilder.clearField(fd);
            }
        }

        private static class PathResolutionResult {
            final Object container;
            final String finalPathPart;
            /** When the container is a Struct: the chain of struct keys (ending with {@link #finalPathPart}). */
            final String[] structKeys;
            PathResolutionResult(Object container, String[] structKeys) {
                this.container = container;
                this.structKeys = structKeys;
                this.finalPathPart = structKeys[structKeys.length - 1];
            }
        }

        /**
         * Walks the parent segments of {@code path}, returning the builder that owns the final segment.
         * Once traversal reaches a {@code google.protobuf.Struct} field, the remaining segments are
         * Struct keys and are returned for the caller to resolve, mirroring the read-path semantics.
         *
         * @param materializeParents when {@code true}, unset intermediate parents are created;
         *        when {@code false} (clear operations), an unset parent returns {@code null}
         *        so the caller can treat the operation as a no-op.
         */
        private PathResolutionResult resolvePathToFinalContainer(Message.Builder root, String path, String rule, boolean materializeParents) throws MappingException {
            String[] parts = path.split(PATH_SEPARATOR_REGEX);
            Message.Builder currentBuilder = root;

            for (int i = 0; i < parts.length - 1; i++) {
                String part = parts[i];
                if (currentBuilder.getDescriptorForType().getFullName().equals(Struct.getDescriptor().getFullName())) {
                    return new PathResolutionResult(currentBuilder, Arrays.copyOfRange(parts, i, parts.length));
                }
                FieldDescriptor fd = findField(currentBuilder.getDescriptorForType(), part, rule);

                if (fd.isRepeated() || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    throw new MappingException("Path '" + path + "' attempts to traverse through non-singular message field '" + part + "'", rule);
                }
                if (!materializeParents && !currentBuilder.hasField(fd)) {
                    return null;
                }
                currentBuilder = currentBuilder.getFieldBuilder(fd);
            }
            return new PathResolutionResult(currentBuilder, new String[]{parts[parts.length - 1]});
        }

        /**
         * Detects google.protobuf.Any values by descriptor full name, so that DynamicMessage
         * instances of Any (e.g. from DynamicMessage.parseFrom) are recognised alongside
         * concrete {@link Any} objects.
         */
        private static boolean isAnyMessage(Object value) {
            return value instanceof MessageOrBuilder messageOrBuilder
                    && messageOrBuilder.getDescriptorForType().getFullName().equals(Any.getDescriptor().getFullName());
        }

        /** Converts a value recognised by {@link #isAnyMessage(Object)} into a concrete {@link Any}. */
        private static Any toAny(Object value, String rule) throws MappingException {
            if (value instanceof Any any) {
                return any;
            }
            Message message = value instanceof Message.Builder builder ? builder.build() : (Message) value;
            try {
                return Any.parseFrom(message.toByteString());
            } catch (InvalidProtocolBufferException e) {
                throw new MappingException("Failed to convert dynamic google.protobuf.Any message", e, rule);
            }
        }

        private FieldDescriptor findField(Descriptor d, String name, String fullPath) throws MappingException {
            FieldDescriptor fd = d.findFieldByName(name);
            if (fd == null) {
                throw new MappingException("Field '" + name + "' not found in message '" + d.getName() + "'", fullPath);
            }
            return fd;
        }

        /** Sets {@code leaf} at the nested Struct key chain, creating intermediate structs as needed. */
        private Struct putStructKey(Struct struct, String[] keys, int index, Value leaf) {
            Struct.Builder builder = struct.toBuilder();
            String key = keys[index];
            if (index == keys.length - 1) {
                builder.putFields(key, leaf);
            } else {
                builder.putFields(key, Value.newBuilder()
                        .setStructValue(putStructKey(childStruct(struct, key), keys, index + 1, leaf)).build());
            }
            return builder.build();
        }

        /** Appends {@code value} to the list at the nested Struct key chain, preserving an existing scalar. */
        private Struct appendStructKey(Struct struct, String[] keys, int index, Object value) {
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
                    for (Object item : (List<?>) value) listBuilder.addValues(wrapValue(item));
                } else {
                    listBuilder.addValues(wrapValue(value));
                }
                builder.putFields(key, Value.newBuilder().setListValue(listBuilder).build());
            } else {
                builder.putFields(key, Value.newBuilder()
                        .setStructValue(appendStructKey(childStruct(struct, key), keys, index + 1, value)).build());
            }
            return builder.build();
        }

        /** Removes the nested Struct key; a missing or non-struct intermediate key makes this a no-op. */
        private Struct removeStructKey(Struct struct, String[] keys, int index) {
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
                    .setStructValue(removeStructKey(existing.getStructValue(), keys, index + 1)).build());
            return builder.build();
        }

        /** Existing struct value at {@code key}, or an empty struct when absent or not a struct. */
        private static Struct childStruct(Struct struct, String key) {
            Value existing = struct.getFieldsMap().get(key);
            return existing != null && existing.getKindCase() == Value.KindCase.STRUCT_VALUE
                    ? existing.getStructValue() : Struct.getDefaultInstance();
        }

        private Value wrapValue(Object value) {
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
                    for (Object item : list) listBuilder.addValues(wrapValue(item));
                    return Value.newBuilder().setListValue(listBuilder).build();
                }
                default -> {
                }
            }
            throw new IllegalArgumentException("Cannot wrap unsupported type to Protobuf Value: " + value.getClass().getName());
        }

        private Object unwrapValue(Value value) {
            if (value == null || value.getKindCase() == Value.KindCase.NULL_VALUE) return null;
            return switch (value.getKindCase()) {
                case NUMBER_VALUE -> value.getNumberValue();
                case STRING_VALUE -> value.getStringValue();
                case BOOL_VALUE -> value.getBoolValue();
                case STRUCT_VALUE -> value.getStructValue();
                case LIST_VALUE ->
                        value.getListValue().getValuesList().stream().map(this::unwrapValue).collect(Collectors.toList());
                default -> null;
            };
        }
    }
}

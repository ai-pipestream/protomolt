package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider.Cancellation;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.RuleEvaluationException;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fail-closed input and row checks shared by every {@link ParquetRowMode}: row schema and selected
 * columns, unknown wire fields, unrecognized enum values in emitted columns, and declared
 * constraint/CEL rules.
 */
final class ParquetInputChecks {
    static final String ANY_FULL_NAME = "google.protobuf.Any";
    static final int MAX_MESSAGE_WALK_DEPTH = 128;

    /** The most schema fields one row schema may expand to, counted per column path. */
    static final int MAX_SCHEMA_FIELDS = 4096;

    private ParquetInputChecks() {}

    static void verifyFieldsCompatible(Descriptor expected, Descriptor actual)
            throws RenderFailure {
        record DescriptorPair(Descriptor expected, Descriptor actual) {}
        Deque<DescriptorPair> pending = new ArrayDeque<>();
        Set<DescriptorPair> checked = new HashSet<>();
        pending.push(new DescriptorPair(expected, actual));
        int fieldsSeen = 0;
        while (!pending.isEmpty()) {
            DescriptorPair pair = pending.pop();
            if (pair.expected() == pair.actual() || !checked.add(pair)) {
                continue;
            }
            for (FieldDescriptor expectedField : pair.expected().getFields()) {
                if (++fieldsSeen > MAX_SCHEMA_FIELDS) {
                    throw new RenderFailure(
                            RenderFailure.Kind.LIMIT,
                            "Source compatibility check exceeds maximum schema fields ("
                                    + MAX_SCHEMA_FIELDS + ")");
                }
                FieldDescriptor actualField =
                        pair.actual().findFieldByNumber(expectedField.getNumber());
                if (actualField == null
                        || !actualField.getName().equals(expectedField.getName())
                        || actualField.getType() != expectedField.getType()
                        || actualField.isRepeated() != expectedField.isRepeated()
                        || actualField.isMapField() != expectedField.isMapField()) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED,
                            "Source descriptor '" + pair.actual().getFullName()
                                    + "' is structurally incompatible on field '"
                                    + expectedField.getName() + "' (#" + expectedField.getNumber()
                                    + ")");
                }
                if (expectedField.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    if (!actualField.getMessageType().getFullName()
                            .equals(expectedField.getMessageType().getFullName())) {
                        throw new RenderFailure(
                                RenderFailure.Kind.UNSUPPORTED,
                                "Source descriptor '" + pair.actual().getFullName()
                                        + "' differs in message type for field '"
                                        + expectedField.getName() + "'");
                    }
                    // Descriptor pairs, not names, identify already checked cycles and shared types.
                    pending.push(new DescriptorPair(
                            expectedField.getMessageType(), actualField.getMessageType()));
                } else if (expectedField.getJavaType() == FieldDescriptor.JavaType.ENUM
                        && !sameEnumMapping(expectedField.getEnumType(), actualField.getEnumType())) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED,
                            "Source descriptor '" + pair.actual().getFullName()
                                    + "' differs in enum type or value mapping for field '"
                                    + expectedField.getName() + "'");
                }
            }
        }
    }

    private static boolean sameEnumMapping(EnumDescriptor expected, EnumDescriptor actual) {
        if (!expected.getFullName().equals(actual.getFullName())
                || expected.getValues().size() != actual.getValues().size()) {
            return false;
        }
        for (EnumValueDescriptor value : expected.getValues()) {
            EnumValueDescriptor supplied = actual.findValueByName(value.getName());
            if (supplied == null || supplied.getNumber() != value.getNumber()) {
                return false;
            }
            // Numeric projection must preserve the canonical name when aliases share a number.
            if (!expected.findValueByNumber(value.getNumber()).getName()
                    .equals(actual.findValueByNumber(value.getNumber()).getName())) {
                return false;
            }
        }
        return true;
    }

    static void validateRowSchemaAndColumns(
            Descriptor rowDescriptor, Set<String> selectedColumns) throws RenderFailure {
        if (rowDescriptor.getFields().isEmpty()) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Empty message schema '" + rowDescriptor.getFullName()
                            + "' has zero columns and cannot be rendered as Parquet");
        }
        List<FieldDescriptor> topFields;
        if (selectedColumns.isEmpty()) {
            topFields = rowDescriptor.getFields();
        } else {
            topFields = new ArrayList<>(selectedColumns.size());
            for (String column : selectedColumns) {
                FieldDescriptor field = rowDescriptor.findFieldByName(column);
                if (field == null) {
                    throw new RenderFailure(
                            RenderFailure.Kind.INVALID,
                            "Unknown column '" + column + "' on row schema '"
                                    + rowDescriptor.getFullName() + "'");
                }
                topFields.add(field);
            }
            if (topFields.isEmpty()) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "Selected column list produced zero columns on '"
                                + rowDescriptor.getFullName() + "'");
            }
        }

        Deque<String> ancestry = new ArrayDeque<>();
        ancestry.push(rowDescriptor.getFullName());
        int[] fieldsSeen = {0};
        for (FieldDescriptor field : topFields) {
            inspectFieldSchema(field, field.getName(), ancestry, fieldsSeen);
        }
        ancestry.pop();

        try {
            ProtoParquetSchemas.schema(
                    rowDescriptor, ProtoParquetSchemas.FieldIdResolver.NONE, selectedColumns);
        } catch (IllegalArgumentException schemaFailure) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED, schemaFailure.getMessage(), schemaFailure);
        }
    }

    /**
     * Checks one field and everything under it. A Parquet schema has a column per path, so
     * the same message type reached by two paths is walked twice; {@code fieldsSeen} counts
     * every field on every path and stops the walk at {@link #MAX_SCHEMA_FIELDS}. Nesting
     * stops at {@link #MAX_MESSAGE_WALK_DEPTH}, the same depth the message walks allow.
     */
    static void inspectFieldSchema(
            FieldDescriptor field, String path, Deque<String> ancestry, int[] fieldsSeen)
            throws RenderFailure {
        if (ancestry.size() > MAX_MESSAGE_WALK_DEPTH) {
            throw new RenderFailure(
                    RenderFailure.Kind.LIMIT,
                    "Row schema nests deeper than " + MAX_MESSAGE_WALK_DEPTH
                            + " messages (reached at '" + path + "')");
        }
        fieldsSeen[0]++;
        if (fieldsSeen[0] > MAX_SCHEMA_FIELDS) {
            throw new RenderFailure(
                    RenderFailure.Kind.LIMIT,
                    "Row schema expands to more than " + MAX_SCHEMA_FIELDS
                            + " fields across its column paths (reached at '" + path
                            + "'); select fewer columns or a declared projection");
        }
        if (field.getType() == FieldDescriptor.Type.GROUP) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Proto2 group field '" + path + "' is not supported for Parquet");
        }
        if (field.isMapField()) {
            FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
            inspectFieldSchema(valueField, path + ".value", ancestry, fieldsSeen);
            return;
        }
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return;
        }
        Descriptor nested = field.getMessageType();
        String fullName = nested.getFullName();
        if (ANY_FULL_NAME.equals(fullName)) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Opaque google.protobuf.Any field '" + path
                            + "' cannot be rendered directly to Parquet; select an unpacked Any "
                            + "occurrence source, exclude the column via ParquetRendererOptions."
                            + "columns, or configure a declared MessageProjection profile");
        }
        if (ProtoParquetSchemas.TIMESTAMP.equals(fullName)
                || ProtoParquetSchemas.JSON_TYPES.contains(fullName)) {
            return;
        }
        if (ancestry.contains(fullName)) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Recursive message schema at '" + path + "' ("
                            + String.join(" -> ", ancestry.reversed()) + " -> " + fullName
                            + ") cannot be rendered as a columnar Parquet file");
        }
        if (nested.getFields().isEmpty()) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Empty nested message schema '" + fullName + "' at '" + path
                            + "' has zero columns and cannot be rendered as Parquet");
        }
        ancestry.push(fullName);
        for (FieldDescriptor child : nested.getFields()) {
            inspectFieldSchema(child, path + "." + child.getName(), ancestry, fieldsSeen);
        }
        ancestry.pop();
    }

    static void requireNoUnknownWireFields(Message root, Cancellation cancellation)
            throws RenderFailure {
        record Frame(Message message, int depth) {}
        Deque<Frame> stack = new ArrayDeque<>();
        stack.push(new Frame(root, 0));
        while (!stack.isEmpty()) {
            cancellation.check();
            Frame frame = stack.pop();
            if (frame.depth() > MAX_MESSAGE_WALK_DEPTH) {
                throw new RenderFailure(
                        RenderFailure.Kind.LIMIT,
                        "Input message exceeds maximum nesting depth ("
                                + MAX_MESSAGE_WALK_DEPTH + ")");
            }
            Message current = frame.message();
            if (!current.getUnknownFields().asMap().isEmpty()) {
                throw new RenderFailure(
                        RenderFailure.Kind.UNSUPPORTED,
                        "Source message '" + current.getDescriptorForType().getFullName()
                                + "' contains unknown wire fields "
                                + current.getUnknownFields().asMap().keySet()
                                + " which would be lost in Parquet conversion");
            }
            for (var entry : current.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    continue;
                }
                if (field.isRepeated()) {
                    List<?> items = (List<?>) entry.getValue();
                    for (int i = items.size() - 1; i >= 0; i--) {
                        stack.push(new Frame((Message) items.get(i), frame.depth() + 1));
                    }
                } else {
                    stack.push(new Frame((Message) entry.getValue(), frame.depth() + 1));
                }
            }
        }
    }

    static void requireRecognizedEnumsInSelectedColumns(
            Message root, Set<String> selectedTopColumns, Cancellation cancellation)
            throws RenderFailure {
        record EnumFrame(Message message, Descriptor expected, String pathPrefix,
                         boolean rootLevel, int depth) {}
        record DescriptorPair(Descriptor expected, Descriptor actual) {}
        Set<DescriptorPair> checkedTypes = new HashSet<>();
        Deque<EnumFrame> stack = new ArrayDeque<>();
        stack.push(new EnumFrame(root, root.getDescriptorForType(), "", true, 0));
        while (!stack.isEmpty()) {
            cancellation.check();
            EnumFrame frame = stack.pop();
            if (frame.depth() > MAX_MESSAGE_WALK_DEPTH) {
                throw new RenderFailure(
                        RenderFailure.Kind.LIMIT,
                        "Message exceeds maximum nesting depth (" + MAX_MESSAGE_WALK_DEPTH + ")");
            }
            Message msg = frame.message();
            Descriptor actual = msg.getDescriptorForType();
            if (frame.expected() != actual
                    && checkedTypes.add(new DescriptorPair(frame.expected(), actual))) {
                if (!frame.expected().getFullName().equals(actual.getFullName())) {
                    throw new RenderFailure(
                            RenderFailure.Kind.UNSUPPORTED,
                            "Message value at '" + frame.pathPrefix() + "' has type '"
                                    + actual.getFullName() + "', expected '"
                                    + frame.expected().getFullName() + "'");
                }
                verifyFieldsCompatible(frame.expected(), actual);
            }
            for (var entry : msg.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                if (frame.rootLevel()
                        && !selectedTopColumns.isEmpty()
                        && !selectedTopColumns.contains(field.getName())) {
                    continue;
                }
                String path = frame.pathPrefix().isEmpty()
                        ? field.getName()
                        : frame.pathPrefix() + "." + field.getName();
                if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    if (field.isRepeated()) {
                        List<?> values = (List<?>) entry.getValue();
                        for (int i = 0; i < values.size(); i++) {
                            checkEnumValue(
                                    field,
                                    (EnumValueDescriptor) values.get(i),
                                    path + "[" + i + "]");
                        }
                    } else {
                        checkEnumValue(field, (EnumValueDescriptor) entry.getValue(), path);
                    }
                } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    if (field.isRepeated()) {
                        List<?> items = (List<?>) entry.getValue();
                        for (int i = items.size() - 1; i >= 0; i--) {
                            stack.push(new EnumFrame(
                                    (Message) items.get(i),
                                    field.getMessageType(),
                                    path + "[" + i + "]",
                                    false,
                                    frame.depth() + 1));
                        }
                    } else {
                        stack.push(new EnumFrame(
                                (Message) entry.getValue(), field.getMessageType(),
                                path, false, frame.depth() + 1));
                    }
                }
            }
        }
    }

    static void checkEnumValue(
            FieldDescriptor field, EnumValueDescriptor value, String path) throws RenderFailure {
        if (field.getEnumType() != value.getType()
                && !sameEnumMapping(field.getEnumType(), value.getType())) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Enum value at '" + path + "' has an incompatible type or value mapping for '"
                            + field.getEnumType().getFullName() + "'");
        }
        if (value.getIndex() < 0
                || field.getEnumType().findValueByNumber(value.getNumber()) == null) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Unrecognized enum number " + value.getNumber() + " at '" + path
                            + "' for enum " + field.getEnumType().getFullName());
        }
    }

    static void validateMessageRules(
            Descriptor descriptor, Message message, int rowIndex) throws RenderFailure {
        ValidationResult result =
                evaluateRules(ProtoValidator.forMessageType(descriptor), message);
        if (!result.valid()) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    formatValidationFailure(descriptor, rowIndex, result));
        }
    }

    static ValidationResult evaluateRules(ProtoValidator validator, Message message)
            throws RenderFailure {
        try {
            return validator.validate(message);
        } catch (RuleCompilationException | RuleEvaluationException failure) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "Validation rule evaluation failed on "
                            + message.getDescriptorForType().getFullName() + ": "
                            + failure.getMessage(),
                    failure);
        }
    }

    static String formatValidationFailure(
            Descriptor descriptor, int rowIndex, ValidationResult result) {
        StringBuilder sb = new StringBuilder();
        if (rowIndex >= 0) {
            sb.append("Row ").append(rowIndex).append(" of type ");
        } else {
            sb.append("Message of type ");
        }
        sb.append(descriptor.getFullName()).append(" failed validation:");
        for (ValidationResult.Violation violation : result.violations()) {
            sb.append(" [").append(violation.path()).append("] ")
                    .append(violation.ruleId()).append(": ").append(violation.message());
        }
        return sb.toString();
    }
}

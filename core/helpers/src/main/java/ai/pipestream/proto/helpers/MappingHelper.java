package ai.pipestream.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.*;

/**
 * Helper class for building mapping UIs and providing field discovery.
 * Supports frontend mappers by exposing schema information and validation.
 */
public class MappingHelper {

    private final TypeConverter typeConverter;

    /**
     * Creates a mapping helper.
     */
    public MappingHelper() {
        this.typeConverter = new TypeConverter();
    }

    /**
     * Gets all available field paths for a message type.
     * Useful for populating dropdowns in a UI.
     *
     * @param descriptor The message descriptor
     * @param maxDepth Maximum nesting depth to traverse (prevents infinite recursion)
     * @return List of all field paths in dot notation
     */
    public List<String> getAllFieldPaths(Descriptor descriptor, int maxDepth) {
        List<String> paths = new ArrayList<>();
        collectFieldPaths(descriptor, "", 0, maxDepth, paths, new HashSet<>());
        Collections.sort(paths);
        return paths;
    }

    /**
     * Gets detailed information about all fields in a message.
     * Returns metadata useful for UI rendering.
     *
     * @param descriptor The message descriptor
     * @param maxDepth Maximum nesting depth
     * @return List of field information objects
     */
    public List<FieldInfo> getFieldInfos(Descriptor descriptor, int maxDepth) {
        List<FieldInfo> infos = new ArrayList<>();
        collectFieldInfos(descriptor, "", 0, maxDepth, infos, new HashSet<>());
        return infos;
    }

    /**
     * Validates a mapping rule without executing it.
     *
     * @param rule The mapping rule (e.g., "target.field = source.field")
     * @param sourceDescriptor Source message descriptor
     * @param targetDescriptor Target message descriptor
     * @return Validation result with any errors
     */
    public ValidationResult validateRule(String rule, Descriptor sourceDescriptor, Descriptor targetDescriptor) {
        try {
            // Parse the rule
            String[] parts = parseRule(rule);
            if (parts == null) {
                return ValidationResult.error("Invalid rule syntax: " + rule);
            }

            String targetPath = parts[0];
            String sourcePath = parts[1];
            String operation = parts[2];

            // Validate target path
            PathValidation targetValidation = validatePath(targetPath, targetDescriptor);
            if (!targetValidation.isValid) {
                return ValidationResult.error("Invalid target path '" + targetPath + "': " + targetValidation.error);
            }

            // Validate source path (if not a literal)
            if (!isLiteral(sourcePath)) {
                PathValidation sourceValidation = validatePath(sourcePath, sourceDescriptor);
                if (!sourceValidation.isValid) {
                    return ValidationResult.error("Invalid source path '" + sourcePath + "': " + sourceValidation.error);
                }

                // Check type compatibility
                if (operation.equals("=")) {
                    boolean compatible = areTypesCompatible(
                        sourceValidation.fieldDescriptor,
                        targetValidation.fieldDescriptor
                    );
                    if (!compatible) {
                        return ValidationResult.warning(
                            "Type mismatch: " + sourceValidation.fieldDescriptor.getJavaType() +
                            " → " + targetValidation.fieldDescriptor.getJavaType() +
                            " (will attempt conversion)"
                        );
                    }
                }
            }

            return ValidationResult.success();
        } catch (Exception e) {
            return ValidationResult.error("Validation error: " + e.getMessage());
        }
    }

    /**
     * Finds compatible target fields for a given source field.
     * Useful for auto-suggestion in UI.
     *
     * @param sourcePath The source field path
     * @param sourceDescriptor Source message descriptor
     * @param targetDescriptor Target message descriptor
     * @return List of compatible target field paths, sorted by compatibility score
     */
    public List<FieldSuggestion> suggestTargetFields(
            String sourcePath,
            Descriptor sourceDescriptor,
            Descriptor targetDescriptor) {

        PathValidation sourceValidation = validatePath(sourcePath, sourceDescriptor);
        if (!sourceValidation.isValid) {
            return Collections.emptyList();
        }

        List<FieldSuggestion> suggestions = new ArrayList<>();
        List<String> allTargetPaths = getAllFieldPaths(targetDescriptor, 3);

        for (String targetPath : allTargetPaths) {
            PathValidation targetValidation = validatePath(targetPath, targetDescriptor);
            if (targetValidation.isValid) {
                int score = calculateCompatibilityScore(
                    sourcePath,
                    targetPath,
                    sourceValidation.fieldDescriptor,
                    targetValidation.fieldDescriptor
                );

                if (score > 0) {
                    suggestions.add(new FieldSuggestion(targetPath, score));
                }
            }
        }

        suggestions.sort((a, b) -> Integer.compare(b.score, a.score));
        return suggestions;
    }

    /**
     * Exports a message schema as a tree structure for UI consumption.
     *
     * @param descriptor The message descriptor
     * @param maxDepth Maximum depth to export
     * @return Schema tree
     */
    public SchemaNode exportSchema(Descriptor descriptor, int maxDepth) {
        return buildSchemaNode(descriptor, "", 0, maxDepth, new HashSet<>());
    }

    // Private helper methods

    private void collectFieldPaths(
            Descriptor descriptor,
            String prefix,
            int depth,
            int maxDepth,
            List<String> paths,
            Set<String> visited) {

        if (depth > maxDepth || visited.contains(descriptor.getFullName())) {
            return;
        }
        visited.add(descriptor.getFullName());

        for (FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            paths.add(fieldPath);

            // Recurse into message fields
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !field.isRepeated()) {
                Descriptor fieldType = field.getMessageType();
                // Don't recurse into well-known types that are terminal
                if (!isTerminalType(fieldType)) {
                    collectFieldPaths(fieldType, fieldPath, depth + 1, maxDepth, paths, new HashSet<>(visited));
                }
            }
        }
    }

    private void collectFieldInfos(
            Descriptor descriptor,
            String prefix,
            int depth,
            int maxDepth,
            List<FieldInfo> infos,
            Set<String> visited) {

        if (depth > maxDepth || visited.contains(descriptor.getFullName())) {
            return;
        }
        visited.add(descriptor.getFullName());

        for (FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();

            FieldInfo info = new FieldInfo(
                fieldPath,
                field.getName(),
                field.getJavaType().name(),
                field.isRepeated(),
                field.isRequired(),
                field.hasDefaultValue(),
                depth
            );
            infos.add(info);

            // Recurse into message fields
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !field.isRepeated()) {
                Descriptor fieldType = field.getMessageType();
                if (!isTerminalType(fieldType)) {
                    collectFieldInfos(fieldType, fieldPath, depth + 1, maxDepth, infos, new HashSet<>(visited));
                }
            }
        }
    }

    private SchemaNode buildSchemaNode(
            Descriptor descriptor,
            String path,
            int depth,
            int maxDepth,
            Set<String> visited) {

        // Extract the field name from the path (last component after the last dot)
        String fieldName = path.isEmpty() ? descriptor.getName() : 
            (path.contains(".") ? path.substring(path.lastIndexOf(".") + 1) : path);
        SchemaNode node = new SchemaNode(fieldName, path, "MESSAGE");

        if (depth > maxDepth || visited.contains(descriptor.getFullName())) {
            return node;
        }
        visited.add(descriptor.getFullName());

        for (FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = path.isEmpty() ? field.getName() : path + "." + field.getName();

            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !field.isRepeated()) {
                Descriptor fieldType = field.getMessageType();
                if (!isTerminalType(fieldType)) {
                    SchemaNode childNode = buildSchemaNode(
                        fieldType,
                        fieldPath,
                        depth + 1,
                        maxDepth,
                        new HashSet<>(visited)
                    );
                    childNode.repeated = field.isRepeated();
                    node.children.add(childNode);
                } else {
                    node.children.add(new SchemaNode(
                        field.getName(),
                        fieldPath,
                        field.getJavaType().name(),
                        field.isRepeated()
                    ));
                }
            } else {
                node.children.add(new SchemaNode(
                    field.getName(),
                    fieldPath,
                    field.getJavaType().name(),
                    field.isRepeated()
                ));
            }
        }

        return node;
    }

    private PathValidation validatePath(String path, Descriptor descriptor) {
        try {
            // Limit -1 keeps trailing empty tokens so "user." is seen as ["user", ""] and
            // rejected below instead of silently validating as "user".
            String[] parts = path.split("\\.", -1);
            for (String part : parts) {
                if (part.isEmpty()) {
                    return new PathValidation(false, "Path '" + path + "' contains an empty segment", null);
                }
            }

            Descriptor currentDescriptor = descriptor;
            FieldDescriptor lastField = null;

            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                FieldDescriptor field = currentDescriptor.findFieldByName(part);

                if (field == null) {
                    return new PathValidation(false, "Field '" + part + "' not found in " + currentDescriptor.getName(), null);
                }

                lastField = field;

                if (i < parts.length - 1) {
                    // Not the last part, must be a message to continue
                    if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                        return new PathValidation(false, "Field '" + part + "' is not a message type", null);
                    }
                    if (field.isRepeated()) {
                        return new PathValidation(false, "Cannot traverse through repeated field '" + part + "'", null);
                    }
                    currentDescriptor = field.getMessageType();
                }
            }

            return new PathValidation(true, null, lastField);
        } catch (Exception e) {
            return new PathValidation(false, e.getMessage(), null);
        }
    }

    private boolean areTypesCompatible(FieldDescriptor source, FieldDescriptor target) {
        // Same type is always compatible
        if (source.getJavaType() == target.getJavaType()) {
            return true;
        }

        // Numeric types are generally compatible
        Set<FieldDescriptor.JavaType> numericTypes = EnumSet.of(
            FieldDescriptor.JavaType.INT,
            FieldDescriptor.JavaType.LONG,
            FieldDescriptor.JavaType.FLOAT,
            FieldDescriptor.JavaType.DOUBLE
        );

        if (numericTypes.contains(source.getJavaType()) && numericTypes.contains(target.getJavaType())) {
            return true;
        }

        // String is compatible with most things (via conversion)
        return source.getJavaType() == FieldDescriptor.JavaType.STRING ||
                target.getJavaType() == FieldDescriptor.JavaType.STRING;
    }

    private int calculateCompatibilityScore(
            String sourcePath,
            String targetPath,
            FieldDescriptor sourceField,
            FieldDescriptor targetField) {

        int score = 0;

        // Same type gets high score
        if (sourceField.getJavaType() == targetField.getJavaType()) {
            score += 100;
        } else if (areTypesCompatible(sourceField, targetField)) {
            score += 50;
        } else {
            return 0; // Incompatible
        }

        // Name similarity
        String sourceName = sourcePath.substring(sourcePath.lastIndexOf('.') + 1);
        String targetName = targetPath.substring(targetPath.lastIndexOf('.') + 1);

        if (sourceName.equalsIgnoreCase(targetName)) {
            score += 50;
        } else if (sourceName.toLowerCase().contains(targetName.toLowerCase()) ||
                   targetName.toLowerCase().contains(sourceName.toLowerCase())) {
            score += 25;
        }

        // Same cardinality (both repeated or both singular)
        if (sourceField.isRepeated() == targetField.isRepeated()) {
            score += 20;
        }

        return score;
    }

    private String[] parseRule(String rule) {
        // Handle assignment: target = source
        if (rule.contains("=") && !rule.contains("+=")) {
            String[] parts = rule.split("=", 2);
            if (parts.length == 2) {
                return new String[]{parts[0].trim(), parts[1].trim(), "="};
            }
        }
        // Handle append: target += source
        else if (rule.contains("+=")) {
            String[] parts = rule.split("\\+=", 2);
            if (parts.length == 2) {
                return new String[]{parts[0].trim(), parts[1].trim(), "+="};
            }
        }
        // Handle clear: -target
        else if (rule.trim().startsWith("-")) {
            String target = rule.trim().substring(1).trim();
            return new String[]{target, null, "-"};
        }

        return null;
    }

    private boolean isLiteral(String value) {
        value = value.trim();
        return value.equals("null") ||
               value.equals("true") ||
               value.equals("false") ||
               (value.startsWith("\"") && value.endsWith("\"")) ||
               value.matches("-?\\d+(\\.\\d+)?");
    }

    private boolean isTerminalType(Descriptor descriptor) {
        String fullName = descriptor.getFullName();
        return fullName.startsWith("google.protobuf.") &&
               (fullName.equals("google.protobuf.Struct") ||
                fullName.equals("google.protobuf.Value") ||
                fullName.equals("google.protobuf.Any") ||
                fullName.equals("google.protobuf.Timestamp") ||
                fullName.equals("google.protobuf.Duration"));
    }

    // Data classes for results

    /**
     * Service type for field info.
     */
    public static class FieldInfo {
        /**
         * The path.
         */
        public final String path;
        /**
         * The name.
         */
        public final String name;
        /**
         * The type.
         */
        public final String type;
        /**
         * The repeated.
         */
        public final boolean repeated;
        /**
         * The required.
         */
        public final boolean required;
        /**
         * The has default.
         */
        public final boolean hasDefault;
        /**
         * The depth.
         */
        public final int depth;

        /**
         * Creates field metadata for a discovered path.
         *
         * @param path the full field path
         * @param name the leaf field name
         * @param type the field type name
         * @param repeated whether the field is repeated
         * @param required whether the field is required
         * @param hasDefault whether the field defines a default value
         * @param depth the nesting depth
         */
        public FieldInfo(String path, String name, String type, boolean repeated,
                        boolean required, boolean hasDefault, int depth) {
            this.path = path;
            this.name = name;
            this.type = type;
            this.repeated = repeated;
            this.required = required;
            this.hasDefault = hasDefault;
            this.depth = depth;
        }
    }

    /**
     * Service type for validation result.
     */
    public static class ValidationResult {
        /**
         * The is valid.
         */
        public final boolean isValid;
        /**
         * The message.
         */
        public final String message;
        /**
         * The level.
         */
        public final ValidationLevel level;

        private ValidationResult(boolean isValid, String message, ValidationLevel level) {
            this.isValid = isValid;
            this.message = message;
            this.level = level;
        }

        /**
         * Handles success.
         *
         * @return the result
         */
        public static ValidationResult success() {
            return new ValidationResult(true, "Valid", ValidationLevel.SUCCESS);
        }

        /**
         * Handles error.
         *
         * @param message the message
         * @return the result
         */
        public static ValidationResult error(String message) {
            return new ValidationResult(false, message, ValidationLevel.ERROR);
        }

        /**
         * Handles warning.
         *
         * @param message the message
         * @return the result
         */
        public static ValidationResult warning(String message) {
            return new ValidationResult(true, message, ValidationLevel.WARNING);
        }

        /**
         * Enumerates validation level values.
         */
        public enum ValidationLevel {
            /**
             * Success result.
             */
            SUCCESS,
            /**
             * Warning result.
             */
            WARNING,
            /**
             * Error result.
             */
            ERROR;
        }
    }

    /**
     * Service type for field suggestion.
     */
    public static class FieldSuggestion {
        /**
         * The field path.
         */
        public final String fieldPath;
        /**
         * The score.
         */
        public final int score;

        /**
         * Creates a field suggestion.
         *
         * @param fieldPath the field path
         * @param score the score
         */
        public FieldSuggestion(String fieldPath, int score) {
            this.fieldPath = fieldPath;
            this.score = score;
        }
    }

    /**
     * Service type for schema node.
     */
    public static class SchemaNode {
        /**
         * Display name for this schema node.
         */
        public final String name;
        /**
         * Dot-path for this schema node.
         */
        public final String path;
        /**
         * Logical field type for this schema node.
         */
        public final String type;
        /**
         * The repeated.
         */
        public boolean repeated;
        /**
         * Documents mapping helper members.
         */
        public final List<SchemaNode> children = new ArrayList<>();

        /**
         * Creates a schema node.
         *
         * @param name the name
         * @param path the path
         * @param type the type
         */
        public SchemaNode(String name, String path, String type) {
            this(name, path, type, false);
        }

        /**
         * Creates a schema node.
         *
         * @param name the name
         * @param path the path
         * @param type the type
         * @param repeated the repeated
         */
        public SchemaNode(String name, String path, String type, boolean repeated) {
            this.name = name;
            this.path = path;
            this.type = type;
            this.repeated = repeated;
        }
    }

    private static class PathValidation {
        final boolean isValid;
        final String error;
        final FieldDescriptor fieldDescriptor;

        PathValidation(boolean isValid, String error, FieldDescriptor fieldDescriptor) {
            this.isValid = isValid;
            this.error = error;
            this.fieldDescriptor = fieldDescriptor;
        }
    }
}

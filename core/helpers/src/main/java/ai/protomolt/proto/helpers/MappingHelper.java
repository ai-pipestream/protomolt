package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for building mapping UIs and providing field discovery.
 * Supports frontend mappers by exposing schema information and validation.
 */
public class MappingHelper {

    /**
     * Creates a mapping helper.
     */
    public MappingHelper() {
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
        return MappingPathSupport.getAllFieldPaths(descriptor, maxDepth);
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
        return MappingPathSupport.getFieldInfos(descriptor, maxDepth);
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
        return MappingRuleValidator.validateRule(rule, sourceDescriptor, targetDescriptor);
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
        return MappingSuggestionEngine.suggestTargetFields(sourcePath, sourceDescriptor, targetDescriptor);
    }

    /**
     * Exports a message schema as a tree structure for UI consumption.
     *
     * @param descriptor The message descriptor
     * @param maxDepth Maximum depth to export
     * @return Schema tree
     */
    public SchemaNode exportSchema(Descriptor descriptor, int maxDepth) {
        return MappingSchemaExporter.exportSchema(descriptor, maxDepth);
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
}

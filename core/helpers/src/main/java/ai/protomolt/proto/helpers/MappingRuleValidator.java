package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.EnumSet;
import java.util.Set;

/**
 * Package-private mapping rule syntax, literal, and type-compatibility validator for
 * {@link MappingHelper}.
 */
final class MappingRuleValidator {

    private static final String OP_ASSIGN = "=";
    private static final String OP_APPEND = "+=";
    private static final String OP_CLEAR = "-";
    private static final Set<FieldDescriptor.JavaType> NUMERIC_TYPES = EnumSet.of(
            FieldDescriptor.JavaType.INT,
            FieldDescriptor.JavaType.LONG,
            FieldDescriptor.JavaType.FLOAT,
            FieldDescriptor.JavaType.DOUBLE
    );

    private MappingRuleValidator() {}

    /**
     * Validates a mapping rule against {@code sourceDescriptor} and {@code targetDescriptor}.
     */
    static MappingHelper.ValidationResult validateRule(
            String rule,
            Descriptor sourceDescriptor,
            Descriptor targetDescriptor) {
        try {
            String[] parts = parseRule(rule);
            if (parts == null) {
                return MappingHelper.ValidationResult.error("Invalid rule syntax: " + rule);
            }

            String targetPath = parts[0];
            String sourcePath = parts[1];
            String operation = parts[2];

            MappingPathSupport.PathValidation targetValidation =
                    MappingPathSupport.validatePath(targetPath, targetDescriptor);
            if (!targetValidation.isValid()) {
                return MappingHelper.ValidationResult.error(
                        "Invalid target path '" + targetPath + "': " + targetValidation.error());
            }

            // Validate source path (if not a literal). Clear rules ("-target") have no source.
            if (sourcePath != null && !isLiteral(sourcePath)) {
                MappingPathSupport.PathValidation sourceValidation =
                        MappingPathSupport.validatePath(sourcePath, sourceDescriptor);
                if (!sourceValidation.isValid()) {
                    return MappingHelper.ValidationResult.error(
                            "Invalid source path '" + sourcePath + "': " + sourceValidation.error());
                }

                if (operation.equals(OP_ASSIGN)) {
                    boolean compatible = areTypesCompatible(
                            sourceValidation.fieldDescriptor(),
                            targetValidation.fieldDescriptor()
                    );
                    if (!compatible) {
                        return MappingHelper.ValidationResult.warning(
                                "Type mismatch: " + sourceValidation.fieldDescriptor().getJavaType() +
                                " → " + targetValidation.fieldDescriptor().getJavaType() +
                                " (will attempt conversion)"
                        );
                    }
                }
            }

            return MappingHelper.ValidationResult.success();
        } catch (Exception e) {
            return MappingHelper.ValidationResult.error("Validation error: " + e.getMessage());
        }
    }

    /**
     * Returns true when {@code source} and {@code target} have directly compatible Java types
     * or can be converted via numeric widening/narrowing or string representation.
     */
    static boolean areTypesCompatible(FieldDescriptor source, FieldDescriptor target) {
        if (source.getJavaType() == target.getJavaType()) {
            return true;
        }
        if (NUMERIC_TYPES.contains(source.getJavaType()) && NUMERIC_TYPES.contains(target.getJavaType())) {
            return true;
        }
        return source.getJavaType() == FieldDescriptor.JavaType.STRING
                || target.getJavaType() == FieldDescriptor.JavaType.STRING;
    }

    private static String[] parseRule(String rule) {
        int plusEquals = rule.indexOf(OP_APPEND);
        int equals = rule.indexOf('=');
        if (equals >= 0 && plusEquals < 0) {
            return new String[]{
                    rule.substring(0, equals).trim(),
                    rule.substring(equals + 1).trim(),
                    OP_ASSIGN
            };
        }
        if (plusEquals >= 0) {
            return new String[]{
                    rule.substring(0, plusEquals).trim(),
                    rule.substring(plusEquals + OP_APPEND.length()).trim(),
                    OP_APPEND
            };
        }
        String trimmed = rule.trim();
        if (trimmed.startsWith(OP_CLEAR)) {
            String target = trimmed.substring(1).trim();
            return new String[]{target, null, OP_CLEAR};
        }
        return null;
    }

    static boolean isLiteral(String value) {
        String trimmed = value.trim();
        return trimmed.equals("null")
                || trimmed.equals("true")
                || trimmed.equals("false")
                || (trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || isNumericLiteral(trimmed);
    }

    /**
     * Matches an optional leading {@code '-'}, one or more ASCII digits, and an optional
     * decimal point followed by one or more ASCII digits (equivalent to {@code -?\d+(\.\d+)?}).
     */
    static boolean isNumericLiteral(String value) {
        int length = value.length();
        if (length == 0) {
            return false;
        }
        int index = value.charAt(0) == '-' ? 1 : 0;
        int intDigits = 0;
        while (index < length && isAsciiDigit(value.charAt(index))) {
            intDigits++;
            index++;
        }
        if (intDigits == 0) {
            return false;
        }
        if (index == length) {
            return true;
        }
        if (value.charAt(index) != '.') {
            return false;
        }
        index++;
        int fracDigits = 0;
        while (index < length && isAsciiDigit(value.charAt(index))) {
            fracDigits++;
            index++;
        }
        return fracDigits > 0 && index == length;
    }

    private static boolean isAsciiDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }
}

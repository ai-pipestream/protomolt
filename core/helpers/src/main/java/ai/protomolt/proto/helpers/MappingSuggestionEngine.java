package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Package-private target field suggestion and compatibility scoring helper for
 * {@link MappingHelper}.
 */
final class MappingSuggestionEngine {

    private static final int DEFAULT_SUGGESTION_DEPTH = 3;
    private static final int EXACT_TYPE_SCORE = 100;
    private static final int COMPATIBLE_TYPE_SCORE = 50;
    private static final int EXACT_NAME_SCORE = 50;
    private static final int SUBSTRING_NAME_SCORE = 25;
    private static final int SAME_CARDINALITY_SCORE = 20;

    private MappingSuggestionEngine() {}

    /**
     * Suggests compatible target fields for {@code sourcePath} sorted by descending
     * compatibility score.
     */
    static List<MappingHelper.FieldSuggestion> suggestTargetFields(
            String sourcePath,
            Descriptor sourceDescriptor,
            Descriptor targetDescriptor) {

        MappingPathSupport.PathValidation sourceValidation =
                MappingPathSupport.validatePath(sourcePath, sourceDescriptor);
        if (!sourceValidation.isValid()) {
            return Collections.emptyList();
        }

        List<MappingHelper.FieldSuggestion> suggestions = new ArrayList<>();
        List<String> allTargetPaths =
                MappingPathSupport.getAllFieldPaths(targetDescriptor, DEFAULT_SUGGESTION_DEPTH);

        for (String targetPath : allTargetPaths) {
            MappingPathSupport.PathValidation targetValidation =
                    MappingPathSupport.validatePath(targetPath, targetDescriptor);
            if (targetValidation.isValid()) {
                int score = calculateCompatibilityScore(
                        sourcePath,
                        targetPath,
                        sourceValidation.fieldDescriptor(),
                        targetValidation.fieldDescriptor()
                );

                if (score > 0) {
                    suggestions.add(new MappingHelper.FieldSuggestion(targetPath, score));
                }
            }
        }

        suggestions.sort((a, b) -> Integer.compare(b.score, a.score));
        return suggestions;
    }

    private static int calculateCompatibilityScore(
            String sourcePath,
            String targetPath,
            FieldDescriptor sourceField,
            FieldDescriptor targetField) {

        int score = 0;

        if (sourceField.getJavaType() == targetField.getJavaType()) {
            score += EXACT_TYPE_SCORE;
        } else if (MappingRuleValidator.areTypesCompatible(sourceField, targetField)) {
            score += COMPATIBLE_TYPE_SCORE;
        } else {
            return 0;
        }

        String sourceName = sourcePath.substring(sourcePath.lastIndexOf('.') + 1);
        String targetName = targetPath.substring(targetPath.lastIndexOf('.') + 1);

        if (sourceName.equalsIgnoreCase(targetName)) {
            score += EXACT_NAME_SCORE;
        } else {
            String lowerSource = sourceName.toLowerCase(Locale.ROOT);
            String lowerTarget = targetName.toLowerCase(Locale.ROOT);
            if (lowerSource.contains(lowerTarget) || lowerTarget.contains(lowerSource)) {
                score += SUBSTRING_NAME_SCORE;
            }
        }

        if (sourceField.isRepeated() == targetField.isRepeated()) {
            score += SAME_CARDINALITY_SCORE;
        }

        return score;
    }
}

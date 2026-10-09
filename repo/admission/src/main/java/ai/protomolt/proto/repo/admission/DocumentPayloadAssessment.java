package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Any;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One payload's completed structural/schema traversal and value verdict. Borrowed
 * bytes/assets must remain stable for the assessment lifetime. This establishes
 * neither complete command verification nor policy authorization or retention.
 */
sealed interface DocumentPayloadAssessment {
    Instant evaluatedAt();

    record Accepted(Instant evaluatedAt, DocumentPayloadCheck.AssetResult checked) implements DocumentPayloadAssessment {}

    /** Full occurrence inventory with only the first value failure, never a checked payload. */
    record Invalid(Instant evaluatedAt, Any original,
                   Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAssetBinding> assets,
                   List<DocumentSchemaOccurrences.Occurrence> occurrences, long decodedBytes, Failure failure)
            implements DocumentPayloadAssessment {
        public Invalid { assets = Map.copyOf(assets); occurrences = List.copyOf(occurrences); }
    }

    /** Internal diagnostic identity. Paths/IDs may contain user text; not a public receipt field. */
    record Failure(List<DocumentSchemaOccurrences.Step> occurrence, String fieldPath, String ruleId, String rulePath) {
        public Failure {
            occurrence = List.copyOf(occurrence);
            bounded(fieldPath); bounded(ruleId); bounded(rulePath);
        }
        static Failure from(List<DocumentSchemaOccurrences.Step> occurrence, ValidationResult.Violation violation) {
            // Do not retain payload-bearing diagnostic messages or an unbounded violation list.
            return new Failure(occurrence, violation.path(), violation.ruleId(), violation.rulePath());
        }
        private static void bounded(String value) {
            if (Objects.requireNonNull(value).length() > 16384)
                throw new IllegalArgumentException("payload failure identity exceeds text limit");
        }
    }
}

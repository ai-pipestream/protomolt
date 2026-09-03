package ai.protomolt.proto.prompt;

import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * Renders a {@link ValidationResult} as the JSON rejection a form-filling model retries
 * against. The same artifact is the judge's verdict and the defendant's feedback, so the
 * two can never diverge.
 */
public final class ViolationFeedbackRenderer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ViolationFeedbackRenderer() {
    }

    /** The rejection as a JSON tree: {@code valid}, and on failure an instruction plus
     * one entry per violation (path, rule, message, and rule_path when known). */
    public static ObjectNode toJson(ValidationResult result) {
        Objects.requireNonNull(result, "result");
        if (!result.valid() && result.violations().isEmpty()) {
            throw new IllegalArgumentException(
                    "an invalid ValidationResult must carry at least one violation");
        }
        ObjectNode root = MAPPER.createObjectNode();
        root.put("valid", result.valid());
        if (!result.valid()) {
            root.put("instruction", "The submitted form was rejected. Correct every "
                    + "violation and resubmit the complete form.");
            ArrayNode violations = root.putArray("violations");
            for (ValidationResult.Violation violation : result.violations()) {
                ObjectNode entry = violations.addObject();
                entry.put("path", violation.path());
                entry.put("rule", violation.ruleId());
                entry.put("message", violation.message());
                if (!violation.rulePath().isEmpty()) {
                    entry.put("rule_path", violation.rulePath());
                }
            }
        }
        return root;
    }

    /** As {@link #toJson} but pretty-printed text. */
    public static String render(ValidationResult result) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toJson(result));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize violation feedback", e);
        }
    }
}

package ai.pipestream.proto.prompt;

import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ViolationFeedbackRendererTest {

    @Test
    void rendersRejectionsAsRetryFeedback() throws Exception {
        ValidationResult result = ValidationResult.failed(List.of(
                new ValidationResult.Violation("court", "string.max_len",
                        "must be at most 200 characters"),
                new ValidationResult.Violation("posture", "enum.defined_only",
                        "must be a defined value", "enum")));

        String json = ViolationFeedbackRenderer.render(result);
        JsonNode root = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);

        assertThat(root.get("valid").asBoolean()).isFalse();
        assertThat(root.get("instruction").asText()).contains("resubmit");
        JsonNode violations = root.get("violations");
        assertThat(violations).hasSize(2);
        assertThat(violations.get(0).get("path").asText()).isEqualTo("court");
        assertThat(violations.get(0).get("rule").asText()).isEqualTo("string.max_len");
        assertThat(violations.get(0).has("rule_path")).isFalse();
        assertThat(violations.get(1).get("rule_path").asText()).isEqualTo("enum");
    }

    @Test
    void rendersAValidResultWithoutViolations() {
        JsonNode root = ViolationFeedbackRenderer.toJson(ValidationResult.ok());

        assertThat(root.get("valid").asBoolean()).isTrue();
        assertThat(root.has("violations")).isFalse();
    }

    @Test
    void rejectsAnInvalidResultWithNoViolationsAsACallerBug() {
        assertThatThrownBy(() -> ViolationFeedbackRenderer.toJson(
                new ValidationResult(false, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

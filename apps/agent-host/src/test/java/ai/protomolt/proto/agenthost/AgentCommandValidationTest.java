package ai.protomolt.proto.agenthost;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the validator-path rendering, whose segment walk replaced
 * {@code split("\\.")} with a non-regex loop.
 */
class AgentCommandValidationTest {

    @Test
    void rendersValidatorPathsInJsonSpelling() {
        assertThat(AgentCommandValidation.jsonPath("worker_id")).isEqualTo("workerId");
        assertThat(AgentCommandValidation.jsonPath("task.worker_id")).isEqualTo("task.workerId");
        assertThat(AgentCommandValidation.jsonPath("items[2].sub_field"))
                .isEqualTo("items[2].subField");
        assertThat(AgentCommandValidation.jsonPath("plain")).isEqualTo("plain");
        assertThat(AgentCommandValidation.jsonPath("")).isEqualTo("");
    }

    @Test
    void matchesTheReplacedSplitOnRepresentativePaths() {
        // The oracle renders through the regex split the production code no longer uses.
        for (String path : new String[] {"", "a", "a.b", "a.b.", "a..b", ".a", "x_y.z_w[0]",
                "items[1].deep.nested_name", "."}) {
            assertThat(AgentCommandValidation.jsonPath(path))
                    .as("path '%s'", path)
                    .isEqualTo(oracle(path));
        }
    }

    /** The original implementation, verbatim, as the comparison oracle. */
    private static String oracle(String path) {
        StringBuilder out = new StringBuilder();
        for (String segment : path.split("\\.")) {
            if (out.length() > 0) {
                out.append('.');
            }
            int bracket = segment.indexOf('[');
            String name = bracket < 0 ? segment : segment.substring(0, bracket);
            boolean capitalize = false;
            for (int i = 0; i < name.length(); i++) {
                char character = name.charAt(i);
                if (character == '_') {
                    capitalize = true;
                } else if (capitalize) {
                    out.append(Character.toUpperCase(character));
                    capitalize = false;
                } else {
                    out.append(character);
                }
            }
            if (bracket >= 0) {
                out.append(segment.substring(bracket));
            }
        }
        return out.toString();
    }
}

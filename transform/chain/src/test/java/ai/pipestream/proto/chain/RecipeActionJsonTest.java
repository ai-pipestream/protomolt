package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecipeActionJsonTest {

    @Test
    void identityAcceptsPathSafeBoundedNames() throws Exception {
        ObjectNode input = JsonNodeFactory.instance.objectNode().put("runId", "probe.v1-2");

        assertThat(RecipeActionJson.identity(input, "runId")).isEqualTo("probe.v1-2");
    }

    @Test
    void identityRejectsPathsAndOverlongNamesWithTheFieldPointer() {
        ObjectNode path = JsonNodeFactory.instance.objectNode().put("runId", "../probe");
        ObjectNode longName = JsonNodeFactory.instance.objectNode()
                .put("runId", "x".repeat(129));

        assertInvalidIdentity(path);
        assertInvalidIdentity(longName);
    }

    private static void assertInvalidIdentity(ObjectNode input) {
        assertThatThrownBy(() -> RecipeActionJson.identity(input, "runId"))
                .isInstanceOfSatisfying(ActionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo("invalid-input");
                    assertThat(failure.details()).isPresent().get()
                            .extracting(details -> details.path("pointer").asText())
                            .isEqualTo("/runId");
                });
    }
}

package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowRequestsTest {

    @Test
    void identityAcceptsPathSafeBoundedNames() throws Exception {
        assertThat(WorkflowRequests.identity(runId("probe.v1-2"), "runId"))
                .isEqualTo("probe.v1-2");
    }

    @Test
    void identityRejectsPathsAndOverlongNamesWithTheFieldPointer() throws Exception {
        assertInvalidIdentity(runId("../probe"));
        assertInvalidIdentity(runId("x".repeat(129)));
    }

    private static void assertInvalidIdentity(Message input) {
        assertThatThrownBy(() -> WorkflowRequests.identity(input, "runId"))
                .isInstanceOfSatisfying(ActionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo("invalid-input");
                    assertThat(failure.details()).isPresent().get()
                            .extracting(details -> details.path("pointer").asText())
                            .isEqualTo("/runId");
                });
    }

    /**
     * A request carrying the run id, read without the rules: these cases are about what the
     * helper makes of a name, and the message's own uuid rule would refuse them first.
     */
    private static Message runId(String value) throws ActionException {
        ObjectNode envelope = JsonNodeFactory.instance.objectNode().put("runId", value);
        return CatalogContract.read(envelope,
                CatalogContract.request("ReplayWorkflowRequest"), "runId");
    }
}

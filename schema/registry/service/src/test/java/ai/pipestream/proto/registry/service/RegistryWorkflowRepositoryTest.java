package ai.pipestream.proto.registry.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.workflow.WorkflowRepository;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The registry's {@link WorkflowRepository} view: absent workflows read as
 * empty, corrupt stored JSON fails loudly instead of reading as "not found".
 */
class RegistryWorkflowRepositoryTest {

    @Test
    void anAbsentWorkflowReadsAsEmpty() {
        WorkflowRepository repository =
                RegistryModule.workflowRepository(name -> Optional.empty());
        assertThat(repository.workflow("nope")).isEmpty();
    }

    @Test
    void aStoredWorkflowParsesToAnObject() {
        WorkflowRepository repository = RegistryModule.workflowRepository(
                name -> Optional.of("{\"inputType\":\"test.v1.In\",\"steps\":[]}"));
        assertThat(repository.workflow("w")).isPresent();
    }

    @Test
    void malformedStoredJsonFailsLoudly() {
        WorkflowRepository repository = RegistryModule.workflowRepository(
                name -> Optional.of("{not json"));
        assertThatThrownBy(() -> repository.workflow("broken"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("broken")
                .hasMessageContaining("not valid JSON");
    }

    @Test
    void aNonObjectStoredWorkflowFailsLoudly() {
        WorkflowRepository repository = RegistryModule.workflowRepository(
                name -> Optional.of("[1, 2, 3]"));
        assertThatThrownBy(() -> repository.workflow("array"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("array")
                .hasMessageContaining("not a JSON object");
    }
}

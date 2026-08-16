package ai.pipestream.proto.serve;

import ai.pipestream.proto.registry.RegistryStoreException;
import ai.pipestream.proto.workflow.WorkflowRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The workflow repository fails hard: a store failure or a corrupt stored workflow surfaces with
 * the workflow name in the message, never as a silent "not found". Only a genuine store miss
 * answers empty.
 */
class WorkflowRepositoryFailureTest {

    @Test
    void storeFailurePropagates() {
        WorkflowRepository repo = ProtoMoltServe.workflowRepository(name -> {
            throw new RegistryStoreException("Failed to read workflow " + name);
        });
        assertThatThrownBy(() -> repo.workflow("broken-store"))
                .isInstanceOf(RegistryStoreException.class)
                .hasMessageContaining("broken-store");
    }

    @Test
    void malformedStoredJsonThrows() {
        WorkflowRepository repo = ProtoMoltServe.workflowRepository(
                name -> Optional.of("{not json"));
        assertThatThrownBy(() -> repo.workflow("garbled"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("garbled")
                .hasCauseInstanceOf(JsonProcessingException.class);
    }

    @Test
    void nonObjectStoredJsonThrows() {
        WorkflowRepository repo = ProtoMoltServe.workflowRepository(
                name -> Optional.of("[1, 2, 3]"));
        assertThatThrownBy(() -> repo.workflow("array-workflow"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("array-workflow");
    }

    @Test
    void missingWorkflowAnswersEmpty() {
        WorkflowRepository repo = ProtoMoltServe.workflowRepository(name -> Optional.empty());
        assertThat(repo.workflow("nope")).isEmpty();
    }

    @Test
    void storedObjectParses() {
        WorkflowRepository repo = ProtoMoltServe.workflowRepository(
                name -> Optional.of("{\"name\": \"ok\"}"));
        assertThat(repo.workflow("ok"))
                .hasValueSatisfying(node -> assertThat(node.path("name").asText()).isEqualTo("ok"));
    }
}

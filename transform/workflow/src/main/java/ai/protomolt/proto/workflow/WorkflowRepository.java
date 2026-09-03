package ai.protomolt.proto.workflow;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

/**
 * Resolves named workflows for {@code run-workflow}. The registry's Git store is the canonical
 * implementation; anything that can hand back the stored {@code CompiledWorkflow} JSON
 * qualifies.
 */
public interface WorkflowRepository {

    Optional<ObjectNode> workflow(String name);
}

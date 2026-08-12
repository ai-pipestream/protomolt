package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/** Lists the coordinator's worker registrations: identity, admission, and capabilities. */
final class DelegationWorkerListAction extends DelegationAction {

    /** Upper bound on workers one call returns. */
    static final int MAX_WORKERS = 256;

    DelegationWorkerListAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-worker-list";
    }

    @Override
    public String description() {
        return "Lists every worker registered with the delegation coordinator: identity, "
                + "admission and connection state, provider metadata, and capabilities. Use "
                + "it to discover a worker before offering a task.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = DelegationActionJson.schema();
        schema.putObject("properties");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        List<InProcessDelegationCoordinator.WorkerView> workers;
        try {
            workers = bridge.coordinator().workers();
        } catch (RuntimeException e) {
            throw failure(e);
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        ArrayNode array = output.putArray("workers");
        workers.stream().limit(MAX_WORKERS).forEach(worker -> array.add(workerJson(worker)));
        output.put("truncated", workers.size() > MAX_WORKERS);
        return output;
    }

    private static ObjectNode workerJson(InProcessDelegationCoordinator.WorkerView worker) {
        ObjectNode node = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance
                .objectNode();
        WorkerHello hello = worker.hello();
        node.put("workerId", worker.workerId());
        node.put("admitted", worker.admitted());
        node.put("connected", worker.connected());
        node.put("provider", hello.getProvider());
        node.put("model", hello.getModel());
        node.put("modelVersion", hello.getModelVersion());
        ArrayNode capabilities = node.putArray("capabilities");
        for (WorkerCapability capability : hello.getCapabilitiesList()) {
            ObjectNode entry = capabilities.addObject();
            entry.put("name", capability.getName());
            entry.put("description", capability.getDescription());
        }
        return node;
    }
}

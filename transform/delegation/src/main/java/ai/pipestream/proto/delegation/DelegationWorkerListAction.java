package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.delegation.v1.ListWorkersRequest;
import ai.pipestream.proto.delegation.v1.ListWorkersResponse;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.delegation.v1.WorkerRegistrationSummary;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import com.google.protobuf.Descriptors.Descriptor;

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
    public Descriptor requestType() {
        return ListWorkersRequest.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        // The verb takes no input, but the envelope is still parsed against the declared
        // request so a caller that sends members gets told, rather than having them ignored.
        DelegationActionJson.parse(input, ListWorkersRequest.newBuilder(), name());
        List<InProcessDelegationCoordinator.WorkerView> workers;
        try {
            workers = bridge.coordinator().workers();
        } catch (RuntimeException e) {
            throw failure(e);
        }
        ListWorkersResponse.Builder response = ListWorkersResponse.newBuilder()
                .setOk(true)
                .setTruncated(workers.size() > MAX_WORKERS);
        workers.stream().limit(MAX_WORKERS).forEach(worker -> response.addWorkers(summary(worker)));
        return DelegationActionJson.render(response.build(), context);
    }

    private static WorkerRegistrationSummary summary(
            InProcessDelegationCoordinator.WorkerView worker) {
        WorkerHello hello = worker.hello();
        return WorkerRegistrationSummary.newBuilder()
                .setWorkerId(worker.workerId())
                .setAdmitted(worker.admitted())
                .setConnected(worker.connected())
                .setProvider(hello.getProvider())
                .setModel(hello.getModel())
                .setModelVersion(hello.getModelVersion())
                .addAllCapabilities(hello.getCapabilitiesList())
                .build();
    }
}

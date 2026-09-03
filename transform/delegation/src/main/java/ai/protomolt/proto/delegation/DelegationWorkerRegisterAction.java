package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.RegisterWorkerRequest;
import ai.protomolt.proto.delegation.v1.RegisterWorkerResponse;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * Registers a worker on the delegation bridge: opens a real delegation stream, sends
 * the hello, and returns the coordinator's admission decision. The worker session
 * outlives any MCP session; one live stream per worker id.
 */
final class DelegationWorkerRegisterAction extends DelegationAction {

    /** The only delegation protocol version that exists. */
    private static final int PROTOCOL_VERSION = 1;

    DelegationWorkerRegisterAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-worker-register";
    }

    @Override
    public String description() {
        return "Registers this agent as a delegation worker: opens the worker stream, sends "
                + "the hello (identity, provider metadata, capabilities), and returns the "
                + "coordinator's admission decision. Check 'admitted' before accepting work.";
    }

    @Override
    public Descriptor requestType() {
        return RegisterWorkerRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return RegisterWorkerResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        RegisterWorkerRequest request = CatalogContract.as(
                input, RegisterWorkerRequest.getDefaultInstance(), name());
        // Omitted provider metadata arrives as the empty string, which is what an unset
        // proto3 string already is, so it copies across unconditionally.
        WorkerHello hello = WorkerHello.newBuilder()
                .setWorkerId(request.getWorkerId())
                .setProtocolVersion(PROTOCOL_VERSION)
                .setProvider(request.getProvider())
                .setModel(request.getModel())
                .setModelVersion(request.getModelVersion())
                .addAllCapabilities(request.getCapabilitiesList())
                .build();
        DelegationBridge.WorkerRegistration registration;
        try {
            registration = bridge.registerWorker(hello);
        } catch (RuntimeException e) {
            throw failure(hello.getWorkerId(), e);
        }
        RegisterWorkerResponse.Builder response = RegisterWorkerResponse.newBuilder()
                .setOk(true)
                .setWorkerId(registration.workerId())
                .setAdmitted(registration.admitted());
        if (registration.admitted()) {
            response.setSessionId(registration.sessionId());
        } else {
            response.setReason(registration.reason());
        }
        return response.build();
    }
}

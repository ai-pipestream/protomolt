package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.delegation.v1.CheckpointReference;
import ai.protomolt.proto.delegation.v1.OfferTaskRequest;
import ai.protomolt.proto.delegation.v1.OfferTaskResponse;
import ai.protomolt.proto.delegation.v1.TaskOffer;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.time.Duration;
import java.util.UUID;

/** Offers one bounded task attempt to an admitted worker, with its lease. */
final class DelegationOfferAction extends DelegationAction {

    /** The lease an offer runs on when the request does not choose one. */
    static final int DEFAULT_LEASE_SECONDS = 300;

    DelegationOfferAction(DelegationBridge bridge) {
        super(bridge);
    }

    @Override
    public String name() {
        return "delegation-offer";
    }

    @Override
    public String description() {
        return "Offers a bounded task to an admitted worker: the task spec (objective, scope, "
                + "constraints, required acceptance checks, context artifacts), the lease in "
                + "seconds, and an optional resume checkpoint, as a proto3-JSON "
                + "OfferTaskRequest. A spec may also declare a deliverable contract: a "
                + "serialized FileDescriptorSet as base64 bytes plus the full name of the "
                + "message the candidate must produce. The returned offer carries that "
                + "contract's JSON schema, rendered from the descriptor set. Returns the "
                + "emitted offer. The coordinator owns the lifecycle; this only adapts.";
    }

    @Override
    public Descriptor requestType() {
        return OfferTaskRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return OfferTaskResponse.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // worker_id, spec and the lease bound are declared on the request message, so the
        // contract check in parse refuses a malformed offer before it reaches here.
        OfferTaskRequest request = CatalogContract.as(
                input, OfferTaskRequest.getDefaultInstance(), name());
        // proto3 cannot distinguish an omitted task id from an empty one, and both mean the
        // same thing here: a new task, whose id the coordinator generates.
        String taskId = request.getTaskId().isEmpty()
                ? UUID.randomUUID().toString()
                : request.getTaskId();
        // Likewise for the lease: an omitted lease arrives as 0, which the request's
        // ignore_if_zero rule lets through precisely so this default can apply.
        int leaseSeconds = request.getLeaseSeconds() == 0
                ? DEFAULT_LEASE_SECONDS
                : request.getLeaseSeconds();
        // A contract is the caller's own input, so a set that does not link or does not
        // declare the type it names is bad input rather than a coordinator failure.
        if (request.getSpec().hasContract()) {
            try {
                DeliverableContracts.compile(request.getSpec().getContract());
            } catch (IllegalArgumentException e) {
                throw new ActionException("invalid-input",
                        name() + " cannot use the spec's deliverable contract: "
                                + e.getMessage());
            }
        }
        CheckpointReference resumeFrom = request.hasResumeFrom() ? request.getResumeFrom() : null;
        TaskOffer offer;
        try {
            offer = bridge.offer(request.getWorkerId(), taskId, request.getSpec(),
                    Duration.ofSeconds(leaseSeconds), resumeFrom);
        } catch (RuntimeException e) {
            throw failure(request.getWorkerId(), e);
        }
        return OfferTaskResponse.newBuilder()
                .setOk(true)
                .setTaskId(taskId)
                .setOffer(offer)
                .build();
    }
}

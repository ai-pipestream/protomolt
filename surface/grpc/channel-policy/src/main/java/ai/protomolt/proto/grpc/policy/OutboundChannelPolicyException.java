package ai.protomolt.proto.grpc.policy;

/** Thrown when an outbound target, transport, deadline, or channel budget is not permitted. */
public final class OutboundChannelPolicyException extends IllegalArgumentException {

    public OutboundChannelPolicyException(String message) {
        super(message);
    }
}

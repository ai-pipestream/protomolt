package ai.protomolt.proto.grpc.policy;

/** Thrown when an outbound target, transport, deadline, or channel budget is not permitted. */
public final class OutboundChannelPolicyException extends IllegalArgumentException {

    private final boolean channelLimitExhausted;

    public OutboundChannelPolicyException(String message) {
        this(message, false);
    }

    /**
     * @param message what was refused
     * @param channelLimitExhausted true when the only reason is that every channel slot is in
     *        use, so the same request may succeed once a slot is free
     */
    public OutboundChannelPolicyException(String message, boolean channelLimitExhausted) {
        super(message);
        this.channelLimitExhausted = channelLimitExhausted;
    }

    /** True when the refusal is a full channel budget, not a forbidden target or deadline. */
    public boolean channelLimitExhausted() {
        return channelLimitExhausted;
    }
}

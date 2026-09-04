package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelAcknowledgementPoint;
import ai.protomolt.proto.mesh.runtime.v1.ChannelDeliveryMode;
import ai.protomolt.proto.mesh.runtime.v1.ChannelOrderingGuarantee;
import ai.protomolt.proto.mesh.runtime.v1.ChannelOverflowAction;
import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.NamedChannelPolicy;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;

/** Explicit built-in policies for callers that need the local durable channel. */
public final class ChannelPolicies {

    public static final String LOCAL_DURABLE_ID = "local-durable-v1";

    private ChannelPolicies() {
    }

    public static NamedChannelPolicy localDurable() {
        return NamedChannelPolicy.newBuilder()
                .setPolicyId(LOCAL_DURABLE_ID)
                .setPolicy(ChannelPolicy.newBuilder()
                        .setDeliveryMode(
                                ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_LOCAL_DURABLE_WAL)
                        .setOverflowAction(
                                ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_BACKPRESSURE)
                        .setMaxItems(100_000)
                        .setMaxBytes(64L * 1024 * 1024)
                        .setMaximumAttempts(3)
                        .setRetryPolicyReference("default-retry")
                        .setDeadLetterChannelReference("default-dead-letter")
                        .setOrderingGuarantee(
                                ChannelOrderingGuarantee.CHANNEL_ORDERING_GUARANTEE_NONE)
                        .setConcurrencyBound(1_000)
                        .setAcknowledgementPoint(ChannelAcknowledgementPoint
                                .CHANNEL_ACKNOWLEDGEMENT_POINT_DESCENDANT_SETTLEMENT)
                        .setRequiredCompletionPolicy(
                                CompletionPolicy.COMPLETION_POLICY_STRICT))
                .build();
    }
}

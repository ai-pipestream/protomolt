package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Opens the channel for a verb invocation. The default honors the verbs' {@code tls} input:
 * plaintext unless the caller asks for TLS (system trust roots).
 */
@FunctionalInterface
public interface ChannelFactory {

    ManagedChannel open(String target, boolean tls);

    /** Validates a target before any schema or network work begins. */
    default void validateTarget(String target, boolean tls) {
    }

    /** Validates a call deadline when this factory is backed by a host policy. */
    default void validateDeadline(long deadlineMillis) {
    }

    /** Returns the policy backing this factory, or null for an unbounded custom test seam. */
    default OutboundChannelPolicy policy() {
        return null;
    }

    static ChannelFactory standard() {
        return standard(OutboundChannelPolicy.defaults());
    }

    /** Creates a factory that validates every target and leases a bounded channel slot. */
    static ChannelFactory standard(OutboundChannelPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("channel policy must not be null");
        }
        return new ChannelFactory() {
            @Override
            public ManagedChannel open(String target, boolean tls) {
                return policy.open(target, tls, canonical -> {
                    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(canonical);
                    if (!tls) {
                        builder.usePlaintext();
                    }
                    return builder.build();
                });
            }

            @Override
            public void validateTarget(String target, boolean tls) {
                policy.validateTarget(target, tls);
            }

            @Override
            public void validateDeadline(long deadlineMillis) {
                policy.validateDeadline(deadlineMillis);
            }

            @Override
            public OutboundChannelPolicy policy() {
                return policy;
            }
        };
    }
}

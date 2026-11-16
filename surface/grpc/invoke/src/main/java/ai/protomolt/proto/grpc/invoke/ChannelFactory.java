package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Opens the channel for a verb invocation. The default honors the verbs' {@code tls} input:
 * plaintext unless the caller asks for TLS (system trust roots).
 */
@FunctionalInterface
public interface ChannelFactory {

    ManagedChannel open(String target, boolean tls);

    /**
     * Opens a TLS channel that presents host-resolved channel credentials: a custom trust
     * bundle, a client certificate, or both. A factory that cannot honor them refuses
     * rather than opening a channel without them.
     */
    default ManagedChannel open(String target, ChannelCredentials credentials) {
        throw new IllegalArgumentException(
                "this channel factory cannot present custom TLS credentials");
    }

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

    /**
     * Evicts any pooled channels associated with {@code credentials} so rotated or revoked TLS
     * material is never reused on subsequent calls. Unpooled factories ignore this notification.
     */
    default void evict(ChannelCredentials credentials) {
    }

    /** A {@link ChannelFactory} that owns reusable underlying channels and shuts them down on close. */
    interface Closeable extends ChannelFactory, AutoCloseable {
        @Override
        void close();
    }

    static ChannelFactory standard() {
        return standard(OutboundChannelPolicy.defaults());
    }

    /**
     * Creates a bounded pooled factory with {@link OutboundChannelPolicy#defaults()} that reuses
     * healthy credentialed TLS channels across per-call policy leases until evicted or closed.
     */
    static Closeable pooled() {
        return pooled(OutboundChannelPolicy.defaults());
    }

    /**
     * Creates a bounded pooled factory backed by {@code policy}. Each call still acquires and
     * releases a policy concurrency slot when its returned channel is shut down, while healthy
     * credentialed TLS channels return to the pool until evicted or {@link Closeable#close()} runs.
     */
    static Closeable pooled(OutboundChannelPolicy policy) {
        return new PooledChannelFactory(policy);
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
            public ManagedChannel open(String target, ChannelCredentials credentials) {
                if (credentials == null) {
                    throw new IllegalArgumentException("channel credentials must not be null");
                }
                return policy.open(target, true,
                        canonical -> Grpc.newChannelBuilder(canonical, credentials).build());
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

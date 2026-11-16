package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;

/**
 * Package-private hook allowing TLS resolvers that cache {@link io.grpc.ChannelCredentials} to
 * notify pooled {@link ChannelFactory} instances when underlying certificate or key files rotate.
 */
interface EvictableTlsResolver {

    void registerEvictionTarget(ChannelFactory channels);

    static void bind(ProfileCredentialResolver credentials, ChannelFactory channels) {
        if (credentials instanceof EvictableTlsResolver evictable && channels != null) {
            evictable.registerEvictionTarget(channels);
        }
    }
}

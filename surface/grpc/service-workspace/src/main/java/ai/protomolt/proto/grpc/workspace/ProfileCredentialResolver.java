package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import io.grpc.ChannelCredentials;
import io.grpc.Metadata;
import java.util.Objects;

/** Host-owned outbound credentials; resolved material never enters an action request. */
@FunctionalInterface
public interface ProfileCredentialResolver {
    Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint) throws ActionException;

    /**
     * TLS channel credentials for an endpoint naming a trust or client-certificate reference.
     * A resolver without TLS support refuses, so such an endpoint is never reached with
     * default trust or without its client certificate.
     */
    default ChannelCredentials channelCredentials(ServiceProfile profile,
                                                  ServiceEndpoint endpoint)
            throws ActionException {
        throw new ActionException("unsupported-transport",
                "custom trust and client-certificate references need a configured TLS resolver");
    }

    /**
     * A resolver answering bearer headers from {@code bearer} and TLS material from
     * {@code tls}. A null {@code bearer} means no endpoint may name a credential reference.
     */
    static ProfileCredentialResolver withTls(ProfileCredentialResolver bearer,
                                             ProfileTlsResolver tls) {
        Objects.requireNonNull(tls, "tls");
        return new ProfileCredentialResolver() {
            @Override
            public Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint)
                    throws ActionException {
                if (bearer == null) {
                    throw new ActionException("credential-unavailable",
                            "outbound profile credentials are not configured");
                }
                return bearer.headers(profile, endpoint);
            }

            @Override
            public ChannelCredentials channelCredentials(ServiceProfile profile,
                                                         ServiceEndpoint endpoint)
                    throws ActionException {
                return tls.channelCredentials(profile, endpoint);
            }
        };
    }
}

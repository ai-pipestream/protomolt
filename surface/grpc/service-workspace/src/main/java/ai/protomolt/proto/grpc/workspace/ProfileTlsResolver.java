package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import io.grpc.ChannelCredentials;

/**
 * Host-owned TLS material for an endpoint's {@code trust_ref} and
 * {@code client_certificate_ref}. The profile carries only the opaque references; the host
 * turns them into channel credentials each time a channel opens, so rotated material is
 * picked up without a restart. Key material never enters an action request, a stored
 * profile, a reply or an error.
 */
@FunctionalInterface
public interface ProfileTlsResolver {

    /**
     * Channel credentials for one TLS endpoint naming at least one TLS reference. Fails with
     * {@code unsupported-transport} when called for a non-TLS endpoint, and with
     * {@code credential-unavailable} when a reference is not approved for the endpoint or its
     * material cannot be loaded; never answers null.
     */
    ChannelCredentials channelCredentials(ServiceProfile profile, ServiceEndpoint endpoint)
            throws ActionException;
}

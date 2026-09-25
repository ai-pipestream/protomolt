package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import io.grpc.Metadata;

/** Rejects unresolved TLS material and keeps resolved bearer headers off the wire contract. */
final class ProfileCredentials {
    private ProfileCredentials() {}

    static Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint,
                            ProfileCredentialResolver resolver) throws ActionException {
        if (!endpoint.getTrustRef().isBlank() || !endpoint.getClientCertificateRef().isBlank()) {
            throw new ActionException("unsupported-transport",
                    "custom trust and client-certificate references need a configured TLS resolver");
        }
        if (endpoint.getCredentialRef().isBlank()) return new Metadata();
        if (resolver == null) {
            throw new ActionException("credential-unavailable",
                    "outbound profile credentials are not configured");
        }
        return resolver.headers(profile, endpoint);
    }
}

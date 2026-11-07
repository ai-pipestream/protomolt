package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/** Rejects unresolved TLS material and keeps resolved bearer headers off the wire contract. */
final class ProfileCredentials {
    private ProfileCredentials() {}

    /**
     * The call credentials for one endpoint, or null when it names no credential reference.
     * Resolves once up front so a missing or malformed reference still fails before a channel
     * opens; the returned credentials re-resolve on every call, so material that rotates
     * between calls is never silently stale.
     */
    static CallCredentials callCredentials(ServiceProfile profile, ServiceEndpoint endpoint,
                                           ProfileCredentialResolver resolver)
            throws ActionException {
        headers(profile, endpoint, resolver);
        if (endpoint.getCredentialRef().isBlank()) {
            return null;
        }
        return new ProfileCallCredentials(profile, endpoint, resolver);
    }

    /**
     * Fails when the endpoint's credential reference cannot be resolved. Inspection never
     * contacts the target, so this local check is how inspection reports a profile whose
     * credential is missing rather than describing a service the next call cannot reach.
     */
    static void requireResolvable(ServiceProfile profile, ServiceEndpoint endpoint,
                                  ProfileCredentialResolver resolver) throws ActionException {
        if (endpoint.getCredentialRef().isBlank()) {
            return;
        }
        if (resolver == null) {
            throw new ActionException("credential-unavailable",
                    "outbound profile credentials are not configured");
        }
        resolver.headers(profile, endpoint);
    }

    /** A channel that presents the credentials on every call made through it. */
    static Channel decorate(Channel channel, CallCredentials credentials) {
        return ClientInterceptors.intercept(channel, new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
                return next.newCall(method, options.withCallCredentials(credentials));
            }
        });
    }

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

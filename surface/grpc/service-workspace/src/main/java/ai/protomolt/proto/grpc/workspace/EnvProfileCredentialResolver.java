package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.inference.spi.CredentialResolver;
import ai.protomolt.proto.inference.spi.EnvCredentialResolver;
import io.grpc.Metadata;
import java.util.Objects;
import java.util.Set;

/** Environment-backed bearer credentials constrained to exact host-approved endpoints. */
public final class EnvProfileCredentialResolver implements ProfileCredentialResolver {
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    public record Binding(String profile, String endpoint, String host, int port,
                          Transport transport, String credentialRef) {
        public Binding {
            Objects.requireNonNull(profile);
            Objects.requireNonNull(endpoint);
            Objects.requireNonNull(host);
            Objects.requireNonNull(transport);
            CredentialResolver.checkFormat(credentialRef);
            if (!credentialRef.startsWith("env:") || port < 1 || port > 65535) {
                throw new IllegalArgumentException("unsupported profile credential binding");
            }
        }
    }

    private final Set<Binding> allowed;
    private final CredentialResolver secrets;

    public EnvProfileCredentialResolver(Set<Binding> allowed) {
        this(allowed, new EnvCredentialResolver());
    }

    public EnvProfileCredentialResolver(Set<Binding> allowed, CredentialResolver secrets) {
        this.allowed = Set.copyOf(Objects.requireNonNull(allowed));
        this.secrets = Objects.requireNonNull(secrets);
    }

    @Override public Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint)
            throws ActionException {
        Binding actual;
        try {
            actual = new Binding(profile.getName(), endpoint.getName(), endpoint.getHost(),
                    endpoint.getPort(), endpoint.getTransport(), endpoint.getCredentialRef());
        } catch (IllegalArgumentException invalid) {
            throw new ActionException("credential-unavailable",
                    "profile credential reference is unsupported");
        }
        if (!allowed.contains(actual)) {
            throw new ActionException("credential-unavailable",
                    "profile endpoint is not approved for outbound credentials");
        }
        try {
            String bearer = CredentialResolver.resolveBearer(secrets, actual.credentialRef());
            Metadata headers = new Metadata();
            headers.put(AUTHORIZATION, "Bearer " + bearer);
            return headers;
        } catch (RuntimeException refused) {
            throw new ActionException("credential-unavailable",
                    "profile credential could not be resolved");
        }
    }
}

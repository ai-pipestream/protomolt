package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * gRPC call credentials for one profile endpoint. Every call re-resolves the endpoint's
 * host credential reference through the configured resolver, so rotated material is picked
 * up and resolved values are never cached on a channel or carried into profiles,
 * descriptors, replies, or logs. A reference that stops resolving fails the call with
 * {@link Status.Code#UNAUTHENTICATED}; nothing falls back to an anonymous call.
 */
final class ProfileCallCredentials extends CallCredentials {

    private final ServiceProfile profile;
    private final ServiceEndpoint endpoint;
    private final ProfileCredentialResolver resolver;

    ProfileCallCredentials(ServiceProfile profile, ServiceEndpoint endpoint,
                           ProfileCredentialResolver resolver) {
        this.profile = Objects.requireNonNull(profile, "profile");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor executor,
                                     MetadataApplier applier) {
        final Metadata headers;
        try {
            headers = resolver.headers(profile, endpoint);
        } catch (ActionException | RuntimeException unavailable) {
            // The resolver contract already withholds the reference and material;
            // the status description is fixed text for the same reason.
            applier.fail(Status.UNAUTHENTICATED.withDescription(
                    "profile credential could not be resolved"));
            return;
        }
        applier.apply(headers);
    }

    @Override
    public void thisUsesUnstableApi() {
    }
}

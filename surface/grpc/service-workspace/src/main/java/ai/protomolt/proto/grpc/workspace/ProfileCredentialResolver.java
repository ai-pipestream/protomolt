package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import io.grpc.Metadata;

/** Host-owned outbound credentials; resolved material never enters an action request. */
@FunctionalInterface
public interface ProfileCredentialResolver {
    Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint) throws ActionException;
}

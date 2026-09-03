package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;

/**
 * What happens after a profile is reflected and stored: the seam through which a
 * successful register or refresh turns the profile's methods into live verbs.
 */
@FunctionalInterface
interface ProfileStored {

    void stored(ServiceProfile profile) throws ActionException;
}

package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;

/**
 * What happens after a profile is reflected and stored: the seam through which a
 * successful register or refresh turns the profile's methods into live verbs.
 */
@FunctionalInterface
interface ProfileStored {

    void stored(ServiceProfile profile) throws ActionException;
}

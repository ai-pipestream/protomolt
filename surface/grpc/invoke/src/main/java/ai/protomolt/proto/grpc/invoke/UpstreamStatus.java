package ai.protomolt.proto.grpc.invoke;

import io.grpc.CallCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.Status;

/**
 * The part of an upstream failure an action may surface. Unauthenticated calls keep the
 * status as it arrived. Calls that presented host-resolved call or channel credentials keep
 * only the status code: the description, cause, and trailers are withheld, because an
 * authenticated endpoint can echo presented credential or client-identity details in its
 * refusal.
 */
public final class UpstreamStatus {

    private UpstreamStatus() {
    }

    /** The status safe to render for a call that presented the given call credentials, if any. */
    public static Status reply(Status status, CallCredentials credentials) {
        return reply(status, credentials != null);
    }

    /**
     * The status safe to render for a call that presented the given call or TLS channel
     * credentials, if any.
     */
    public static Status reply(Status status, CallCredentials credentials,
                               ChannelCredentials channelCredentials) {
        return reply(status, credentials != null || channelCredentials != null);
    }

    /** The status safe to render when {@code credentialed} tells whether host credentials ran. */
    public static Status reply(Status status, boolean credentialed) {
        return credentialed ? Status.fromCode(status.getCode()) : status;
    }
}

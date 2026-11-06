package ai.protomolt.proto.grpc.invoke;

import io.grpc.CallCredentials;
import io.grpc.Status;

/**
 * The part of an upstream failure an action may surface. Unauthenticated calls keep the
 * status as it arrived. Calls that presented profile credentials keep only the status code:
 * the description, cause, and trailers are withheld, because an authenticated endpoint can
 * echo the presented credential in its refusal.
 */
public final class UpstreamStatus {

    private UpstreamStatus() {
    }

    /** The status safe to render for a call that presented the given credentials, if any. */
    public static Status reply(Status status, CallCredentials credentials) {
        return credentials == null ? status : Status.fromCode(status.getCode());
    }
}

package ai.protomolt.proto.authz.grpc;

import ai.protomolt.proto.actions.Caller;
import io.grpc.Context;

/**
 * The resolved {@link Caller} on the gRPC call context. The authenticating interceptor puts
 * it there once per call; handlers read it with {@link #current()}. A call with no entry is
 * one no authenticating interceptor saw — an open, trusted-network server — and runs with
 * process authority, exactly as it does today.
 */
public final class CallerContexts {

    /** The call's resolved caller; absent on an unauthenticated (open) server. */
    public static final Context.Key<Caller> CALLER = Context.key("protomolt-caller");

    /** Complete trusted authentication result; never populated from request identity headers. */
    public static final Context.Key<ai.protomolt.proto.authz.AuthenticatedCaller> AUTHENTICATED_CALLER =
            Context.key("protomolt-authenticated-caller");

    /** Empty on a trusted in-process call without an authenticating interceptor. */
    public static java.util.Optional<ai.protomolt.proto.authz.AuthenticatedCaller> authentication() {
        var authentication = AUTHENTICATED_CALLER.get();
        if (authentication != null && !authentication.caller().equals(CALLER.get()))
            throw new IllegalStateException("Authentication context differs from caller context");
        return java.util.Optional.ofNullable(authentication);
    }

    private CallerContexts() {
    }

    /** The current call's caller, or the operator when no interceptor resolved one. */
    public static Caller current() {
        Caller caller = CALLER.get();
        return caller == null ? Caller.operator() : caller;
    }
}

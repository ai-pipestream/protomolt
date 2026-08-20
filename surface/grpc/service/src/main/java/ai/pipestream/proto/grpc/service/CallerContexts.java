package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.Caller;
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

    private CallerContexts() {
    }

    /** The current call's caller, or the operator when no interceptor resolved one. */
    public static Caller current() {
        Caller caller = CALLER.get();
        return caller == null ? Caller.operator() : caller;
    }
}

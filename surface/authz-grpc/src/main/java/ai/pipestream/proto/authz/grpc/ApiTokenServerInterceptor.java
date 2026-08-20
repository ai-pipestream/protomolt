package ai.pipestream.proto.authz.grpc;

import ai.pipestream.proto.actions.Caller;
import ai.pipestream.proto.authz.CallerResolver;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Call credential check for the gRPC surface: every call must carry a credential in
 * {@code api_token} metadata (or {@code authorization: Bearer <token>}). The operator token
 * is compared in constant time and runs with process authority; with a {@link CallerResolver},
 * a credential a mounted access policy names runs as its principal, placed on the call
 * context for the per-method scope check. Applied server-wide, reflection included — grpcurl
 * passes it with {@code -H 'api_token: ...'}.
 */
public final class ApiTokenServerInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final byte[] expected;
    private final CallerResolver resolver;

    public ApiTokenServerInterceptor(String expectedToken) {
        this(expectedToken, null);
    }

    public ApiTokenServerInterceptor(String expectedToken, CallerResolver resolver) {
        this.expected = Objects.requireNonNull(expectedToken, "expectedToken")
                .getBytes(StandardCharsets.UTF_8);
        this.resolver = resolver;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String presented = headers.get(API_TOKEN);
        if (presented == null) {
            String authorization = headers.get(AUTHORIZATION);
            if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
                presented = authorization.substring(7).trim();
            }
        }
        if (presented == null || presented.isBlank()) {
            call.close(Status.UNAUTHENTICATED.withDescription("Missing API token 'api_token'"),
                    new Metadata());
            return new ServerCall.Listener<>() { };
        }
        Caller caller;
        if (MessageDigest.isEqual(expected, presented.getBytes(StandardCharsets.UTF_8))) {
            caller = Caller.operator();
        } else if (resolver != null) {
            caller = resolver.resolve(presented).orElse(null);
        } else {
            caller = null;
        }
        if (caller == null) {
            // The refusal carries nothing an attacker could use to recover a credential.
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid API token"), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        return Contexts.interceptCall(Context.current().withValue(CallerContexts.CALLER, caller),
                call, headers, next);
    }
}

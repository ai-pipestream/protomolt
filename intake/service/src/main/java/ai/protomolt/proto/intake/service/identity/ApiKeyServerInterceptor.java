package ai.protomolt.proto.intake.service.identity;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;

/**
 * Authenticates every intake call from gRPC metadata. The credential rides
 * {@code x-api-key}, or the standard {@code authorization} header with an
 * optional {@code Bearer} prefix; no request message carries identity.
 *
 * <p>Resolution failures split exactly as the intake contract demands:
 * a missing, blank, or unknown credential is {@code UNAUTHENTICATED} (who is
 * calling was never established); scope violations are the service's job and
 * surface later as {@code PERMISSION_DENIED}. A failing key store is
 * {@code INTERNAL} — never misreported as a bad key.
 */
public final class ApiKeyServerInterceptor implements ServerInterceptor {

    /** The metadata key intake documents first: {@code x-api-key}. */
    public static final Metadata.Key<String> API_KEY =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    /** The standard {@code authorization} header, accepted with or without a {@code Bearer} prefix. */
    public static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /** The resolved scope of the authenticated call, for handlers downstream of this interceptor. */
    public static final Context.Key<IntakeScope> SCOPE = Context.key("intake-scope");

    private final ApiKeyIdentityResolver resolver;

    public ApiKeyServerInterceptor(ApiKeyIdentityResolver resolver) {
        if (resolver == null) {
            throw new IllegalArgumentException("resolver must not be null");
        }
        this.resolver = resolver;
    }

    /**
     * The scope of the current call. Handlers call this instead of touching
     * the context key directly.
     *
     * @throws IllegalStateException when called outside an intercepted call —
     *         a wiring bug, not a caller error
     */
    public static IntakeScope currentScope() {
        IntakeScope scope = SCOPE.get();
        if (scope == null) {
            throw new IllegalStateException(
                    "no IntakeScope on the call context; is ApiKeyServerInterceptor installed?");
        }
        return scope;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String credential = credentialFrom(headers);
        if (credential == null || credential.isBlank()) {
            call.close(
                    Status.UNAUTHENTICATED.withDescription(
                            "missing API key: send x-api-key or authorization metadata"),
                    new Metadata());
            return new ServerCall.Listener<>() {};
        }
        Optional<IntakeScope> scope;
        try {
            scope = resolver.resolve(credential);
        } catch (RuntimeException e) {
            call.close(
                    Status.INTERNAL.withDescription("API key store failure"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        if (scope.isEmpty()) {
            call.close(Status.UNAUTHENTICATED.withDescription("unknown API key"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        Context authenticated = Context.current().withValue(SCOPE, scope.get());
        return Contexts.interceptCall(authenticated, call, headers, next);
    }

    private static String credentialFrom(Metadata headers) {
        String apiKey = headers.get(API_KEY);
        if (apiKey != null && !apiKey.isBlank()) {
            return apiKey.trim();
        }
        String authorization = headers.get(AUTHORIZATION);
        if (authorization == null) {
            return null;
        }
        String value = authorization.trim();
        if (value.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return value.substring(7).trim();
        }
        return value;
    }
}

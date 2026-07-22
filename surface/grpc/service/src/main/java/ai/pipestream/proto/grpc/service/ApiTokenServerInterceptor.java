package ai.pipestream.proto.grpc.service;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Shared-secret call credential check for the gRPC surface: every call must carry the token
 * in {@code api_token} metadata (or {@code authorization: Bearer <token>}), compared in
 * constant time. Applied server-wide, reflection included — grpcurl passes it with
 * {@code -H 'api_token: ...'}.
 */
public final class ApiTokenServerInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final byte[] expected;

    public ApiTokenServerInterceptor(String expectedToken) {
        this.expected = Objects.requireNonNull(expectedToken, "expectedToken")
                .getBytes(StandardCharsets.UTF_8);
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
        if (!MessageDigest.isEqual(expected, presented.getBytes(StandardCharsets.UTF_8))) {
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid API token"), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        return next.startCall(call, headers);
    }
}

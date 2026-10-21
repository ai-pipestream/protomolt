package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Caller;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The request-to-caller seam for request-shaped transports (the Connect gateway, browser
 * routes): resolves the credential a request presents, in whatever header carries it, to the
 * caller the request runs as. {@link CallerResolver} says who a bare credential belongs to;
 * this says which credential a request presents and how it is read, so a bearer token and a
 * console session cookie compose on one host through {@link #firstOf(List)}.
 *
 * <p>Contract: {@link Optional#empty()} means the request presents no usable credential
 * (none, unknown, expired, or ambiguous), and the transport answers unauthenticated (HTTP
 * 401). A credential store failure (an unreachable identity provider, a broken database) is
 * a thrown {@link IllegalStateException}, which the transport answers as unavailable (HTTP
 * 503): an outage never reads as a bad credential. Implementations must be safe for
 * concurrent use.
 */
@FunctionalInterface
public interface RequestCallerResolver {

    /** The header carrying the credential directly, as on the gRPC surface. */
    String API_TOKEN_HEADER = "api_token";

    /** The standard header carrying {@code Bearer <credential>}. */
    String AUTHORIZATION_HEADER = "Authorization";

    /**
     * Resolves the caller a request runs as.
     *
     * @param headers the request's headers
     * @return the authenticated caller, or empty when the request presents no usable credential
     * @throws IllegalStateException when the credential store cannot answer
     */
    Optional<AuthenticatedCaller> resolve(RequestHeaders headers);

    /**
     * The operator token alone: a request presenting it runs with process authority, and any
     * other credential is unknown.
     *
     * @param operatorToken the operator credential; nonblank
     * @return the resolver
     * @see #bearer(String, CallerResolver)
     */
    static RequestCallerResolver bearer(String operatorToken) {
        return bearer(operatorToken, null, "operatorToken");
    }

    /**
     * The HTTP twin of the gRPC {@code ApiTokenServerInterceptor}: the credential is the
     * {@value #API_TOKEN_HEADER} header, or else an {@value #AUTHORIZATION_HEADER} header with
     * the {@code Bearer} scheme (any case). The operator token, compared in constant time,
     * runs with process authority; any other credential goes to {@code resolver}. A request
     * carrying the chosen header more than once presents an ambiguous credential and resolves
     * empty, as does a blank one.
     *
     * @param operatorToken the operator credential; nonblank
     * @param resolver resolves every credential other than the operator token
     * @return the resolver
     */
    static RequestCallerResolver bearer(String operatorToken, CallerResolver resolver) {
        return bearer(operatorToken, Objects.requireNonNull(resolver, "resolver"), "operatorToken");
    }

    private static RequestCallerResolver bearer(String operatorToken, CallerResolver resolver,
            String name) {
        Objects.requireNonNull(operatorToken, name);
        if (operatorToken.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        byte[] expected = operatorToken.getBytes(StandardCharsets.UTF_8);
        return headers -> {
            Objects.requireNonNull(headers, "headers");
            Optional<String> presented = presentedCredential(headers);
            if (presented.isEmpty()) {
                return Optional.empty();
            }
            String credential = presented.get();
            if (MessageDigest.isEqual(expected, credential.getBytes(StandardCharsets.UTF_8))) {
                return Optional.of(AuthenticatedCaller.unbound(Caller.operator()));
            }
            return resolver == null ? Optional.empty() : resolver.resolveAuthenticated(credential);
        };
    }

    private static Optional<String> presentedCredential(RequestHeaders headers) {
        List<String> tokens = headers.values(API_TOKEN_HEADER);
        if (!tokens.isEmpty()) {
            return tokens.size() == 1 ? nonBlank(tokens.getFirst()) : Optional.empty();
        }
        List<String> authorizations = headers.values(AUTHORIZATION_HEADER);
        if (authorizations.size() != 1) {
            return Optional.empty();
        }
        String authorization = authorizations.getFirst();
        if (!authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return Optional.empty();
        }
        return nonBlank(authorization.substring(7));
    }

    private static Optional<String> nonBlank(String credential) {
        String trimmed = credential.trim();
        return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    }

    /**
     * A resolver asking each of {@code resolvers} in order and answering the first present
     * caller, so a host mounts its credential kinds side by side. Empty only when every
     * resolver answers empty; a store failure propagates from whichever resolver it struck,
     * without asking the rest.
     *
     * @param resolvers the resolvers, in the order they are asked; at least one, no nulls
     * @return the composed resolver
     */
    static RequestCallerResolver firstOf(List<RequestCallerResolver> resolvers) {
        Objects.requireNonNull(resolvers, "resolvers");
        if (resolvers.isEmpty()) {
            throw new IllegalArgumentException("firstOf requires at least one resolver");
        }
        List<RequestCallerResolver> ordered = List.copyOf(resolvers);
        return headers -> {
            for (RequestCallerResolver resolver : ordered) {
                Optional<AuthenticatedCaller> caller = resolver.resolve(headers);
                if (caller.isPresent()) {
                    return caller;
                }
            }
            return Optional.empty();
        };
    }
}

package ai.pipestream.proto.authz;

import ai.pipestream.proto.actions.Caller;
import java.util.List;
import java.util.Optional;

/**
 * The credential-to-caller seam: resolves a presented credential to the principal it was
 * minted for. This is where external stores plug in — the access-policy digests,
 * {@link OidcCallerResolver OIDC introspection}, a JDBC table — mirroring the intake
 * service's key-store seam.
 *
 * <p>Contract: {@link Optional#empty()} means the credential is unknown or no longer valid;
 * the transport maps that to its unauthenticated refusal. Resolvers must not throw for a
 * merely unknown credential and must be safe for concurrent use. A store failure (an
 * unreachable IdP, a broken database) is a thrown {@link IllegalStateException} — an outage
 * must never masquerade as a bad-credential verdict. The credential arrives exactly as
 * presented, already stripped of transport framing, and is never null.
 */
public interface CallerResolver {

    /** Resolves a presented credential to its caller, or empty when it is unknown. */
    Optional<Caller> resolve(String credential);

    /**
     * A resolver asking each of {@code resolvers} in order and answering the first match,
     * so a deployment mounts its stores side by side — the access policy first, then the
     * external ones. Empty only when every resolver answers empty; a store failure
     * propagates from whichever resolver it struck.
     */
    static CallerResolver chain(List<? extends CallerResolver> resolvers) {
        if (resolvers == null || resolvers.isEmpty()) {
            throw new IllegalArgumentException("chain requires at least one resolver");
        }
        List<CallerResolver> ordered = List.copyOf(resolvers);
        return credential -> {
            for (CallerResolver resolver : ordered) {
                Optional<Caller> caller = resolver.resolve(credential);
                if (caller.isPresent()) {
                    return caller;
                }
            }
            return Optional.empty();
        };
    }
}

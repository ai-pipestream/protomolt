package ai.pipestream.proto.authz;

import ai.pipestream.proto.actions.Caller;
import java.util.Optional;

/**
 * The credential-to-caller seam: resolves a presented credential to the principal it was
 * minted for. This is where external stores plug in later — OIDC introspection, a JDBC
 * table — mirroring the intake door's key-store seam.
 *
 * <p>Contract: {@link Optional#empty()} means the credential is unknown or no longer valid;
 * the transport maps that to its unauthenticated refusal. Resolvers must not throw for a
 * merely unknown credential and must be safe for concurrent use. The credential arrives
 * exactly as presented, already stripped of transport framing, and is never null.
 */
public interface CallerResolver {

    /** Resolves a presented credential to its caller, or empty when it is unknown. */
    Optional<Caller> resolve(String credential);
}

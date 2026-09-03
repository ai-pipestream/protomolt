package ai.protomolt.proto.intake.service.identity;

import java.util.Optional;

/**
 * The key-store seam: resolves a presented API-key credential to the
 * {@link IntakeScope} the key was minted with.
 *
 * <p>This is where external key stores plug in — an IdP that treats API keys
 * as client credentials (Keycloak), a JDBC-backed store for air-gapped
 * deployments, or an in-memory table for tests and demos. The service never
 * sees how keys are stored or rotated; rotation-with-grace falls out of the
 * model naturally, because a store may resolve several concurrently valid
 * credentials (current and grace-window keys) to the same scope.
 *
 * <p>Contract: {@link Optional#empty()} means the credential is unknown or no
 * longer valid — the caller maps that to {@code UNAUTHENTICATED}. Resolvers
 * must not throw for a merely unknown key; exceptions are for the store itself
 * failing, and surface as {@code INTERNAL}. Resolvers must be safe for
 * concurrent use.
 */
public interface ApiKeyIdentityResolver {

    /**
     * Resolves a presented credential to its scope.
     *
     * @param credential the API key exactly as presented (already stripped of
     *        transport framing such as a {@code Bearer} prefix); never null
     * @return the scope the key carries, or empty when the key is unknown
     */
    Optional<IntakeScope> resolve(String credential);
}

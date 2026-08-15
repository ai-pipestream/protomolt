package ai.pipestream.proto.intake.service.identity;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A concurrent in-memory key table: the resolver for tests, demos, and
 * env-seeded standalone deployments. Several keys may map to one scope — that
 * is exactly what a rotation grace window looks like — and {@link #revoke}
 * ends a key's validity immediately.
 */
public final class InMemoryApiKeyIdentityResolver implements ApiKeyIdentityResolver {

    private final Map<String, IntakeScope> keys = new ConcurrentHashMap<>();

    @Override
    public Optional<IntakeScope> resolve(String credential) {
        return Optional.ofNullable(keys.get(credential));
    }

    /**
     * Registers (or replaces) a key.
     *
     * @param credential the key material; must not be blank
     * @param scope the scope the key carries
     * @return this resolver, for seeding chains
     */
    public InMemoryApiKeyIdentityResolver register(String credential, IntakeScope scope) {
        if (credential == null || credential.isBlank()) {
            throw new IllegalArgumentException("credential must not be blank");
        }
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        keys.put(credential, scope);
        return this;
    }

    /** Removes a key; unknown keys are a no-op. */
    public void revoke(String credential) {
        keys.remove(credential);
    }
}

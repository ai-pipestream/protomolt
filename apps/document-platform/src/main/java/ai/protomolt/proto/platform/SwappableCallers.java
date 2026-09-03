package ai.protomolt.proto.platform;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.CallerResolver;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The caller resolver handed to the serving roles before the config lane
 * exists: the servers are wired inside the composer boot, while an access
 * policy may arrive as a config document. Until a policy swaps in, every
 * credential except the operator token resolves to nothing and the
 * transports refuse it unauthenticated — fail-closed, exactly the stance
 * the screening and taxonomy mounts hold while their data is not yet
 * mounted.
 */
final class SwappableCallers implements CallerResolver {

    private final AtomicReference<CallerResolver> delegate =
            new AtomicReference<>(credential -> Optional.empty());

    @Override
    public Optional<Caller> resolve(String credential) {
        return delegate.get().resolve(credential);
    }

    /** Swaps the live policy's resolver; the next request sees it. */
    void swap(CallerResolver resolver) {
        if (resolver == null) {
            throw new IllegalArgumentException("resolver must not be null");
        }
        delegate.set(resolver);
    }
}

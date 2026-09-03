package ai.protomolt.proto.search.embedding;

import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Discovers {@link EmbeddingProvider}s via {@link ServiceLoader}.
 */
public final class EmbeddingProviders {

    private EmbeddingProviders() {
    }

    /**
     * All discovered providers, keyed by {@link EmbeddingProvider#providerId()}.
     *
     * @throws IllegalStateException when two providers register the same id; a
     *         duplicate id is a classpath misconfiguration, and silently keeping
     *         one could derive a corpus with the wrong model
     */
    public static Map<String, EmbeddingProvider> all() {
        return indexById(ServiceLoader.load(EmbeddingProvider.class));
    }

    /** Keys discovered providers by id, rejecting duplicates rather than last-wins. */
    static Map<String, EmbeddingProvider> indexById(Iterable<EmbeddingProvider> discovered) {
        Map<String, EmbeddingProvider> providers = new LinkedHashMap<>();
        for (EmbeddingProvider provider : discovered) {
            EmbeddingProvider previous = providers.putIfAbsent(provider.providerId(), provider);
            if (previous != null) {
                throw new IllegalStateException("Duplicate embedding provider id '"
                        + provider.providerId() + "': " + previous.getClass().getName()
                        + " and " + provider.getClass().getName());
            }
        }
        return Map.copyOf(providers);
    }

    /**
     * The provider registered under {@code providerId}. Every discovered
     * provider is instantiated to find the match; the instances not selected
     * are closed before returning, since the caller can only own what it gets.
     *
     * @throws IllegalArgumentException when no such provider is on the classpath,
     *         listing the ids that are
     */
    public static EmbeddingProvider byId(String providerId) {
        return selectById(ServiceLoader.load(EmbeddingProvider.class), providerId);
    }

    /** Keys discovered providers, returns the match, closes the rest. */
    static EmbeddingProvider selectById(Iterable<EmbeddingProvider> discovered, String providerId) {
        Map<String, EmbeddingProvider> providers = indexById(discovered);
        EmbeddingProvider selected = providers.get(providerId);
        providers.forEach((id, provider) -> {
            if (provider != selected) {
                provider.close();
            }
        });
        if (selected == null) {
            throw new IllegalArgumentException("Unknown embedding provider '" + providerId
                    + "'. Available providers: " + String.join(", ", providers.keySet()));
        }
        return selected;
    }

    /**
     * The provider a chunking policy's embedding spec names, validated against the
     * spec before any text is embedded: the provider registered under the spec's
     * model id must produce vectors of the spec's dimension. This is the lane's
     * provider resolution; a policy can never silently derive with the wrong model
     * or the wrong vector space.
     *
     * <p>Resolution may load the model (a lazily configured provider learns its
     * dimension by loading), so an unconfigured provider fails here, naming its
     * configuration knobs, rather than partway through a corpus.
     *
     * @throws IllegalArgumentException when no provider is registered under the
     *         spec's model id
     * @throws IllegalStateException when the provider's dimension is not the
     *         spec's dims
     */
    public static EmbeddingProvider forSpec(ChunkingPolicy.EmbeddingSpec spec) {
        Objects.requireNonNull(spec, "spec");
        EmbeddingProvider provider = byId(spec.model());
        if (provider.dimension() != spec.dims()) {
            throw new IllegalStateException("Embedding provider '" + spec.model()
                    + "' produces " + provider.dimension()
                    + "-dimensional vectors, but the policy pins dims=" + spec.dims());
        }
        return provider;
    }
}

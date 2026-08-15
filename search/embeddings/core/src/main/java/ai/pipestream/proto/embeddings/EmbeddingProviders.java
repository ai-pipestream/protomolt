package ai.pipestream.proto.embeddings;

import ai.pipestream.proto.index.spi.ChunkingPolicy;
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

    /** All discovered providers, keyed by {@link EmbeddingProvider#providerId()}. */
    public static Map<String, EmbeddingProvider> all() {
        Map<String, EmbeddingProvider> providers = new LinkedHashMap<>();
        for (EmbeddingProvider provider : ServiceLoader.load(EmbeddingProvider.class)) {
            providers.put(provider.providerId(), provider);
        }
        return Map.copyOf(providers);
    }

    /**
     * The provider registered under {@code providerId}.
     *
     * @throws IllegalArgumentException when no such provider is on the classpath,
     *         listing the ids that are
     */
    public static EmbeddingProvider byId(String providerId) {
        Map<String, EmbeddingProvider> providers = all();
        EmbeddingProvider provider = providers.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("Unknown embedding provider '" + providerId
                    + "'. Available providers: " + String.join(", ", providers.keySet()));
        }
        return provider;
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

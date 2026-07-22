package ai.pipestream.proto.embeddings;

import java.util.LinkedHashMap;
import java.util.Map;
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
}

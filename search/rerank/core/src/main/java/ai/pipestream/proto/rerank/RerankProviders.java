package ai.pipestream.proto.rerank;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Discovers {@link RerankProvider}s via {@link ServiceLoader}.
 */
public final class RerankProviders {

    private RerankProviders() {
    }

    /** All discovered providers, keyed by {@link RerankProvider#providerId()}. */
    public static Map<String, RerankProvider> all() {
        Map<String, RerankProvider> providers = new LinkedHashMap<>();
        for (RerankProvider provider : ServiceLoader.load(RerankProvider.class)) {
            providers.put(provider.providerId(), provider);
        }
        return Map.copyOf(providers);
    }

    /**
     * The provider registered under {@code providerId}.
     *
     * @throws IllegalArgumentException when no such provider is on the classpath, listing
     *         the ids that are available
     */
    public static RerankProvider byId(String providerId) {
        Map<String, RerankProvider> providers = all();
        RerankProvider provider = providers.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("Unknown rerank provider '" + providerId
                    + "'. Available providers: " + String.join(", ", providers.keySet()));
        }
        return provider;
    }
}

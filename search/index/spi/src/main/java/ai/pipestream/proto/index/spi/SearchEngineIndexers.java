package ai.pipestream.proto.index.spi;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Discovers {@link SearchEngineIndexerProvider}s via {@link ServiceLoader}.
 *
 * <p>Engine ids must be unique across the classpath. A collision throws
 * rather than resolving last-wins: silent replacement would make classpath
 * order decide which engine answers, and the loser would fail only at map
 * time with shapes the caller never asked for.
 */
public final class SearchEngineIndexers {

    private SearchEngineIndexers() {
    }

    public static Map<String, SearchEngineIndexerProvider> loadProviders() {
        return providersFrom(ServiceLoader.load(SearchEngineIndexerProvider.class));
    }

    /** ServiceLoader-free seam for {@link #loadProviders()}; throws on engine-id collisions. */
    static Map<String, SearchEngineIndexerProvider> providersFrom(
            Iterable<SearchEngineIndexerProvider> discovered) {
        Map<String, SearchEngineIndexerProvider> providers = new LinkedHashMap<>();
        for (SearchEngineIndexerProvider provider : discovered) {
            SearchEngineIndexerProvider previous = providers.putIfAbsent(provider.engineId(), provider);
            if (previous != null) {
                throw new IllegalStateException(
                        "Duplicate search engine id '" + provider.engineId() + "': "
                                + previous.getClass().getName() + " and "
                                + provider.getClass().getName()
                                + " both claim it; engine ids must be unique across the classpath");
            }
        }
        return Map.copyOf(providers);
    }

    public static Map<String, SearchEngineIndexer> createAll(IndexerContext context) {
        Map<String, SearchEngineIndexer> indexers = new LinkedHashMap<>();
        loadProviders().forEach((id, provider) -> indexers.put(id, provider.create(context)));
        return Map.copyOf(indexers);
    }

    public static Optional<SearchEngineIndexer> create(String engineId, IndexerContext context) {
        return Optional.ofNullable(loadProviders().get(engineId)).map(p -> p.create(context));
    }
}

package ai.pipestream.proto.index.spi;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Discovers {@link SearchEngineIndexerProvider}s via {@link ServiceLoader}.
 */
public final class SearchEngineIndexers {

    private SearchEngineIndexers() {
    }

    public static Map<String, SearchEngineIndexerProvider> loadProviders() {
        Map<String, SearchEngineIndexerProvider> providers = new LinkedHashMap<>();
        for (SearchEngineIndexerProvider provider : ServiceLoader.load(SearchEngineIndexerProvider.class)) {
            providers.put(provider.engineId(), provider);
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

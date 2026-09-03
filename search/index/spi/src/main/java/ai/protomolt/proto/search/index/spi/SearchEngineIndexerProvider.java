package ai.protomolt.proto.search.index.spi;

/** ServiceLoader factory for {@link SearchEngineIndexer}. */
public interface SearchEngineIndexerProvider {
    String engineId();

    SearchEngineIndexer create(IndexerContext context);
}

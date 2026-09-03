package ai.protomolt.proto.search.index.opensearch;

import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexer;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link OpenSearchDocumentMapper}. */
public final class OpenSearchIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return OpenSearchDocumentMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new OpenSearchDocumentMapper(context);
    }
}

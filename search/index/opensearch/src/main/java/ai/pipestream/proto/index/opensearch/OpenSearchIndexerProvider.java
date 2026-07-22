package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.index.spi.IndexerContext;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link OpenSearchDocumentMapper}. */
public final class OpenSearchIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return OpenSearchDocumentMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new OpenSearchDocumentMapper(context.fieldMapper());
    }
}

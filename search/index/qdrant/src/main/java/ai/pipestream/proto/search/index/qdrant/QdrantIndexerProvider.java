package ai.pipestream.proto.search.index.qdrant;

import ai.pipestream.proto.search.index.spi.IndexerContext;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link QdrantPointMapper}. */
public final class QdrantIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return QdrantPointMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new QdrantPointMapper(context);
    }
}

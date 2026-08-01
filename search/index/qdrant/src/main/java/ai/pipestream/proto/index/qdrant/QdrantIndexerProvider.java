package ai.pipestream.proto.index.qdrant;

import ai.pipestream.proto.index.spi.IndexerContext;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link QdrantPointMapper}. */
public final class QdrantIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return QdrantPointMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new QdrantPointMapper(context.fieldMapper());
    }
}

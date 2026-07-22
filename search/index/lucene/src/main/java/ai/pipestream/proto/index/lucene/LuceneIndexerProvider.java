package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.index.spi.IndexerContext;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link ProtoLuceneMapper}. */
public final class LuceneIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return ProtoLuceneMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new ProtoLuceneMapper(context.fieldMapper());
    }
}

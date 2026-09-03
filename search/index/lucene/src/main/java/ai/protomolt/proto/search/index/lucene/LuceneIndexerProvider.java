package ai.protomolt.proto.search.index.lucene;

import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexer;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link ProtoLuceneMapper}. */
public final class LuceneIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return ProtoLuceneMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new ProtoLuceneMapper(context);
    }
}

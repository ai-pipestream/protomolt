package ai.protomolt.proto.search.index.solr;

import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexer;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link SolrDocumentMapper}. */
public final class SolrIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return SolrDocumentMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new SolrDocumentMapper(context);
    }
}

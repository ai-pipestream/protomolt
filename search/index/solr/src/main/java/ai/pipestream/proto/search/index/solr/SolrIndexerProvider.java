package ai.pipestream.proto.search.index.solr;

import ai.pipestream.proto.search.index.spi.IndexerContext;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexerProvider;

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

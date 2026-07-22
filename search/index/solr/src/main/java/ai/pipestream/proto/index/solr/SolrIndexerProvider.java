package ai.pipestream.proto.index.solr;

import ai.pipestream.proto.index.spi.IndexerContext;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.index.spi.SearchEngineIndexerProvider;

/** ServiceLoader provider for {@link SolrDocumentMapper}. */
public final class SolrIndexerProvider implements SearchEngineIndexerProvider {
    @Override
    public String engineId() {
        return SolrDocumentMapper.ENGINE_ID;
    }

    @Override
    public SearchEngineIndexer create(IndexerContext context) {
        return new SolrDocumentMapper(context.fieldMapper());
    }
}

package ai.pipestream.proto.search.index.opensearch;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.search.index.spi.IndexerContext;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexerProvider;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexers;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link OpenSearchIndexerProvider}: the engine id, the indexer it builds from an
 * {@link IndexerContext}, and its ServiceLoader registration under META-INF/services.
 */
class OpenSearchIndexerProviderTest {

    private final OpenSearchIndexerProvider provider = new OpenSearchIndexerProvider();

    @Test
    void exposesTheOpensearchEngineId() {
        assertThat(provider.engineId()).isEqualTo("opensearch");
        assertThat(provider.engineId()).isEqualTo(OpenSearchDocumentMapper.ENGINE_ID);
    }

    @Test
    void createsADocumentMapperFromTheContext() {
        SearchEngineIndexer indexer = provider.create(
                new IndexerContext(new ProtoFieldMapperImpl(new DescriptorRegistry())));

        assertThat(indexer).isInstanceOf(OpenSearchDocumentMapper.class);
        assertThat(indexer.engineId()).isEqualTo("opensearch");
    }

    @Test
    void isDiscoveredThroughTheServiceLoader() {
        var providers = SearchEngineIndexers.loadProviders();

        assertThat(providers).containsKey("opensearch");
        assertThat(providers.get("opensearch")).isInstanceOf(OpenSearchIndexerProvider.class);
    }
}

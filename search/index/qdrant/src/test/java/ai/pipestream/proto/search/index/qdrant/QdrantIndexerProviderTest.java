package ai.pipestream.proto.search.index.qdrant;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.search.index.spi.IndexerContext;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.search.index.spi.SearchEngineIndexerProvider;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

class QdrantIndexerProviderTest {

    @Test
    void providerCreatesTheMapper() {
        QdrantIndexerProvider provider = new QdrantIndexerProvider();
        assertThat(provider.engineId()).isEqualTo("qdrant");

        SearchEngineIndexer indexer = provider.create(
                new IndexerContext(new ProtoFieldMapperImpl(new DescriptorRegistry())));
        assertThat(indexer).isInstanceOf(QdrantPointMapper.class);
        assertThat(indexer.engineId()).isEqualTo("qdrant");
    }

    @Test
    void serviceLoaderDiscoversTheProvider() {
        assertThat(ServiceLoader.load(SearchEngineIndexerProvider.class))
                .anySatisfy(provider -> assertThat(provider.engineId()).isEqualTo("qdrant"));
    }
}

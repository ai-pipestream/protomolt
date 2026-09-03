package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexer;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
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

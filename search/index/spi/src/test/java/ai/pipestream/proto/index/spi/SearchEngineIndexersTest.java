package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ServiceLoader plugin seam. {@code StubIndexerProvider} is registered in this module's
 * test {@code META-INF/services}; the engine modules register themselves the same way.
 */
class SearchEngineIndexersTest {

    private static IndexerContext context() {
        return new IndexerContext(new ProtoFieldMapperImpl(new DescriptorRegistry()));
    }

    @Test
    void loadProvidersDiscoversRegisteredProvidersKeyedByEngineId() {
        Map<String, SearchEngineIndexerProvider> providers = SearchEngineIndexers.loadProviders();

        assertThat(providers).containsKey("stub");
        assertThat(providers.get("stub")).isInstanceOf(StubIndexerProvider.class);
        assertThat(providers.get("stub").engineId()).isEqualTo("stub");
    }

    @Test
    void loadProvidersReturnsAnImmutableMap() {
        Map<String, SearchEngineIndexerProvider> providers = SearchEngineIndexers.loadProviders();

        assertThatThrownBy(() -> providers.put("other", new StubIndexerProvider()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void createAllBuildsOneIndexerPerDiscoveredProvider() {
        Map<String, SearchEngineIndexer> indexers = SearchEngineIndexers.createAll(context());

        assertThat(indexers).containsOnlyKeys("stub");
        assertThat(indexers.get("stub").engineId()).isEqualTo("stub");
        assertThatThrownBy(() -> indexers.remove("stub"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void createReturnsTheIndexerForAKnownEngineIdAndPassesTheContextThrough() {
        IndexerContext context = context();
        Optional<SearchEngineIndexer> indexer = SearchEngineIndexers.create("stub", context);

        assertThat(indexer).get().isInstanceOf(StubIndexerProvider.StubIndexer.class);
        assertThat(((StubIndexerProvider.StubIndexer) indexer.orElseThrow()).context())
                .isSameAs(context);
    }

    /** An absent engine id is an empty Optional, not an exception and not a null indexer. */
    @Test
    void createReturnsEmptyForAnUnknownEngineId() {
        assertThat(SearchEngineIndexers.create("no-such-engine", context())).isEmpty();
    }

    @Test
    void createdIndexerMapsThroughTheSharedPlan() throws Exception {
        SearchEngineIndexer indexer = SearchEngineIndexers.create("stub", context()).orElseThrow();
        IndexingPlan plan = IndexingPlanFactory.inferringOnly().create(Struct.getDescriptor());

        assertThat(indexer.map(StringValue.of("x"), plan))
                .isEqualTo("google.protobuf.Struct/google.protobuf.StringValue");
    }
}

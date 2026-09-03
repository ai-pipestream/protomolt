package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.search.chunk.SentencePackedChunker;
import ai.protomolt.proto.search.embedding.VectorizationPolicy;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import com.google.protobuf.Message;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A chunk lane sends its source field to an embedding provider on every index call, and a
 * vector is not the harmless summary it looks like. So a subject whose lane reads a field
 * the schema classified fails the mount unless the deployment named that class: the
 * refusal lands at boot, beside the lane's other wiring errors, not on the first document.
 */
class SensitiveVectorizationTest {

    @TempDir
    Path work;

    private static ChunkingPolicy policy() {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        12, 0, 2, 30, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        SearchTestProvider.PROVIDER_ID, SearchTestProvider.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    /**
     * A subject chunking {@code body}. {@code bodySensitivity} classifies the chunked
     * field; {@code titleSensitivity} classifies a field that is indexed but never
     * vectorized, which is the case the gate must leave alone.
     */
    private static ServedMapping served(String bodySensitivity, String titleSensitivity) {
        IndexMapping mapping = new IndexMapping("test.Doc", List.of(
                new IndexMapping.IndexedField("id", "id",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).stored(true).build()),
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT), false, titleSensitivity),
                new IndexMapping.IndexedField("body", "body",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT), false, bodySensitivity)));
        return new ServedMapping(mapping, "id",
                message -> "doc-1",
                new ServedMapping.ChunkLane(policy(), "body", SensitiveVectorizationTest::body));
    }

    private static String body(Message message) {
        return "some text to chunk";
    }

    private void mount(ServedMapping served, VectorizationPolicy vectorization) {
        try (LuceneSearchStore store = new LuceneSearchStore(
                work, Map.of("docs", served), null, false, vectorization)) {
            // Opening the store is the assertion; a mount that survives serves.
        }
    }

    @Test
    void aClassifiedChunkSourceFailsTheMountByName() {
        assertThatThrownBy(() ->
                mount(served("pii", ""), VectorizationPolicy.unclassifiedOnly()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("docs")
                .hasMessageContaining("body")
                .hasMessageContaining("pii");
    }

    @Test
    void namingTheClassMountsIt() {
        assertThatCode(() ->
                mount(served("pii", ""), VectorizationPolicy.permitting(Set.of("pii"))))
                .doesNotThrowAnyException();
    }

    @Test
    void namingADifferentClassStillRefuses() {
        assertThatThrownBy(() ->
                mount(served("pii", ""), VectorizationPolicy.permitting(Set.of("internal"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pii");
    }

    @Test
    void unrestrictedMountsAnyClass() {
        assertThatCode(() -> mount(served("secret", ""), VectorizationPolicy.unrestricted()))
                .doesNotThrowAnyException();
    }

    @Test
    void anUnclassifiedLaneMountsAsItAlwaysHas() {
        assertThatCode(() -> mount(served("", ""), VectorizationPolicy.unclassifiedOnly()))
                .doesNotThrowAnyException();
    }

    @Test
    void aClassifiedFieldOffTheLaneIsNotTheGatesBusiness() {
        // 'title' is classified and indexed, but nothing vectorizes it. Indexing
        // restricted content is the masking layer's decision; this gate is only about
        // what leaves for an embedding provider, so the mount must survive.
        assertThatCode(() ->
                mount(served("", "pii"), VectorizationPolicy.unclassifiedOnly()))
                .doesNotThrowAnyException();
    }

    @Test
    void aSubjectWithNoChunkLaneIsUnaffected() {
        IndexMapping mapping = new IndexMapping("test.Doc", List.of(
                new IndexMapping.IndexedField("id", "id",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).stored(true).build()),
                new IndexMapping.IndexedField("body", "body",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT), false, "pii")));
        ServedMapping laneless = new ServedMapping(mapping, "id", message -> "doc-1", null);

        assertThatCode(() -> mount(laneless, VectorizationPolicy.unclassifiedOnly()))
                .doesNotThrowAnyException();
    }
}

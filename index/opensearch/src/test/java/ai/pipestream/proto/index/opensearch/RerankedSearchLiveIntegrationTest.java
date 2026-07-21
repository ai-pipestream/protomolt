package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.PlanEmbedder;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.rerank.RerankProvider;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.testcontainers.OpenSearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link RerankedSemanticSearch} end to end against a real engine with fixture providers, so
 * the lane needs Docker and nothing else: sentences flow through
 * {@link OpenSearchDocumentMapper}, get vectors from a fixed-table 4-dimensional
 * {@link EmbeddingProvider} via {@link PlanEmbedder} (the vectors form dog / cat / car
 * neighborhoods, so the kNN order is known), land through {@link OpenSearchSink}, and a
 * keyword-table {@link RerankProvider} reorders the recalled candidates. The suite skips when
 * Docker is unavailable.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RerankedSearchLiveIntegrationTest {

    // Latest OpenSearch line. The container defaults to DISABLE_SECURITY_PLUGIN=true,
    // so the suite talks plain HTTP with no auth.
    @Container
    static final OpenSearchContainer<?> OPENSEARCH = new OpenSearchContainer<>(
            DockerImageName.parse("opensearchproject/opensearch:3.7.0"));

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    // Query "a young dog" sits next to the dog cluster, so kNN recalls dog, puppy, kitten, cat
    // in that order. The keyword reranker scores puppy above dog, flipping the top two.
    private static final String QUERY = "a young dog";
    private static final Map<String, String> SENTENCES = new LinkedHashMap<>();
    private static final Map<String, float[]> VECTORS = new LinkedHashMap<>();

    static {
        SENTENCES.put("dog", "the dog sat on the mat");
        SENTENCES.put("puppy", "the puppy played in the garden");
        SENTENCES.put("cat", "the cat slept on the windowsill");
        SENTENCES.put("kitten", "the kitten chased a toy mouse");
        SENTENCES.put("car", "the car drove down the highway");
        SENTENCES.put("engine", "the engine roared to life");
        VECTORS.put(QUERY, new float[]{0.98f, 0.08f, 0f, 0f});
        VECTORS.put("the dog sat on the mat", new float[]{1.0f, 0.1f, 0f, 0f});
        VECTORS.put("the puppy played in the garden", new float[]{0.95f, 0.15f, 0f, 0f});
        VECTORS.put("the cat slept on the windowsill", new float[]{0.1f, 1.0f, 0f, 0f});
        VECTORS.put("the kitten chased a toy mouse", new float[]{0.15f, 0.95f, 0.05f, 0f});
        VECTORS.put("the car drove down the highway", new float[]{0f, 0.05f, 1.0f, 0.1f});
        VECTORS.put("the engine roared to life", new float[]{0.05f, 0f, 0.95f, 0.15f});
    }

    private static String base;
    private static String index;
    private static OpenSearchSink sink;
    private static Descriptor descriptor;
    private static IndexingPlan plan;

    @BeforeAll
    static void setUp() {
        base = OPENSEARCH.getHttpHostAddress();
        index = "reranked-" + UUID.randomUUID().toString().substring(0, 12);
        sink = new OpenSearchSink(base, HTTP);
        descriptor = sentenceDescriptor();
        plan = new IndexingPlan(descriptor.getFullName(), List.of(
                new IndexingPlan.IndexedField("sentence", "sentence",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                new IndexingPlan.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(4)
                                .build())));
    }

    @AfterAll
    static void deleteIndex() throws Exception {
        if (index != null) {
            HTTP.send(HttpRequest.newBuilder(URI.create(base + "/" + index))
                    .DELETE().build(), HttpResponse.BodyHandlers.discarding());
        }
    }

    @Test
    void rerankHeadReordersTheKnnRecallList() throws Exception {
        assertThat(sink.ensureIndex(index, plan)).isTrue();

        EmbeddingProvider embedder = new FixtureEmbeddingProvider();
        OpenSearchDocumentMapper mapper = new OpenSearchDocumentMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        PlanEmbedder planEmbedder = new PlanEmbedder(embedder, plan);
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : SENTENCES.entrySet()) {
            DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("sentence"), entry.getValue())
                    .build();
            documents.put(entry.getKey(), planEmbedder.embed(mapper.map(message, plan)));
        }
        sink.bulkWrite(index, documents, true);

        OpenSearchSearch search = new OpenSearchSearch(base, HTTP);
        List<Float> queryVector = toList(embedder.embed(QUERY));

        // Raw recall: kNN order is dog first, puppy second.
        List<OpenSearchHit> recall = search.knn(index, "embedding", queryVector, 4);
        assertThat(recall).extracting(OpenSearchHit::id)
                .containsExactly("dog", "puppy", "kitten", "cat");

        // The rerank head flips the top two and truncates to k.
        RerankedSemanticSearch semantic = new RerankedSemanticSearch(
                search, embedder, keywordReranker());
        List<RankedHit> hits = semantic.search(index, "embedding", "sentence", QUERY, 2, 4);

        assertThat(hits).extracting(RankedHit::id).containsExactly("puppy", "dog");
        RankedHit first = hits.get(0);
        assertThat(first.text()).isEqualTo("the puppy played in the garden");
        assertThat(first.relevanceScore()).isEqualTo(0.99);
        assertThat(first.knnScore())
                .as("the kNN score rides along, and recall put puppy second")
                .isGreaterThan(0)
                .isLessThan(hits.get(1).knnScore());
        assertThat(hits.get(1).relevanceScore()).isEqualTo(0.90);
    }

    // ---- fixtures ----

    /** Scores by a hand-set keyword table, so the ranking is deterministic. */
    private static RerankProvider keywordReranker() {
        Map<String, Double> scoresByKeyword = new LinkedHashMap<>();
        scoresByKeyword.put("puppy", 0.99);
        scoresByKeyword.put("dog", 0.90);
        scoresByKeyword.put("kitten", 0.05);
        scoresByKeyword.put("cat", 0.04);
        return new RerankProvider() {
            @Override
            public String providerId() {
                return "keyword-fixture";
            }

            @Override
            public List<Double> score(String query, List<String> texts) {
                List<Double> scores = new ArrayList<>(texts.size());
                for (String text : texts) {
                    double score = scoresByKeyword.entrySet().stream()
                            .filter(entry -> text.contains(entry.getKey()))
                            .mapToDouble(Map.Entry::getValue)
                            .max()
                            .orElse(0.0);
                    scores.add(score);
                }
                return scores;
            }
        };
    }

    /** A fixed-table embedder: known texts map to the cluster vectors above. */
    private static final class FixtureEmbeddingProvider implements EmbeddingProvider {

        @Override
        public String providerId() {
            return "fixed-table-fixture";
        }

        @Override
        public int dimension() {
            return 4;
        }

        @Override
        public float[] embed(String text) {
            float[] vector = VECTORS.get(text);
            if (vector == null) {
                throw new IllegalArgumentException("Fixture has no vector for: " + text);
            }
            return vector;
        }
    }

    private static List<Float> toList(float[] embedding) {
        List<Float> vector = new ArrayList<>(embedding.length);
        for (float component : embedding) {
            vector.add(component);
        }
        return vector;
    }

    private static Descriptor sentenceDescriptor() {
        try {
            FileDescriptorProto file = FileDescriptorProto.newBuilder()
                    .setName("reranked/sentence.proto")
                    .setPackage("reranked")
                    .setSyntax("proto3")
                    .addMessageType(DescriptorProto.newBuilder()
                            .setName("Sentence")
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("sentence").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                            .addField(FieldDescriptorProto.newBuilder()
                                    .setName("embedding").setNumber(2)
                                    .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                    .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)))
                    .build();
            return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                    .findMessageTypeByName("Sentence");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

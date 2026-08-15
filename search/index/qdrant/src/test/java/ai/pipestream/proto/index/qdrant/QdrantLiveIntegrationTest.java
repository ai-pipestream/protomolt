package ai.pipestream.proto.index.qdrant;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.repo.v1.ChunkEmbedding;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import qdrant.Points.PointStruct;
import qdrant.Points.ScoredPoint;
import qdrant.Points.SearchResponse;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The mapper and the gRPC sink against a live Qdrant: the collection created by
 * {@link QdrantSink#ensureCollectionForPoints} must accept the mapped points, a named-vector
 * search must rank the right chunk first, and a second ensure must leave the collection
 * untouched. The engine is a Testcontainers Qdrant matching the vendored protos (v1.18.3);
 * the suite skips when Docker is unavailable.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QdrantLiveIntegrationTest {

    private static final String MODEL = "sentence-transformers/all-MiniLM-L6-v2";
    private static final String VECTOR_NAME = QdrantPointMapper.vectorName(MODEL);

    @Container
    static final GenericContainer<?> QDRANT = new GenericContainer<>(
            DockerImageName.parse("qdrant/qdrant:v1.18.3"))
            .withExposedPorts(6334);

    private static String collection;
    private static ManagedChannel channel;
    private static QdrantSink sink;
    private static IndexMapping mapping;
    private static List<PointStruct> points;

    @BeforeAll
    static void setUp() throws Exception {
        collection = "it-" + UUID.randomUUID().toString().substring(0, 12);
        channel = ManagedChannelBuilder
                .forAddress("localhost", QDRANT.getMappedPort(6334))
                .usePlaintext()
                .build();
        sink = new QdrantSink(channel);

        Document document = Document.newBuilder()
                .setDocId("it-doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Vector Databases")
                        .setSourceUri("s3://bucket/vector-databases.html")
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setResultId("rs-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-apples")
                                        .setChunkNumber(0)
                                        .setText("apples and orchards")
                                        .addEmbeddings(embedding(1f, 0f, 0f, 0f)))
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-oranges")
                                        .setChunkNumber(1)
                                        .setText("oranges and groves")
                                        .addEmbeddings(embedding(0f, 1f, 0f, 0f)))))
                .build();
        IndexMapping indexMapping = new IndexMapping("ai.pipestream.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("search_metadata.title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("search_metadata.source_uri", "source_uri",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                // The declared vector hint drives collection creation: dims enforced on the
                // embeddings, similarity rendered as the collection's distance.
                new IndexMapping.IndexedField(
                        "search_metadata.semantic_results.chunks.embeddings.vector", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(4).build())));
        mapping = indexMapping;
        points = new QdrantPointMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()))
                .map(document, mapping);
        assertThat(points).hasSize(2);
    }

    @AfterAll
    static void shutdown() {
        if (sink != null) {
            sink.close(); // caller-owned channel: closed explicitly below
        }
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    private static ChunkEmbedding embedding(float... vector) {
        ChunkEmbedding.Builder builder = ChunkEmbedding.newBuilder()
                .setEmbeddingId("e-" + UUID.randomUUID().toString().substring(0, 8))
                .setModel(MODEL)
                .setDimensions(vector.length);
        for (float component : vector) {
            builder.addVector(component);
        }
        return builder.build();
    }

    @Test
    @Order(1)
    void ensureCollectionCreatesFromThePoints() {
        assertThat(sink.ensureCollectionForPoints(collection, points, mapping)).isTrue();
    }

    @Test
    @Order(2)
    void upsertedPointsAnswerNamedVectorSearch() {
        sink.upsert(collection, points);

        SearchResponse response = sink.search(
                collection, VECTOR_NAME, List.of(0.98f, 0.02f, 0f, 0f), 2);

        assertThat(response.getResultList()).hasSize(2);
        ScoredPoint top = response.getResult(0);
        assertThat(top.getId().getUuid())
                .isEqualTo(QdrantPointMapper.pointUuid("it-doc-1", "rs-1", "c-apples").toString());
        Map<String, qdrant.JsonWithInt.Value> payload = top.getPayloadMap();
        assertThat(payload.get("chunk_text").getStringValue()).isEqualTo("apples and orchards");
        assertThat(payload.get("title").getStringValue()).isEqualTo("Vector Databases");
        assertThat(payload.get("doc_id").getStringValue()).isEqualTo("it-doc-1");
    }

    @Test
    @Order(3)
    void reindexingUpsertsTheSameDeterministicPoints() {
        // Mapping again yields identical ids, so a second upsert keeps the count at two.
        sink.upsert(collection, points);

        SearchResponse response = sink.search(
                collection, VECTOR_NAME, List.of(0f, 0f, 0f, 1f), 10);
        assertThat(response.getResultList()).hasSize(2);
    }

    @Test
    @Order(4)
    void ensureCollectionLeavesAnExistingCollectionUntouched() {
        assertThat(sink.ensureCollection(collection, Map.of(VECTOR_NAME, 4))).isFalse();
    }
}

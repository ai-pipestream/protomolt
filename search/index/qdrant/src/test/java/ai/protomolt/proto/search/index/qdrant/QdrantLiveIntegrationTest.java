package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
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
 * Docker is required.
 */
@Tag("integration")
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QdrantLiveIntegrationTest {

    private static final String MODEL = "sentence-transformers/all-MiniLM-L6-v2";
    private static final String VECTOR_NAME = QdrantPointMapper.vectorName(MODEL);
    private static final String DIGEST =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

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
                        .addSegmentations(Segmentation.newBuilder()
                                .setSegmentationId("rs-1")
                                .setContentDigest(DIGEST)
                                .setKind(SegmentationKind.SEGMENTATION_KIND_PARAGRAPH)
                                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                                .setSource(TextSource.newBuilder()
                                        .setFieldPath("search_metadata.body")
                                        .setBinding(SourceTextBinding.newBuilder()
                                                .setResolvedTextSha256(DIGEST)
                                                .setUtf8Length(38)))
                                .addSegments(Segment.newBuilder()
                                        .setSegmentId("c-apples")
                                        .setOrdinal(0)
                                        .setText("apples and orchards")
                                        .setStart(0)
                                        .setEnd(19))
                                .addSegments(Segment.newBuilder()
                                        .setSegmentId("c-oranges")
                                        .setOrdinal(1)
                                        .setText("oranges and groves")
                                        .setStart(20)
                                        .setEnd(38)))
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-1")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(DIGEST)
                                .setModel(MODEL)
                                .setDimensions(4)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(segmentVector("c-apples", 1f, 0f, 0f, 0f))
                                .addVectors(segmentVector("c-oranges", 0f, 1f, 0f, 0f))))
                .build();
        IndexMapping indexMapping = new IndexMapping("ai.protomolt.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("search_metadata.title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("search_metadata.source_uri", "source_uri",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                // The declared vector hint drives collection creation: dims enforced on the
                // embeddings, similarity rendered as the collection's distance.
                new IndexMapping.IndexedField(
                        "search_metadata.embedding_sets.vectors.values", "embedding",
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

    private static SegmentVector segmentVector(String segmentId, float... vector) {
        SegmentVector.Builder builder = SegmentVector.newBuilder()
                .setSegmentId(segmentId);
        for (float component : vector) {
            builder.addValues(component);
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

package ai.pipestream.proto.index.qdrant;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.repo.v1.ChunkEmbedding;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import qdrant.Common.PointId;
import qdrant.JsonWithInt.Value;
import qdrant.Points.PointStruct;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QdrantPointMapperTest {

    private static final String MINILM = "sentence-transformers/all-MiniLM-L6-v2";
    private static final String MINILM_VECTOR = "sentence-transformers_all-MiniLM-L6-v2";

    private final QdrantPointMapper mapper =
            new QdrantPointMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static IndexMapping mapping() {
        return new IndexMapping("ai.pipestream.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("search_metadata.title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("search_metadata.source_uri", "source_uri",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                // Unset on every fixture: proves missing values are skipped, not nulls.
                new IndexMapping.IndexedField("search_metadata.body", "body",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                // Chunk-level data must not be flattened onto every point's payload.
                new IndexMapping.IndexedField("search_metadata.semantic_results.chunks.text",
                        "flattened_chunk_text", ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                // Vector hints are excluded: vectors come from the embeddings.
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(2).build())));
    }

    private static ChunkEmbedding embedding(String model, float... vector) {
        ChunkEmbedding.Builder builder = ChunkEmbedding.newBuilder()
                .setEmbeddingId(UUID.randomUUID().toString())
                .setModel(model)
                .setDimensions(vector.length);
        for (float component : vector) {
            builder.addVector(component);
        }
        return builder.build();
    }

    private static Document document() {
        return Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Qdrant in Practice")
                        .setSourceUri("s3://bucket/qdrant.html")
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setResultId("rs-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-1")
                                        .setChunkNumber(0)
                                        .setText("the first chunk")
                                        .addEmbeddings(embedding(MINILM, 0.1f, 0.2f))
                                        .addEmbeddings(embedding("e5_small.onnx", 0.9f, 0.8f)))
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-2")
                                        .setChunkNumber(1)
                                        .setText("the second chunk")
                                        .addEmbeddings(embedding(MINILM, 0.3f, 0.4f)))
                                // No embeddings: the chunk must not produce a point.
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-3")
                                        .setChunkNumber(2)
                                        .setText("the unembedded chunk"))))
                .build();
    }

    @Test
    void mapsOnePointPerEmbeddedChunk() throws Exception {
        List<PointStruct> points = mapper.map(document(), mapping());

        assertThat(points).hasSize(2);

        PointStruct first = points.get(0);
        assertThat(first.getId()).isEqualTo(PointId.newBuilder()
                .setUuid(QdrantPointMapper.pointUuid("doc-1", "rs-1", "c-1").toString())
                .build());

        // Both models ride the same point as named vectors, sanitized.
        Map<String, qdrant.Points.Vector> vectors =
                first.getVectors().getVectors().getVectorsMap();
        assertThat(vectors).containsOnlyKeys(MINILM_VECTOR, "e5_small_onnx");
        assertThat(vectors.get(MINILM_VECTOR).getDense().getDataList())
                .containsExactly(0.1f, 0.2f);

        Map<String, Value> payload = first.getPayloadMap();
        assertThat(payload.get("title").getStringValue()).isEqualTo("Qdrant in Practice");
        assertThat(payload.get("source_uri").getStringValue()).isEqualTo("s3://bucket/qdrant.html");
        assertThat(payload.get("doc_id").getStringValue()).isEqualTo("doc-1");
        assertThat(payload.get("result_id").getStringValue()).isEqualTo("rs-1");
        assertThat(payload.get("chunk_id").getStringValue()).isEqualTo("c-1");
        assertThat(payload.get("chunk_number").getIntegerValue()).isZero();
        assertThat(payload.get("chunk_text").getStringValue()).isEqualTo("the first chunk");
        assertThat(payload.get("models").getListValue().getValuesList())
                .extracting(Value::getStringValue)
                .containsExactly(MINILM_VECTOR, "e5_small_onnx");
        assertThat(payload).doesNotContainKeys("body", "flattened_chunk_text", "embedding");

        // The second point carries only the one model it was embedded with.
        assertThat(points.get(1).getVectors().getVectors().getVectorsMap())
                .containsOnlyKeys(MINILM_VECTOR);
        assertThat(points.get(1).getPayloadMap().get("chunk_text").getStringValue())
                .isEqualTo("the second chunk");
    }

    @Test
    void expandsRegistryKnownStructuredDataIntoEveryPointPayload() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(ChunkEmbedding.getDescriptor());
        QdrantPointMapper anyMapper = new QdrantPointMapper(new ProtoFieldMapperImpl(registry));
        Document document = document().toBuilder()
                .setStructuredData(Any.pack(ChunkEmbedding.newBuilder()
                        .setEmbeddingId("emb-9")
                        .setModel("m1")
                        .setDimensions(2)
                        .build()))
                .build();
        List<IndexMapping.IndexedField> fields = new ArrayList<>(mapping().fields());
        fields.add(new IndexMapping.IndexedField("structured_data", "structured_data",
                ResolvedFieldHint.of(IndexFieldKind.ANY)));
        IndexMapping mappingWithAny = new IndexMapping("ai.pipestream.proto.repo.v1.Document", fields);

        List<PointStruct> points = anyMapper.map(document, mappingWithAny);

        assertThat(points).hasSize(2);
        Map<String, Value> payload = points.get(0).getPayloadMap();
        assertThat(payload.get("structured_data_model").getStringValue()).isEqualTo("m1");
        assertThat(payload.get("structured_data_dimensions").getIntegerValue()).isEqualTo(2);
        assertThat(payload).doesNotContainKey("structured_data");
    }

    @Test
    void unknownStructuredDataTypeUrlFailsInsteadOfUpserting() throws Exception {
        Document document = document().toBuilder()
                .setStructuredData(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.test.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();
        List<IndexMapping.IndexedField> fields = new ArrayList<>(mapping().fields());
        fields.add(new IndexMapping.IndexedField("structured_data", "structured_data",
                ResolvedFieldHint.of(IndexFieldKind.ANY)));
        IndexMapping mappingWithAny = new IndexMapping("ai.pipestream.proto.repo.v1.Document", fields);

        assertThatThrownBy(() -> mapper.map(document, mappingWithAny))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("structured_data")
                .hasMessageContaining("type.googleapis.com/ai.pipestream.test.MissingType");
    }

    @Test
    void pointIdsAreDeterministic() throws Exception {
        assertThat(mapper.map(document(), mapping()))
                .isEqualTo(mapper.map(document(), mapping()));
        assertThat(QdrantPointMapper.pointUuid("doc-1", "rs-1", "c-1"))
                .isEqualTo(UUID.nameUUIDFromBytes("qdrant|doc-1|rs-1|c-1"
                        .getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void documentWithoutSemanticResultsMapsToNoPoints() throws Exception {
        Document bare = Document.newBuilder()
                .setDocId("doc-2")
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("No chunks"))
                .build();
        assertThat(mapper.map(bare, mapping())).isEmpty();
    }

    @Test
    void nonDocumentMessagesAreRejected() {
        Timestamp notADocument = Timestamp.newBuilder().setSeconds(1).build();
        assertThatThrownBy(() -> mapper.map(notADocument, mapping()))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("ai.pipestream.proto.repo.v1.Document")
                .hasMessageContaining("google.protobuf.Timestamp");
    }

    @Test
    void vectorDimensionsComeFromThePoints() throws Exception {
        List<PointStruct> points = mapper.map(document(), mapping());
        assertThat(QdrantPointMapper.vectorDimensions(points))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        MINILM_VECTOR, 2,
                        "e5_small_onnx", 2));
        assertThat(QdrantPointMapper.vectorDimensions(List.of())).isEmpty();
    }

    @Test
    void conflictingVectorDimensionsAreRejected() throws Exception {
        List<PointStruct> points = mapper.map(document(), mapping());
        PointStruct conflicting = points.get(0).toBuilder()
                .setVectors(points.get(0).getVectors().toBuilder()
                        .setVectors(points.get(0).getVectors().getVectors().toBuilder()
                                .putVectors(MINILM_VECTOR, qdrant.Points.Vector.newBuilder()
                                        .setDense(qdrant.Points.DenseVector.newBuilder()
                                                .addData(1f).addData(2f).addData(3f))
                                        .build())))
                .build();
        assertThatThrownBy(() -> QdrantPointMapper.vectorDimensions(
                List.of(points.get(0), conflicting)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(MINILM_VECTOR)
                .hasMessageContaining("2 and 3");
    }

    @Test
    void vectorNamesAreSanitizedModelIds() {
        assertThat(QdrantPointMapper.vectorName(MINILM)).isEqualTo(MINILM_VECTOR);
        assertThat(QdrantPointMapper.vectorName("plain")).isEqualTo("plain");
        assertThat(QdrantPointMapper.vectorName("a/b.c-d_e f")).isEqualTo("a_b_c-d_e_f");
    }

    // ---------------------------------------------------------------- vector hints

    private static IndexMapping mappingWithVectorHint(String path, int dims,
                                                   ai.pipestream.proto.index.spi.VectorSimilarity similarity) {
        return new IndexMapping("ai.pipestream.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("search_metadata.title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField(path, "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(dims)
                                .vectorSimilarity(similarity)
                                .build())));
    }

    @Test
    void declaredVectorDimsAreEnforcedOnWrite() {
        IndexMapping strict = mappingWithVectorHint(
                "search_metadata.semantic_results.chunks.embeddings.vector", 3,
                ai.pipestream.proto.index.spi.VectorSimilarity.COSINE);

        assertThatThrownBy(() -> mapper.map(document(), strict))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("has 2 dimensions")
                .hasMessageContaining("declares 3");
    }

    @Test
    void vectorSpecsTakeSizeAndDistanceFromTheHint() throws Exception {
        List<PointStruct> points = mapper.map(document(), mappingWithVectorHint(
                "search_metadata.semantic_results.chunks.embeddings.vector", 2,
                ai.pipestream.proto.index.spi.VectorSimilarity.L2));

        assertThat(QdrantPointMapper.vectorSpecs(points, mappingWithVectorHint(
                        "search_metadata.semantic_results.chunks.embeddings.vector", 2,
                        ai.pipestream.proto.index.spi.VectorSimilarity.L2)))
                .containsExactlyInAnyOrder(
                        new QdrantVectorSpec(MINILM_VECTOR, 2, qdrant.Collections.Distance.Euclid),
                        new QdrantVectorSpec("e5_small_onnx", 2, qdrant.Collections.Distance.Euclid));
    }

    @Test
    void vectorSpecsFallBackToDataAndCosineWithoutAHint() throws Exception {
        List<PointStruct> points = mapper.map(document(), mapping());

        // mapping() declares its VECTOR field with dims 2 and default (COSINE) similarity.
        assertThat(QdrantPointMapper.vectorSpecs(points, mapping()))
                .containsExactlyInAnyOrder(
                        QdrantVectorSpec.cosine(MINILM_VECTOR, 2),
                        QdrantVectorSpec.cosine("e5_small_onnx", 2));
        // No mapping at all: pure data inference.
        assertThat(QdrantPointMapper.vectorSpecs(points, null))
                .containsExactlyInAnyOrder(
                        QdrantVectorSpec.cosine(MINILM_VECTOR, 2),
                        QdrantVectorSpec.cosine("e5_small_onnx", 2));
    }

    @Test
    void vectorHintPrefersTheSemanticResultsPath() {
        IndexMapping twoVectors = new IndexMapping("ai.pipestream.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("other_embedding", "other",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(8)
                                .vectorSimilarity(ai.pipestream.proto.index.spi.VectorSimilarity.L2)
                                .build()),
                new IndexMapping.IndexedField(
                        "search_metadata.semantic_results.chunks.embeddings.vector", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(2)
                                .vectorSimilarity(ai.pipestream.proto.index.spi.VectorSimilarity.DOT_PRODUCT)
                                .build())));

        // Two VECTOR fields would be ambiguous; the semantic-results one wins.
        assertThat(QdrantPointMapper.vectorHint(twoVectors)).isPresent()
                .get().extracting(hint -> hint.vectorSimilarity())
                .isEqualTo(ai.pipestream.proto.index.spi.VectorSimilarity.DOT_PRODUCT);
    }

    @Test
    void hintSimilaritiesMapToQdrantDistances() {
        assertThat(QdrantPointMapper.distance(ai.pipestream.proto.index.spi.VectorSimilarity.COSINE))
                .isEqualTo(qdrant.Collections.Distance.Cosine);
        assertThat(QdrantPointMapper.distance(ai.pipestream.proto.index.spi.VectorSimilarity.L2))
                .isEqualTo(qdrant.Collections.Distance.Euclid);
        assertThat(QdrantPointMapper.distance(ai.pipestream.proto.index.spi.VectorSimilarity.DOT_PRODUCT))
                .isEqualTo(qdrant.Collections.Distance.Dot);
        assertThat(QdrantPointMapper.distance(ai.pipestream.proto.index.spi.VectorSimilarity.MAX_INNER_PRODUCT))
                .isEqualTo(qdrant.Collections.Distance.Dot);
    }

    @Test
    void payloadValueConversion() {
        assertThat(QdrantPointMapper.toValue("s").getStringValue()).isEqualTo("s");
        assertThat(QdrantPointMapper.toValue(7L).getIntegerValue()).isEqualTo(7);
        assertThat(QdrantPointMapper.toValue(3).getIntegerValue()).isEqualTo(3);
        assertThat(QdrantPointMapper.toValue(0.5d).getDoubleValue()).isEqualTo(0.5d);
        assertThat(QdrantPointMapper.toValue(0.5f).getDoubleValue()).isEqualTo(0.5d);
        assertThat(QdrantPointMapper.toValue(true).getBoolValue()).isTrue();
        assertThat(QdrantPointMapper.toValue(List.of("a", 1L)).getListValue().getValuesList())
                .hasSize(2);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(0).build();
        assertThat(QdrantPointMapper.toValue(timestamp).getStringValue())
                .isEqualTo("1970-01-01T00:00:00Z");
        // No flat payload representation.
        assertThat(QdrantPointMapper.toValue(SearchMetadata.getDefaultInstance())).isNull();
    }
}

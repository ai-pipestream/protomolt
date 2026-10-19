package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.MappingException;
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
    private static final String DIGEST =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private final QdrantPointMapper mapper =
            new QdrantPointMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    private static IndexMapping mapping() {
        return new IndexMapping("ai.protomolt.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("search_metadata.title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("search_metadata.source_uri", "source_uri",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                // Unset on every fixture: proves missing values are skipped, not nulls.
                new IndexMapping.IndexedField("search_metadata.body", "body",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                // Segment-level data must not be flattened onto every point's payload.
                new IndexMapping.IndexedField("search_metadata.segmentations.segments.text",
                        "flattened_chunk_text", ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                new IndexMapping.IndexedField("search_metadata.embedding_sets.model",
                        "flattened_model", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("search_metadata.segmentations.segments.text",
                        "selected_segment_text", ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                // Vector hints are excluded: vectors come from the embeddings.
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(2).build())));
    }

    private static SegmentVector segmentVector(String segmentId, float... vector) {
        SegmentVector.Builder builder = SegmentVector.newBuilder()
                .setSegmentId(segmentId);
        for (float component : vector) {
            builder.addValues(component);
        }
        return builder.build();
    }

    private static Document document() {
        return document(Similarity.SIMILARITY_COSINE);
    }

    private static Document document(Similarity similarity) {
        Segmentation segmentation = Segmentation.newBuilder()
                .setSegmentationId("rs-1")
                .setContentDigest(DIGEST)
                .setKind(SegmentationKind.SEGMENTATION_KIND_PARAGRAPH)
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .setSource(TextSource.newBuilder()
                        .setFieldPath("search_metadata.body")
                        .setBinding(SourceTextBinding.newBuilder()
                                .setResolvedTextSha256(DIGEST)
                                .setUtf8Length(53)))
                .addSegments(Segment.newBuilder()
                        .setSegmentId("c-1")
                        .setOrdinal(0)
                        .setText("the first chunk")
                        .setStart(0)
                        .setEnd(15))
                .addSegments(Segment.newBuilder()
                        .setSegmentId("c-2")
                        .setOrdinal(1)
                        .setText("the second chunk")
                        .setStart(16)
                        .setEnd(32))
                // No embeddings: the segment must not produce a point.
                .addSegments(Segment.newBuilder()
                        .setSegmentId("c-3")
                        .setOrdinal(2)
                        .setText("the unembedded chunk")
                        .setStart(33)
                        .setEnd(53))
                .build();
        EmbeddingSet minilmSet = EmbeddingSet.newBuilder()
                .setEmbeddingSetId("es-minilm")
                .setSegmentationId("rs-1")
                .setSegmentationDigest(DIGEST)
                .setModel(MINILM)
                .setDimensions(2)
                .setSimilarity(similarity)
                .addVectors(segmentVector("c-1", 0.1f, 0.2f))
                .addVectors(segmentVector("c-2", 0.3f, 0.4f))
                .build();
        EmbeddingSet e5Set = EmbeddingSet.newBuilder()
                .setEmbeddingSetId("es-e5")
                .setSegmentationId("rs-1")
                .setSegmentationDigest(DIGEST)
                .setModel("e5_small.onnx")
                .setDimensions(2)
                .setSimilarity(similarity)
                .addVectors(segmentVector("c-1", 0.9f, 0.8f))
                .build();
        return Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Qdrant in Practice")
                        .setSourceUri("s3://bucket/qdrant.html")
                        .addSegmentations(segmentation)
                        .addEmbeddingSets(minilmSet)
                        .addEmbeddingSets(e5Set))
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
        assertThat(payload.get("segmentation_id").getStringValue()).isEqualTo("rs-1");
        assertThat(payload.get("chunk_id").getStringValue()).isEqualTo("c-1");
        assertThat(payload.get("segment_id").getStringValue()).isEqualTo("c-1");
        assertThat(payload.get("chunk_number").getIntegerValue()).isZero();
        assertThat(payload.get("chunk_text").getStringValue()).isEqualTo("the first chunk");
        assertThat(payload.get("models").getListValue().getValuesList())
                .extracting(Value::getStringValue)
                .containsExactly(MINILM_VECTOR, "e5_small_onnx");
        assertThat(payload).doesNotContainKeys(
                "body", "flattened_chunk_text", "flattened_model",
                "selected_segment_text", "embedding");

        // The second point carries only the one model it was embedded with.
        assertThat(points.get(1).getVectors().getVectors().getVectorsMap())
                .containsOnlyKeys(MINILM_VECTOR);
        assertThat(points.get(1).getPayloadMap().get("chunk_text").getStringValue())
                .isEqualTo("the second chunk");
    }

    @Test
    void expandsRegistryKnownStructuredDataIntoEveryPointPayload() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.register(Segment.getDescriptor());
        QdrantPointMapper anyMapper = new QdrantPointMapper(new ProtoFieldMapperImpl(registry));
        Document document = document().toBuilder()
                .setStructuredData(Any.pack(Segment.newBuilder()
                        .setSegmentId("seg-9")
                        .setOrdinal(2)
                        .setText("hello")
                        .setStart(0)
                        .setEnd(5)
                        .build()))
                .build();
        List<IndexMapping.IndexedField> fields = new ArrayList<>(mapping().fields());
        fields.add(new IndexMapping.IndexedField("structured_data", "structured_data",
                ResolvedFieldHint.of(IndexFieldKind.ANY)));
        IndexMapping mappingWithAny = new IndexMapping("ai.protomolt.proto.repo.v1.Document", fields);

        List<PointStruct> points = anyMapper.map(document, mappingWithAny);

        assertThat(points).hasSize(2);
        Map<String, Value> payload = points.get(0).getPayloadMap();
        assertThat(payload.get("structured_data_segment_id").getStringValue()).isEqualTo("seg-9");
        assertThat(payload.get("structured_data_ordinal").getIntegerValue()).isEqualTo(2);
        assertThat(payload).doesNotContainKey("structured_data");
    }

    @Test
    void unknownStructuredDataTypeUrlFailsInsteadOfUpserting() throws Exception {
        Document document = document().toBuilder()
                .setStructuredData(Any.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.protomolt.test.MissingType")
                        .setValue(ByteString.copyFromUtf8("x"))
                        .build())
                .build();
        List<IndexMapping.IndexedField> fields = new ArrayList<>(mapping().fields());
        fields.add(new IndexMapping.IndexedField("structured_data", "structured_data",
                ResolvedFieldHint.of(IndexFieldKind.ANY)));
        IndexMapping mappingWithAny = new IndexMapping("ai.protomolt.proto.repo.v1.Document", fields);

        assertThatThrownBy(() -> mapper.map(document, mappingWithAny))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("structured_data")
                .hasMessageContaining("type.googleapis.com/ai.protomolt.test.MissingType");
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
                .hasMessageContaining("ai.protomolt.proto.repo.v1.Document")
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

    private static IndexMapping mappingWithoutVectorHint() {
        return new IndexMapping("ai.protomolt.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("search_metadata.title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));
    }

    private static IndexMapping mappingWithVectorHint(String path, int dims,
                                                   ai.protomolt.proto.search.index.spi.VectorSimilarity similarity) {
        return new IndexMapping("ai.protomolt.proto.repo.v1.Document", List.of(
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
                "search_metadata.embedding_sets.vectors.values", 3,
                ai.protomolt.proto.search.index.spi.VectorSimilarity.COSINE);

        assertThatThrownBy(() -> mapper.map(document(), strict))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("has 2 dimensions")
                .hasMessageContaining("declares 3");
    }

    @Test
    void vectorSpecsTakeSizeAndDistanceFromTheHint() throws Exception {
        Document euclideanDoc = document(Similarity.SIMILARITY_EUCLIDEAN);
        IndexMapping l2Mapping = mappingWithVectorHint(
                "search_metadata.embedding_sets.vectors.values", 2,
                ai.protomolt.proto.search.index.spi.VectorSimilarity.L2);
        List<PointStruct> points = mapper.map(euclideanDoc, l2Mapping);

        assertThat(QdrantPointMapper.vectorSpecs(points, l2Mapping))
                .containsExactlyInAnyOrder(
                        new QdrantVectorSpec(MINILM_VECTOR, 2, qdrant.Collections.Distance.Euclid),
                        new QdrantVectorSpec("e5_small_onnx", 2, qdrant.Collections.Distance.Euclid));
        assertThat(QdrantPointMapper.vectorSpecs(euclideanDoc, l2Mapping))
                .containsExactlyInAnyOrder(
                        new QdrantVectorSpec(MINILM_VECTOR, 2, qdrant.Collections.Distance.Euclid),
                        new QdrantVectorSpec("e5_small_onnx", 2, qdrant.Collections.Distance.Euclid));
    }

    @Test
    void projectedPointsRequireExplicitSimilarity() throws Exception {
        List<PointStruct> points = mapper.map(document(), mapping());

        // mapping() declares its VECTOR field with dims 2 and default (COSINE) similarity.
        assertThat(QdrantPointMapper.vectorSpecs(points, mapping()))
                .containsExactlyInAnyOrder(
                        QdrantVectorSpec.cosine(MINILM_VECTOR, 2),
                        QdrantVectorSpec.cosine("e5_small_onnx", 2));
        assertThatThrownBy(() -> QdrantPointMapper.vectorSpecs(points, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Points do not carry similarity");
    }

    @Test
    void vectorSpecsHonorEmbeddingSetSimilarityWithoutAHint() throws Exception {
        Document dotDoc = document(Similarity.SIMILARITY_DOT_PRODUCT);
        List<PointStruct> dotPoints = mapper.map(dotDoc, mappingWithoutVectorHint());
        assertThatThrownBy(() -> QdrantPointMapper.vectorSpecs(dotPoints, mappingWithoutVectorHint()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(QdrantPointMapper.vectorSpecs(dotDoc, null))
                .containsExactlyInAnyOrder(
                        new QdrantVectorSpec(MINILM_VECTOR, 2, qdrant.Collections.Distance.Dot),
                        new QdrantVectorSpec("e5_small_onnx", 2, qdrant.Collections.Distance.Dot));

        Document euclidDoc = document(Similarity.SIMILARITY_EUCLIDEAN);
        List<PointStruct> euclidPoints = mapper.map(euclidDoc, mappingWithoutVectorHint());
        assertThatThrownBy(() -> QdrantPointMapper.vectorSpecs(euclidPoints, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(QdrantPointMapper.vectorSpecs(euclidDoc, mappingWithoutVectorHint()))
                .containsExactlyInAnyOrder(
                        new QdrantVectorSpec(MINILM_VECTOR, 2, qdrant.Collections.Distance.Euclid),
                        new QdrantVectorSpec("e5_small_onnx", 2, qdrant.Collections.Distance.Euclid));
    }

    @Test
    void equalAndSerializedPointsCannotSupplyUndeclaredSimilarity() throws Exception {
        var dot = document(Similarity.SIMILARITY_DOT_PRODUCT);
        var euclid = document(Similarity.SIMILARITY_EUCLIDEAN);
        var dotPoints = mapper.map(dot, mappingWithoutVectorHint());
        var euclidPoints = mapper.map(euclid, mappingWithoutVectorHint());
        // Different source metrics project to identical wire points. A cache keyed by those
        // points cannot carry the source contract, even before serialization or restart.
        assertThat(dotPoints).isEqualTo(euclidPoints);
        var restored = new java.util.ArrayList<PointStruct>();
        for (var point : dotPoints) restored.add(PointStruct.parseFrom(point.toByteArray()));
        assertThatThrownBy(() -> QdrantPointMapper.vectorSpecs(restored, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Points do not carry similarity");
        assertThat(QdrantPointMapper.vectorSpecs(dot, null))
                .allSatisfy(spec -> assertThat(spec.distance()).isEqualTo(qdrant.Collections.Distance.Dot));
        assertThat(QdrantPointMapper.vectorSpecs(euclid, null))
                .allSatisfy(spec -> assertThat(spec.distance()).isEqualTo(qdrant.Collections.Distance.Euclid));
    }

    @Test
    void conflictingVectorHintSimilarityIsRejected() {
        IndexMapping l2Hint = mappingWithVectorHint(
                "search_metadata.embedding_sets.vectors.values", 2,
                ai.protomolt.proto.search.index.spi.VectorSimilarity.L2);

        assertThatThrownBy(() -> mapper.map(document(Similarity.SIMILARITY_COSINE), l2Hint))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("SIMILARITY_COSINE")
                .hasMessageContaining("L2");
        assertThatThrownBy(() -> QdrantPointMapper.vectorSpecs(
                document(Similarity.SIMILARITY_COSINE), l2Hint))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("SIMILARITY_COSINE")
                .hasMessageContaining("L2");
    }

    @Test
    void duplicateModelOnSameSegmentationIsRejected() {
        Document base = document();
        Document duplicateModel = base.toBuilder()
                .setSearchMetadata(base.getSearchMetadata().toBuilder()
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-minilm-dup")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(DIGEST)
                                .setModel(MINILM)
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(segmentVector("c-1", 0.5f, 0.6f))))
                .build();

        assertThatThrownBy(() -> mapper.map(duplicateModel, mapping()))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("collide on Qdrant vector name")
                .hasMessageContaining(MINILM_VECTOR);
    }

    @Test
    void collidingSanitizedModelNamesAreRejected() {
        Segmentation segmentation = document().getSearchMetadata().getSegmentations(0);
        Document colliding = Document.newBuilder()
                .setDocId("doc-collide")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSegmentations(segmentation)
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-slash")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(DIGEST)
                                .setModel("a/b")
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(segmentVector("c-1", 0.1f, 0.2f)))
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-underscore")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(DIGEST)
                                .setModel("a_b")
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(segmentVector("c-1", 0.3f, 0.4f))))
                .build();

        assertThatThrownBy(() -> mapper.map(colliding, mapping()))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("a/b")
                .hasMessageContaining("a_b")
                .hasMessageContaining("collide on Qdrant vector name");
    }

    @Test
    void mismatchedSegmentationDigestIsRejected() {
        String otherDigest =
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        Segmentation segmentation = document().getSearchMetadata().getSegmentations(0);
        Document mismatched = Document.newBuilder()
                .setDocId("doc-mismatch")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSegmentations(segmentation)
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("es-bad-digest")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(otherDigest)
                                .setModel(MINILM)
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(segmentVector("c-1", 0.1f, 0.2f))))
                .build();

        assertThatThrownBy(() -> mapper.map(mismatched, mapping()))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("segmentation_digest")
                .hasMessageContaining("content_digest");
    }

    @Test
    void vectorHintPrefersTheSemanticResultsPath() {
        IndexMapping twoVectors = new IndexMapping("ai.protomolt.proto.repo.v1.Document", List.of(
                new IndexMapping.IndexedField("other_embedding", "other",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(8)
                                .vectorSimilarity(ai.protomolt.proto.search.index.spi.VectorSimilarity.L2)
                                .build()),
                new IndexMapping.IndexedField(
                        "search_metadata.embedding_sets.vectors.values", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(2)
                                .vectorSimilarity(ai.protomolt.proto.search.index.spi.VectorSimilarity.DOT_PRODUCT)
                                .build())));

        // Two VECTOR fields would be ambiguous; the embedding_sets one wins.
        assertThat(QdrantPointMapper.vectorHint(twoVectors)).isPresent()
                .get().extracting(hint -> hint.vectorSimilarity())
                .isEqualTo(ai.protomolt.proto.search.index.spi.VectorSimilarity.DOT_PRODUCT);
    }

    @Test
    void hintSimilaritiesMapToQdrantDistances() throws Exception {
        assertThat(QdrantPointMapper.distance(ai.protomolt.proto.search.index.spi.VectorSimilarity.COSINE))
                .isEqualTo(qdrant.Collections.Distance.Cosine);
        assertThat(QdrantPointMapper.distance(ai.protomolt.proto.search.index.spi.VectorSimilarity.L2))
                .isEqualTo(qdrant.Collections.Distance.Euclid);
        assertThat(QdrantPointMapper.distance(ai.protomolt.proto.search.index.spi.VectorSimilarity.DOT_PRODUCT))
                .isEqualTo(qdrant.Collections.Distance.Dot);
        assertThat(QdrantPointMapper.distance(ai.protomolt.proto.search.index.spi.VectorSimilarity.MAX_INNER_PRODUCT))
                .isEqualTo(qdrant.Collections.Distance.Dot);
        assertThat(QdrantPointMapper.distance(Similarity.SIMILARITY_COSINE))
                .isEqualTo(qdrant.Collections.Distance.Cosine);
        assertThat(QdrantPointMapper.distance(Similarity.SIMILARITY_DOT_PRODUCT))
                .isEqualTo(qdrant.Collections.Distance.Dot);
        assertThat(QdrantPointMapper.distance(Similarity.SIMILARITY_EUCLIDEAN))
                .isEqualTo(qdrant.Collections.Distance.Euclid);
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

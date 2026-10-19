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
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;
import qdrant.JsonWithInt.ListValue;
import qdrant.Points.PointStruct;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fan-out edge cases for the Qdrant payload: a mapping path under a repeated ancestor
 * (here {@code blob_bag.blobs.blob}) reads as a list, which every point's payload has to
 * represent — or leave out.
 */
class QdrantPointMapperFanOutTest {

    private static final String DIGEST =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private final QdrantPointMapper mapper =
            new QdrantPointMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void fanOutScalarsBecomeOneListPayloadValue() throws Exception {
        Document document = documentWith(
                Blob.newBuilder().setBlobId("b1").setFilename("a.txt").setSizeBytes(11),
                Blob.newBuilder().setBlobId("b2").setFilename("b.txt").setSizeBytes(22));
        IndexMapping mapping = mapping(
                new IndexMapping.IndexedField("blob_bag.blobs.blob.filename", "filenames",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD), true),
                new IndexMapping.IndexedField("blob_bag.blobs.blob.size_bytes", "sizes",
                        ResolvedFieldHint.of(IndexFieldKind.INT64), true));

        List<PointStruct> points = mapper.map(document, mapping);

        assertThat(points).hasSize(1);
        Map<String, qdrant.JsonWithInt.Value> payload = points.get(0).getPayloadMap();
        assertThat(payload.get("filenames").getListValue().getValuesList())
                .extracting(qdrant.JsonWithInt.Value::getStringValue)
                .containsExactly("a.txt", "b.txt");
        assertThat(payload.get("sizes").getListValue().getValuesList())
                .extracting(qdrant.JsonWithInt.Value::getIntegerValue)
                .containsExactly(11L, 22L);
    }

    @Test
    void anElementWithoutTheLeafOnlyDropsItsOwnValue() throws Exception {
        Document document = documentWith(
                Blob.newBuilder().setBlobId("b1").setFilename("a.txt"),
                Blob.newBuilder().setBlobId("b2"));
        IndexMapping mapping = mapping(new IndexMapping.IndexedField(
                "blob_bag.blobs.blob.filename", "filenames",
                ResolvedFieldHint.of(IndexFieldKind.KEYWORD), true));

        ListValue filenames = mapper.map(document, mapping).get(0)
                .getPayloadMap().get("filenames").getListValue();

        assertThat(filenames.getValuesList())
                .extracting(qdrant.JsonWithInt.Value::getStringValue)
                .containsExactly("a.txt");
    }

    @Test
    // A fan-out whose every element has no payload representation is skipped like the
    // singular case, never written as an empty list.
    void fanOutOfValuesWithNoPayloadShapeIsSkippedNotAnEmptyList() throws Exception {
        Struct metadata = Struct.newBuilder()
                .putFields("k", Value.newBuilder().setStringValue("v").build())
                .build();
        Document document = documentWith(
                Blob.newBuilder().setBlobId("b1").setMetadata(metadata),
                Blob.newBuilder().setBlobId("b2").setMetadata(metadata));
        IndexMapping mapping = mapping(new IndexMapping.IndexedField(
                "blob_bag.blobs.blob.metadata", "blob_metadata",
                ResolvedFieldHint.of(IndexFieldKind.OBJECT), true));

        // Singular baseline: a message value with no flat payload shape is left out.
        Document singular = Document.newBuilder(document())
                .setBlobBag(BlobBag.newBuilder().setBlob(
                        Blob.newBuilder().setBlobId("b0").setMetadata(metadata)))
                .build();
        assertThat(mapper.map(singular, mapping(new IndexMapping.IndexedField(
                "blob_bag.blob.metadata", "blob_metadata",
                ResolvedFieldHint.of(IndexFieldKind.OBJECT), false)))
                .get(0).getPayloadMap()).doesNotContainKey("blob_metadata");

        assertThat(mapper.map(document, mapping).get(0).getPayloadMap())
                .doesNotContainKey("blob_metadata");
    }

    private static IndexMapping mapping(IndexMapping.IndexedField... fields) {
        return new IndexMapping(Document.getDescriptor().getFullName(), List.of(fields));
    }

    /** A document with one embedded chunk (so one point exists) and the given blobs. */
    private static Document documentWith(Blob.Builder... blobs) {
        Blobs.Builder bag = Blobs.newBuilder();
        for (Blob.Builder blob : blobs) {
            bag.addBlob(blob);
        }
        return Document.newBuilder(document())
                .setBlobBag(BlobBag.newBuilder().setBlobs(bag))
                .build();
    }

    private static Document document() {
        return Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSegmentations(Segmentation.newBuilder()
                                .setSegmentationId("rs-1")
                                .setContentDigest(DIGEST)
                                .setKind(SegmentationKind.SEGMENTATION_KIND_PARAGRAPH)
                                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                                .setSource(TextSource.newBuilder()
                                        .setFieldPath("search_metadata.body")
                                        .setBinding(SourceTextBinding.newBuilder()
                                                .setResolvedTextSha256(DIGEST)
                                                .setUtf8Length(14)))
                                .addSegments(Segment.newBuilder()
                                        .setSegmentId("c-1")
                                        .setOrdinal(0)
                                        .setText("the only chunk")
                                        .setStart(0)
                                        .setEnd(14)))
                        .addEmbeddingSets(EmbeddingSet.newBuilder()
                                .setEmbeddingSetId("e-1")
                                .setSegmentationId("rs-1")
                                .setSegmentationDigest(DIGEST)
                                .setModel("m")
                                .setDimensions(2)
                                .setSimilarity(Similarity.SIMILARITY_COSINE)
                                .addVectors(SegmentVector.newBuilder()
                                        .setSegmentId("c-1")
                                        .addValues(0.1f)
                                        .addValues(0.2f))))
                .build();
    }
}

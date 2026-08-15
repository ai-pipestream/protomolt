package ai.pipestream.proto.index.qdrant;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.Blobs;
import ai.pipestream.proto.repo.v1.ChunkEmbedding;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
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
                        .addSemanticResults(SemanticProcessingResult.newBuilder()
                                .setResultId("rs-1")
                                .addChunks(SemanticChunk.newBuilder()
                                        .setChunkId("c-1")
                                        .setText("the only chunk")
                                        .addEmbeddings(ChunkEmbedding.newBuilder()
                                                .setEmbeddingId("e-1")
                                                .setModel("m")
                                                .setDimensions(2)
                                                .addVector(0.1f)
                                                .addVector(0.2f)))))
                .build();
    }
}

package ai.pipestream.proto.search.index.qdrant;

import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.search.index.spi.VectorSimilarity;
import org.junit.jupiter.api.Test;
import qdrant.Collections.Distance;
import qdrant.Points.FieldType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QdrantSchemaGeneratorTest {

    private final QdrantSchemaGenerator generator = new QdrantSchemaGenerator();

    private static IndexMapping mapping() {
        return new IndexMapping("library.Book", List.of(
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT).analyzer("english").build()),
                new IndexMapping.IndexedField("genre", "genre",
                        ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                new IndexMapping.IndexedField("rank", "rank",
                        ResolvedFieldHint.of(IndexFieldKind.INT64)),
                new IndexMapping.IndexedField("score", "score",
                        ResolvedFieldHint.of(IndexFieldKind.DOUBLE)),
                new IndexMapping.IndexedField("published", "published",
                        ResolvedFieldHint.of(IndexFieldKind.BOOLEAN)),
                new IndexMapping.IndexedField("created", "created",
                        ResolvedFieldHint.of(IndexFieldKind.DATE)),
                new IndexMapping.IndexedField("blob", "blob",
                        ResolvedFieldHint.of(IndexFieldKind.BINARY)),
                new IndexMapping.IndexedField("nested", "nested",
                        ResolvedFieldHint.of(IndexFieldKind.NESTED)),
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(384)
                                .vectorSimilarity(VectorSimilarity.DOT_PRODUCT)
                                .build())));
    }

    @Test
    void vectorHintsBecomeNamedVectors() {
        QdrantSchemaGenerator.QdrantSchema schema = generator.generate(mapping());

        assertThat(schema.vectors()).containsExactly(
                new QdrantVectorSpec("embedding", 384, Distance.Dot));
    }

    @Test
    void scalarKindsBecomePayloadIndexes() {
        QdrantSchemaGenerator.QdrantSchema schema = generator.generate(mapping());

        assertThat(schema.payloadIndexes()).containsExactlyInAnyOrder(
                new QdrantSchemaGenerator.PayloadIndex("title", FieldType.FieldTypeText),
                new QdrantSchemaGenerator.PayloadIndex("genre", FieldType.FieldTypeKeyword),
                new QdrantSchemaGenerator.PayloadIndex("rank", FieldType.FieldTypeInteger),
                new QdrantSchemaGenerator.PayloadIndex("score", FieldType.FieldTypeFloat),
                new QdrantSchemaGenerator.PayloadIndex("published", FieldType.FieldTypeBool),
                new QdrantSchemaGenerator.PayloadIndex("created", FieldType.FieldTypeDatetime));
        // BINARY and NESTED have no Qdrant payload index.
        assertThat(schema.payloadIndexes())
                .extracting(QdrantSchemaGenerator.PayloadIndex::fieldName)
                .doesNotContain("blob", "nested");
    }

    /**
     * The renderer has no point data to size a vector from, so a VECTOR hint without
     * vector_dims must fail by name — not surface the bare "size must be > 0" of the
     * {@link QdrantVectorSpec} constructor.
     */
    @Test
    void dimensionlessVectorHintIsRejectedByName() {
        IndexMapping dimensionless = new IndexMapping("library.Book", List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.of(IndexFieldKind.VECTOR))));

        assertThatThrownBy(() -> generator.generate(dimensionless))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'embedding'")
                .hasMessageContaining("vector_dims");
    }

    @Test
    void defaultSimilarityIsCosine() {
        IndexMapping cosineMapping = new IndexMapping("library.Book", List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(4).build())));

        assertThat(generator.generate(cosineMapping).vectors())
                .containsExactly(QdrantVectorSpec.cosine("embedding", 4));
    }
}

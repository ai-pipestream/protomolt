package ai.protomolt.proto.search.index.qdrant;

import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import qdrant.Points.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Renders the Qdrant collection schema for an {@link IndexMapping} — the same role
 * {@code OpenSearchMappingGenerator}, {@code SolrSchemaGenerator}, and
 * {@code LuceneFieldSpecs} play for their engines.
 *
 * <p>VECTOR-hinted fields become named vectors ({@link QdrantVectorSpec}: field name, hinted
 * {@code vector_dims}, distance from the hinted {@code vector_similarity}). A VECTOR hint
 * that declares no {@code vector_dims} is rejected with an {@link IllegalArgumentException}
 * naming the field: unlike {@link QdrantSink#ensureCollectionForPoints}, which sizes vectors
 * by measuring the point data, the renderer has only the mapping to go on, and a named vector
 * cannot be declared without a size. Scalar fields become payload index declarations (Qdrant
 * {@link FieldType}), which a deployment applies with {@code CreateFieldIndex} calls;
 * nested/object/range/binary kinds have no Qdrant payload index and are omitted.
 */
public final class QdrantSchemaGenerator {

    /** The rendered collection schema: named vectors plus payload field indexes. */
    public record QdrantSchema(List<QdrantVectorSpec> vectors, List<PayloadIndex> payloadIndexes) {
        public QdrantSchema {
            vectors = List.copyOf(Objects.requireNonNull(vectors, "vectors"));
            payloadIndexes = List.copyOf(Objects.requireNonNull(payloadIndexes, "payloadIndexes"));
        }
    }

    /** One payload field index declaration (a {@code CreateFieldIndex} call's payload). */
    public record PayloadIndex(String fieldName, FieldType fieldType) {
        public PayloadIndex {
            Objects.requireNonNull(fieldName, "fieldName");
            Objects.requireNonNull(fieldType, "fieldType");
        }
    }

    /**
     * Renders the collection schema for {@code mapping}.
     *
     * @throws IllegalArgumentException if a VECTOR-hinted field declares no
     *     {@code vector_dims} (see the class contract)
     */
    public QdrantSchema generate(IndexMapping mapping) {
        Objects.requireNonNull(mapping, "mapping");
        List<QdrantVectorSpec> vectors = new ArrayList<>();
        List<PayloadIndex> payloadIndexes = new ArrayList<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            ResolvedFieldHint hint = field.hint();
            if (hint.type() == IndexFieldKind.VECTOR) {
                if (hint.vectorDims() <= 0) {
                    throw new IllegalArgumentException("Field '" + field.fieldName()
                            + "' is hinted VECTOR but declares no vector_dims; a named "
                            + "vector cannot be declared without a size. Add vector_dims "
                            + "to the indexing hint, or skip schema generation and let "
                            + "QdrantSink.ensureCollectionForPoints size the vector from "
                            + "the point data.");
                }
                vectors.add(new QdrantVectorSpec(field.fieldName(), hint.vectorDims(),
                        QdrantPointMapper.distance(hint.vectorSimilarity())));
                continue;
            }
            FieldType fieldType = payloadType(hint.type());
            if (fieldType != null) {
                payloadIndexes.add(new PayloadIndex(field.fieldName(), fieldType));
            }
        }
        return new QdrantSchema(vectors, payloadIndexes);
    }

    /** The Qdrant payload index type for a scalar hint kind, or {@code null} when none applies. */
    static FieldType payloadType(IndexFieldKind kind) {
        return switch (kind) {
            case KEYWORD -> FieldType.FieldTypeKeyword;
            case INT32, INT64 -> FieldType.FieldTypeInteger;
            case FLOAT, DOUBLE -> FieldType.FieldTypeFloat;
            case BOOLEAN -> FieldType.FieldTypeBool;
            case DATE -> FieldType.FieldTypeDatetime;
            case TEXT -> FieldType.FieldTypeText;
            default -> null;
        };
    }
}

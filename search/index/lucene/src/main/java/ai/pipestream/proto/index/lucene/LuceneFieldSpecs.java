package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.VectorElementType;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-field Lucene specs derived from an {@link IndexingPlan} — the Lucene counterpart of
 * schema generation. Lucene has no schema artifact, so instead of a mappings file consumers
 * get a typed report to apply when configuring the index: per-field analyzers (e.g. via
 * {@code PerFieldAnalyzerWrapper} — {@link ProtoLuceneMapper} cannot instantiate analyzers
 * from names), KNN codec parameters, and expected docValues shapes.
 *
 * <p>Sub-fields appear as their own entries named {@code field.sub}, matching the fields
 * {@link ProtoLuceneMapper} emits.
 */
public record LuceneFieldSpecs(String messageFullName, List<FieldSpec> fields) {

    public LuceneFieldSpecs {
        Objects.requireNonNull(messageFullName, "messageFullName");
        fields = List.copyOf(Objects.requireNonNull(fields, "fields"));
    }

    public static LuceneFieldSpecs from(IndexingPlan plan) {
        Objects.requireNonNull(plan, "plan");
        List<FieldSpec> specs = new ArrayList<>();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            ResolvedFieldHint hint = field.hint();
            specs.add(new FieldSpec(
                    field.fieldName(),
                    hint.type(),
                    hint.stored(),
                    hint.indexed(),
                    hint.sortable(),
                    hint.facetable(),
                    hint.analyzer(),
                    hint.searchAnalyzer(),
                    hint.vectorDims(),
                    hint.vectorSimilarity(),
                    hint.vectorElementType(),
                    hint.dateFormat(),
                    hint.engineParams(ProtoLuceneMapper.ENGINE_ID)));
            for (ResolvedFieldHint.SubField sub : hint.subFields()) {
                // indexed-only companion fields, mirroring ProtoLuceneMapper emission
                specs.add(new FieldSpec(
                        field.fieldName() + "." + sub.name(),
                        sub.type(),
                        false,
                        true,
                        false,
                        false,
                        sub.analyzer(),
                        "",
                        0,
                        hint.vectorSimilarity(),
                        hint.vectorElementType(),
                        "",
                        Map.of()));
            }
        }
        return new LuceneFieldSpecs(plan.messageFullName(), specs);
    }

    public Optional<FieldSpec> find(String name) {
        return fields.stream().filter(f -> f.name().equals(name)).findFirst();
    }

    /**
     * One engine field. Analyzer names are engine-interpreted — apply them at IndexWriter
     * level. Vector attributes are meaningful only when {@code kind == VECTOR}.
     */
    public record FieldSpec(
            String name,
            IndexFieldKind kind,
            boolean stored,
            boolean indexed,
            boolean sortable,
            boolean facetable,
            String analyzer,
            String searchAnalyzer,
            int vectorDims,
            VectorSimilarity vectorSimilarity,
            VectorElementType vectorElementType,
            String dateFormat,
            Map<String, String> engineParams) {

        public FieldSpec {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(kind, "kind");
            engineParams = engineParams == null ? Map.of() : Map.copyOf(engineParams);
        }

        /** Lucene similarity for {@code VECTOR} fields. */
        public VectorSimilarityFunction similarityFunction() {
            return LuceneFieldSpecs.similarityFunction(vectorSimilarity);
        }

        /** Lucene encoding for {@code VECTOR} fields. */
        public VectorEncoding vectorEncoding() {
            return vectorElementType == VectorElementType.BYTE
                    ? VectorEncoding.BYTE
                    : VectorEncoding.FLOAT32;
        }

        /**
         * The docValues shape {@link ProtoLuceneMapper} emits for this field's flags.
         *
         * <p>Lucene allows one doc-values type per field, so when a field is both sortable and
         * facetable the multi-valued form wins: it serves faceting directly and sorting via
         * {@code SortedSetSortField} / {@code SortedNumericSortField}.
         */
        public DocValuesType docValuesType() {
            if (!sortable && !facetable) {
                return DocValuesType.NONE;
            }
            return switch (kind) {
                case INT32, INT64, FLOAT, DOUBLE, DATE ->
                        facetable ? DocValuesType.SORTED_NUMERIC : DocValuesType.NUMERIC;
                default -> facetable ? DocValuesType.SORTED_SET : DocValuesType.SORTED;
            };
        }
    }

    /** Engine-neutral similarity mapped onto Lucene's {@link VectorSimilarityFunction}. */
    static VectorSimilarityFunction similarityFunction(VectorSimilarity similarity) {
        return switch (similarity) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
            case L2 -> VectorSimilarityFunction.EUCLIDEAN;
            case MAX_INNER_PRODUCT -> VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
        };
    }
}

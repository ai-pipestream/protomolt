package ai.pipestream.proto.index.solr;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.VectorSimilarity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Generates Solr managed-schema definitions from an {@link IndexingPlan}: a list of
 * {@code field} maps, the {@code fieldType} maps the fields require beyond Solr's stock
 * types (currently the {@code solr.DenseVectorField} types for vectors), and the
 * {@code copyField} maps that feed multi-fields. POST the pieces to the Schema API
 * ({@code add-field} / {@code add-field-type} / {@code add-copy-field}).
 *
 * <p>Conventions: scalar kinds use the stock {@code pint}/{@code plong}/{@code pfloat}/
 * {@code pdouble}/{@code pdate}/{@code string}/{@code boolean}/{@code binary} types; TEXT
 * uses the hint's analyzer name as the fieldType name when set ({@code text_general}
 * otherwise) — analyzer names are engine-interpreted. OBJECT/NESTED values are emitted as
 * JSON strings by {@link SolrDocumentMapper}, so they map to {@code string}. Sortable or
 * facetable hints set {@code docValues: true}. Ranges have no native Solr type and become
 * two fields {@code field_min} / {@code field_max}. Each {@link ResolvedFieldHint.SubField}
 * becomes a {@code field_sub} field plus a copyField from the parent. Vector fieldTypes
 * carry {@code vectorDimension} / {@code similarityFunction} and, when tuned,
 * {@code hnswMaxConnections} (m) / {@code hnswBeamWidth} (ef_construction); both DOT_PRODUCT
 * and MAX_INNER_PRODUCT map to Solr's {@code dot_product}. Escape-hatch params keyed
 * {@code "solr.*"} are merged into the field map verbatim. {@code null_value} has no
 * schema shape in Solr — {@link SolrDocumentMapper} substitutes it at document time.
 */
public final class SolrSchemaGenerator {

    public SolrSchema generate(IndexingPlan plan) {
        Objects.requireNonNull(plan, "plan");
        Map<String, Map<String, Object>> fieldTypes = new LinkedHashMap<>();
        List<Map<String, Object>> fields = new ArrayList<>();
        List<Map<String, Object>> copyFields = new ArrayList<>();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            ResolvedFieldHint hint = field.hint();
            if (hint.type().isRange()) {
                fields.add(fieldMap(field.fieldName() + "_min", boundType(hint.type()), hint));
                fields.add(fieldMap(field.fieldName() + "_max", boundType(hint.type()), hint));
                continue;
            }
            String type = hint.type() == IndexFieldKind.VECTOR
                    ? vectorType(fieldTypes, hint)
                    : fieldTypeName(hint.type(), hint.analyzer());
            fields.add(fieldMap(field.fieldName(), type, hint));
            for (ResolvedFieldHint.SubField sub : hint.subFields()) {
                String subName = field.fieldName() + "_" + sub.name();
                Map<String, Object> subField = new LinkedHashMap<>();
                subField.put("name", subName);
                subField.put("type", fieldTypeName(sub.type(), sub.analyzer()));
                subField.put("indexed", true);
                subField.put("stored", false);
                fields.add(subField);
                Map<String, Object> copyField = new LinkedHashMap<>();
                copyField.put("source", field.fieldName());
                copyField.put("dest", subName);
                copyFields.add(copyField);
            }
        }
        return new SolrSchema(List.copyOf(fieldTypes.values()), fields, copyFields);
    }

    private static Map<String, Object> fieldMap(String name, String type, ResolvedFieldHint hint) {
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("name", name);
        field.put("type", type);
        field.put("indexed", hint.indexed());
        field.put("stored", hint.stored());
        if (hint.sortable() || hint.facetable()) {
            field.put("docValues", true);
        }
        field.putAll(hint.engineParams(SolrDocumentMapper.ENGINE_ID));
        return field;
    }

    /** Registers (once) and names the DenseVectorField type this vector hint requires. */
    private static String vectorType(Map<String, Map<String, Object>> fieldTypes, ResolvedFieldHint hint) {
        StringBuilder name = new StringBuilder("knn_vector_")
                .append(hint.vectorDims())
                .append('_')
                .append(hint.vectorSimilarity().name().toLowerCase(Locale.ROOT));
        ResolvedFieldHint.HnswParams hnsw = hint.hnswParams();
        if (hnsw != null && hnsw.m() > 0) {
            name.append("_m").append(hnsw.m());
        }
        if (hnsw != null && hnsw.efConstruction() > 0) {
            name.append("_ef").append(hnsw.efConstruction());
        }
        return fieldTypes.computeIfAbsent(name.toString(), typeName -> {
            Map<String, Object> fieldType = new LinkedHashMap<>();
            fieldType.put("name", typeName);
            fieldType.put("class", "solr.DenseVectorField");
            fieldType.put("vectorDimension", hint.vectorDims());
            fieldType.put("similarityFunction", similarityFunction(hint.vectorSimilarity()));
            if (hnsw != null && hnsw.m() > 0) {
                fieldType.put("hnswMaxConnections", hnsw.m());
            }
            if (hnsw != null && hnsw.efConstruction() > 0) {
                fieldType.put("hnswBeamWidth", hnsw.efConstruction());
            }
            return fieldType;
        }).get("name").toString();
    }

    private static String similarityFunction(VectorSimilarity similarity) {
        return switch (similarity) {
            case COSINE -> "cosine";
            case L2 -> "euclidean";
            // Solr's DenseVectorField has no distinct max-inner-product function.
            case DOT_PRODUCT, MAX_INNER_PRODUCT -> "dot_product";
        };
    }

    private static String boundType(IndexFieldKind rangeKind) {
        return switch (rangeKind) {
            case INT_RANGE -> "pint";
            case LONG_RANGE -> "plong";
            case FLOAT_RANGE -> "pfloat";
            case DOUBLE_RANGE -> "pdouble";
            case DATE_RANGE -> "pdate";
            default -> throw new IllegalArgumentException("not a range kind: " + rangeKind);
        };
    }

    private static String fieldTypeName(IndexFieldKind kind, String analyzer) {
        return switch (kind) {
            case TEXT -> analyzer.isBlank() ? "text_general" : analyzer;
            case KEYWORD -> "string";
            case INT32 -> "pint";
            case INT64 -> "plong";
            case FLOAT -> "pfloat";
            case DOUBLE -> "pdouble";
            case BOOLEAN -> "boolean";
            case DATE -> "pdate";
            case BINARY -> "binary";
            // flat documents: object-shaped values land as JSON strings
            case OBJECT, NESTED, UNSPECIFIED -> "string";
            default -> throw new IllegalArgumentException("no direct field type for " + kind);
        };
    }

    /** Managed-schema pieces: {@code fieldTypes}, {@code fields}, {@code copyFields}. */
    public record SolrSchema(
            List<Map<String, Object>> fieldTypes,
            List<Map<String, Object>> fields,
            List<Map<String, Object>> copyFields) {

        public SolrSchema {
            fieldTypes = List.copyOf(fieldTypes);
            fields = List.copyOf(fields);
            copyFields = List.copyOf(copyFields);
        }
    }
}

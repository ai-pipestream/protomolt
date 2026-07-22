package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.MapMode;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.VectorElementType;
import ai.pipestream.proto.index.spi.VectorSimilarity;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Generates OpenSearch index mappings JSON (as {@code Map<String,Object>}) from an
 * {@link IndexingPlan}. Serialize the result with any JSON library and PUT it as the
 * index {@code mappings} body.
 *
 * <p>Per-field output: the mapping {@code type} for each {@link IndexFieldKind} (ranges
 * become {@code *_range} types; a {@link MapMode#JSON} map becomes {@code keyword}, since
 * {@link OpenSearchDocumentMapper} writes such a map as one JSON string), {@code index: false}
 * for unindexed fields other than {@link IndexFieldKind#BINARY} (the binary mapper takes only
 * {@code doc_values} and {@code store}, and fails the mapping put on anything else),
 * {@code doc_values: true} when the hint is sortable or facetable, {@code null_value},
 * {@code analyzer} / {@code search_analyzer}, {@code format} from the date format hint,
 * sub-fields under {@code fields} (queried as {@code field.sub}), and
 * {@code knn_vector} with dimension / {@code data_type} / HNSW method parameters for
 * vectors. Escape-hatch params keyed {@code "opensearch.*"} are merged into the property
 * verbatim; params for other engines are ignored.
 *
 * <p>The hint's {@code stored} flag produces no output. OpenSearch returns field values from
 * {@code _source}, which is on by default, so the per-field {@code store} parameter only adds
 * a second copy. Every hint carries {@code stored} true unless it is {@link IndexFieldKind#SKIP},
 * so deriving {@code store} from it would store the whole document twice. Fields that do need
 * it declare {@code "opensearch.store"} as an engine param.
 *
 * <p>Similarity mapping: COSINE → {@code cosinesimil}, L2 → {@code l2}, and both
 * DOT_PRODUCT and MAX_INNER_PRODUCT → {@code innerproduct} (OpenSearch's single
 * inner-product space).
 */
public final class OpenSearchMappingGenerator {

    /** Mappings body: {@code {"properties": {field: {...}}}}. */
    public Map<String, Object> generate(IndexingPlan plan) {
        Objects.requireNonNull(plan, "plan");
        Map<String, Object> properties = new LinkedHashMap<>();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            properties.put(field.fieldName(), property(field.hint()));
        }
        Map<String, Object> mappings = new LinkedHashMap<>();
        mappings.put("properties", properties);
        return mappings;
    }

    private static Map<String, Object> property(ResolvedFieldHint hint) {
        Map<String, Object> property = new LinkedHashMap<>();
        if (hint.type() == IndexFieldKind.VECTOR) {
            knnVector(property, hint);
        } else if (hint.mapMode() == MapMode.JSON) {
            // OpenSearchDocumentMapper renders a JSON-mode map as one JSON string; an object
            // mapping rejects that value at index time.
            property.put("type", "keyword");
        } else {
            property.put("type", mappingType(hint.type()));
        }
        // The binary mapper accepts only doc_values and store; any other parameter, index
        // included, fails the mapping put.
        if (!hint.indexed() && hint.type() != IndexFieldKind.BINARY) {
            property.put("index", false);
        }
        if (hint.sortable() || hint.facetable()) {
            property.put("doc_values", true);
        }
        if (hint.nullValue() != null) {
            property.put("null_value", hint.missingSubstitute().orElseThrow());
        }
        hint.analyzerOverride().ifPresent(analyzer -> property.put("analyzer", analyzer));
        hint.searchAnalyzerOverride().ifPresent(analyzer -> property.put("search_analyzer", analyzer));
        hint.dateFormatOverride().ifPresent(format -> property.put("format", format));
        if (!hint.subFields().isEmpty()) {
            Map<String, Object> fields = new LinkedHashMap<>();
            for (ResolvedFieldHint.SubField sub : hint.subFields()) {
                Map<String, Object> subProperty = new LinkedHashMap<>();
                subProperty.put("type", mappingType(sub.type()));
                sub.analyzerOverride().ifPresent(analyzer -> subProperty.put("analyzer", analyzer));
                fields.put(sub.name(), subProperty);
            }
            property.put("fields", fields);
        }
        property.putAll(hint.engineParams(OpenSearchDocumentMapper.ENGINE_ID));
        return property;
    }

    private static void knnVector(Map<String, Object> property, ResolvedFieldHint hint) {
        property.put("type", "knn_vector");
        property.put("dimension", hint.vectorDims());
        if (hint.vectorElementType() == VectorElementType.BYTE) {
            property.put("data_type", "byte");
        }
        Map<String, Object> method = new LinkedHashMap<>();
        method.put("name", "hnsw");
        method.put("space_type", spaceType(hint.vectorSimilarity()));
        ResolvedFieldHint.HnswParams hnsw = hint.hnswParams();
        if (hnsw != null && (hnsw.m() > 0 || hnsw.efConstruction() > 0)) {
            Map<String, Object> parameters = new LinkedHashMap<>();
            if (hnsw.m() > 0) {
                parameters.put("m", hnsw.m());
            }
            if (hnsw.efConstruction() > 0) {
                parameters.put("ef_construction", hnsw.efConstruction());
            }
            method.put("parameters", parameters);
        }
        property.put("method", method);
    }

    private static String spaceType(VectorSimilarity similarity) {
        return switch (similarity) {
            case COSINE -> "cosinesimil";
            case L2 -> "l2";
            // OpenSearch has a single inner-product space for both.
            case DOT_PRODUCT, MAX_INNER_PRODUCT -> "innerproduct";
        };
    }

    private static String mappingType(IndexFieldKind kind) {
        return switch (kind) {
            case TEXT -> "text";
            case KEYWORD -> "keyword";
            case INT32 -> "integer";
            case INT64 -> "long";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case BOOLEAN -> "boolean";
            case DATE -> "date";
            case BINARY -> "binary";
            case NESTED -> "nested";
            case INT_RANGE -> "integer_range";
            case LONG_RANGE -> "long_range";
            case FLOAT_RANGE -> "float_range";
            case DOUBLE_RANGE -> "double_range";
            case DATE_RANGE -> "date_range";
            // UNSPECIFIED reaches here only in hand-built plans; treat it like OBJECT.
            case OBJECT, UNSPECIFIED -> "object";
            case VECTOR, SKIP -> throw new IllegalArgumentException("no direct mapping type for " + kind);
        };
    }
}

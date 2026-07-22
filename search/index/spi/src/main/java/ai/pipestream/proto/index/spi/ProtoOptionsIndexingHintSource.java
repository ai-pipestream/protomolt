package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.index.hints.FieldIndexHint;
import ai.pipestream.proto.index.hints.IndexFieldType;
import ai.pipestream.proto.index.hints.IndexingHintsProto;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;

import java.util.Optional;

/**
 * Reads {@code (ai.pipestream.proto.index.hints.v1.index)} custom options from the descriptor.
 * Requires descriptors parsed/built with {@link #registerExtensions(ExtensionRegistry)}.
 */
public final class ProtoOptionsIndexingHintSource implements IndexingHintSource {

    public static void registerExtensions(ExtensionRegistry registry) {
        IndexingHintsProto.registerAllExtensions(registry);
    }

    @Override
    public Optional<ResolvedFieldHint> resolve(FieldDescriptor field) {
        var options = field.getOptions();
        if (!options.hasExtension(IndexingHintsProto.index)) {
            return Optional.empty();
        }
        return Optional.of(toResolved(options.getExtension(IndexingHintsProto.index), field));
    }

    static ResolvedFieldHint toResolved(FieldIndexHint hint, FieldDescriptor field) {
        IndexFieldKind kind = toKind(hint.getType());
        // Type left unset → infer it from the descriptor; explicitly-set attributes still win.
        ResolvedFieldHint defaults = kind == IndexFieldKind.UNSPECIFIED
                ? InferringIndexingHintSource.infer(field)
                : ResolvedFieldHint.of(kind);
        ResolvedFieldHint.Builder builder = defaults.toBuilder()
                .stored(hint.hasStored() ? hint.getStored() : defaults.stored())
                .indexed(hint.hasIndexed() ? hint.getIndexed() : defaults.indexed())
                .name(hint.getName())
                .vectorDims(hint.getVectorDims())
                .vectorSimilarity(toSimilarity(hint.getVectorSimilarity()))
                .vectorElementType(toElementType(hint.getVectorElementType()))
                .analyzer(hint.getAnalyzer())
                .searchAnalyzer(hint.getSearchAnalyzer())
                .skipIfMissing(hint.hasSkipIfMissing() ? hint.getSkipIfMissing() : true)
                .sortable(hint.getSortable())
                .facetable(hint.getFacetable())
                .mapMode(toMapMode(hint.getMapMode()))
                .dateFormat(hint.getDateFormat())
                .dateResolution(toResolution(hint.getDateResolution()))
                .engineParams(hint.getEngineParamsMap());
        if (hint.hasNullValue()) {
            builder.nullValue(hint.getNullValue());
        }
        if (hint.hasHnsw()) {
            builder.hnswParams(new ResolvedFieldHint.HnswParams(
                    hint.getHnsw().hasM() ? hint.getHnsw().getM() : 0,
                    hint.getHnsw().hasEfConstruction() ? hint.getHnsw().getEfConstruction() : 0));
        }
        for (var subField : hint.getSubFieldsList()) {
            builder.addSubField(new ResolvedFieldHint.SubField(
                    toKind(subField.getType()), subField.getName(), subField.getAnalyzer()));
        }
        return builder.build();
    }

    private static VectorSimilarity toSimilarity(ai.pipestream.proto.index.hints.VectorSimilarity similarity) {
        return switch (similarity) {
            case VECTOR_SIMILARITY_DOT_PRODUCT -> VectorSimilarity.DOT_PRODUCT;
            case VECTOR_SIMILARITY_L2 -> VectorSimilarity.L2;
            case VECTOR_SIMILARITY_MAX_INNER_PRODUCT -> VectorSimilarity.MAX_INNER_PRODUCT;
            // COSINE is the documented default for unspecified.
            case VECTOR_SIMILARITY_COSINE, VECTOR_SIMILARITY_UNSPECIFIED, UNRECOGNIZED -> VectorSimilarity.COSINE;
        };
    }

    private static VectorElementType toElementType(ai.pipestream.proto.index.hints.VectorElementType elementType) {
        return switch (elementType) {
            case VECTOR_ELEMENT_TYPE_BYTE -> VectorElementType.BYTE;
            // FLOAT32 is the documented default for unspecified.
            case VECTOR_ELEMENT_TYPE_FLOAT32, VECTOR_ELEMENT_TYPE_UNSPECIFIED, UNRECOGNIZED -> VectorElementType.FLOAT32;
        };
    }

    private static MapMode toMapMode(ai.pipestream.proto.index.hints.MapMode mapMode) {
        return switch (mapMode) {
            case MAP_MODE_FLATTEN -> MapMode.FLATTEN;
            case MAP_MODE_ENTRIES -> MapMode.ENTRIES;
            case MAP_MODE_JSON -> MapMode.JSON;
            case MAP_MODE_SKIP -> MapMode.SKIP;
            // null = unset: each engine keeps its documented default.
            case MAP_MODE_UNSPECIFIED, UNRECOGNIZED -> null;
        };
    }

    private static DateResolution toResolution(ai.pipestream.proto.index.hints.DateResolution resolution) {
        return switch (resolution) {
            case DATE_RESOLUTION_SECONDS -> DateResolution.SECONDS;
            // MILLIS is the documented default for unspecified.
            case DATE_RESOLUTION_MILLIS, DATE_RESOLUTION_UNSPECIFIED, UNRECOGNIZED -> DateResolution.MILLIS;
        };
    }

    private static IndexFieldKind toKind(IndexFieldType type) {
        return switch (type) {
            case INDEX_FIELD_TYPE_TEXT -> IndexFieldKind.TEXT;
            case INDEX_FIELD_TYPE_KEYWORD -> IndexFieldKind.KEYWORD;
            case INDEX_FIELD_TYPE_INT32 -> IndexFieldKind.INT32;
            case INDEX_FIELD_TYPE_INT64 -> IndexFieldKind.INT64;
            case INDEX_FIELD_TYPE_FLOAT -> IndexFieldKind.FLOAT;
            case INDEX_FIELD_TYPE_DOUBLE -> IndexFieldKind.DOUBLE;
            case INDEX_FIELD_TYPE_BOOLEAN -> IndexFieldKind.BOOLEAN;
            case INDEX_FIELD_TYPE_DATE -> IndexFieldKind.DATE;
            case INDEX_FIELD_TYPE_BINARY -> IndexFieldKind.BINARY;
            case INDEX_FIELD_TYPE_VECTOR -> IndexFieldKind.VECTOR;
            case INDEX_FIELD_TYPE_OBJECT -> IndexFieldKind.OBJECT;
            case INDEX_FIELD_TYPE_NESTED -> IndexFieldKind.NESTED;
            case INDEX_FIELD_TYPE_SKIP -> IndexFieldKind.SKIP;
            case INDEX_FIELD_TYPE_INT_RANGE -> IndexFieldKind.INT_RANGE;
            case INDEX_FIELD_TYPE_LONG_RANGE -> IndexFieldKind.LONG_RANGE;
            case INDEX_FIELD_TYPE_FLOAT_RANGE -> IndexFieldKind.FLOAT_RANGE;
            case INDEX_FIELD_TYPE_DOUBLE_RANGE -> IndexFieldKind.DOUBLE_RANGE;
            case INDEX_FIELD_TYPE_DATE_RANGE -> IndexFieldKind.DATE_RANGE;
            case INDEX_FIELD_TYPE_UNSPECIFIED, UNRECOGNIZED -> IndexFieldKind.UNSPECIFIED;
        };
    }
}

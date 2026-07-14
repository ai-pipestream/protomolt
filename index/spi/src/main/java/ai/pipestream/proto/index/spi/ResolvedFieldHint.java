package ai.pipestream.proto.index.spi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-agnostic field indexing hint (Lucene-aligned vocabulary).
 * NDJSON encoding does not consume this — only search-engine plugins do.
 *
 * <p>Beyond the core type/stored/indexed triple this carries the full indexing standard:
 * vector parameters, multi-fields, analyzers, null handling, docValues flags, map and date
 * behaviour, and the per-engine escape hatch. Prefer {@link #builder(IndexFieldKind)} /
 * {@link #toBuilder()} — the canonical constructor is unwieldy by design.
 *
 * <p>Unset-vs-default conventions: {@code nullValue} is {@code null} when absent (an empty
 * string is a legal substitute); {@code mapMode} is {@code null} when absent (each engine
 * keeps its documented default); {@code hnswParams} is {@code null} when absent (engine
 * tuning defaults apply). Analyzer names, {@code dateFormat}, and {@code name} use the
 * empty string for "unset".
 */
public record ResolvedFieldHint(
        IndexFieldKind type,
        boolean stored,
        boolean indexed,
        String name,
        int vectorDims,
        VectorSimilarity vectorSimilarity,
        VectorElementType vectorElementType,
        HnswParams hnswParams,
        List<SubField> subFields,
        String analyzer,
        String searchAnalyzer,
        String nullValue,
        boolean skipIfMissing,
        boolean sortable,
        boolean facetable,
        MapMode mapMode,
        String dateFormat,
        DateResolution dateResolution,
        Map<String, String> engineParams) {

    public ResolvedFieldHint {
        Objects.requireNonNull(type, "type");
        name = name == null ? "" : name;
        if (vectorDims < 0) {
            throw new IllegalArgumentException("vectorDims must be >= 0");
        }
        vectorSimilarity = vectorSimilarity == null ? VectorSimilarity.COSINE : vectorSimilarity;
        vectorElementType = vectorElementType == null ? VectorElementType.FLOAT32 : vectorElementType;
        subFields = subFields == null ? List.of() : List.copyOf(subFields);
        analyzer = analyzer == null ? "" : analyzer;
        searchAnalyzer = searchAnalyzer == null ? "" : searchAnalyzer;
        dateFormat = dateFormat == null ? "" : dateFormat;
        dateResolution = dateResolution == null ? DateResolution.MILLIS : dateResolution;
        engineParams = engineParams == null ? Map.of() : Map.copyOf(engineParams);
    }

    /** Pre-standard shape: type/stored/indexed/name/dims with every extension at its default. */
    public ResolvedFieldHint(IndexFieldKind type, boolean stored, boolean indexed, String name, int vectorDims) {
        this(type, stored, indexed, name, vectorDims,
                null, null, null, null, "", "", null, true, false, false, null, "", null, null);
    }

    public static ResolvedFieldHint of(IndexFieldKind type) {
        boolean indexed = type != IndexFieldKind.SKIP && type != IndexFieldKind.BINARY;
        boolean stored = type != IndexFieldKind.SKIP;
        return new ResolvedFieldHint(type, stored, indexed, "", 0);
    }

    public static ResolvedFieldHint skipped() {
        return new ResolvedFieldHint(IndexFieldKind.SKIP, false, false, "", 0);
    }

    /** Builder seeded with {@link #of(IndexFieldKind)} defaults for {@code type}. */
    public static Builder builder(IndexFieldKind type) {
        return of(type).toBuilder();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public Optional<String> nameOverride() {
        return name.isBlank() ? Optional.empty() : Optional.of(name);
    }

    public boolean isSkip() {
        return type == IndexFieldKind.SKIP;
    }

    public Optional<String> analyzerOverride() {
        return analyzer.isBlank() ? Optional.empty() : Optional.of(analyzer);
    }

    public Optional<String> searchAnalyzerOverride() {
        return searchAnalyzer.isBlank() ? Optional.empty() : Optional.of(searchAnalyzer);
    }

    public Optional<String> dateFormatOverride() {
        return dateFormat.isBlank() ? Optional.empty() : Optional.of(dateFormat);
    }

    /** The explicit map mode, or {@code engineDefault} when the hint left it unset. */
    public MapMode mapModeOr(MapMode engineDefault) {
        return mapMode != null ? mapMode : engineDefault;
    }

    /**
     * {@code null_value} coerced to the hinted type: numbers for numeric kinds, a boolean for
     * BOOLEAN, an epoch number for DATE when the string parses as a long (the raw string
     * otherwise), and the raw string for everything else. Empty when no substitute is set.
     *
     * @throws NumberFormatException when the substitute does not parse as the hinted number;
     *         {@link IndexingPlanFactory} rejects such hints at planning time
     */
    public Optional<Object> missingSubstitute() {
        if (nullValue == null) {
            return Optional.empty();
        }
        return Optional.of(switch (type) {
            case INT32 -> Integer.parseInt(nullValue);
            case INT64 -> Long.parseLong(nullValue);
            case FLOAT -> Float.parseFloat(nullValue);
            case DOUBLE -> Double.parseDouble(nullValue);
            case BOOLEAN -> Boolean.parseBoolean(nullValue);
            case DATE -> dateSubstitute(nullValue);
            default -> nullValue;
        });
    }

    private static Object dateSubstitute(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return value; // engine-side date string (e.g. ISO-8601)
        }
    }

    /**
     * Escape-hatch params scoped to one engine: keys like {@code "opensearch.index_options"}
     * are returned as {@code "index_options"} for engine id {@code "opensearch"}.
     * Keys for other engines are ignored.
     */
    public Map<String, String> engineParams(String engineId) {
        String prefix = engineId + ".";
        Map<String, String> scoped = new LinkedHashMap<>();
        engineParams.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                scoped.put(key.substring(prefix.length()), value);
            }
        });
        return Map.copyOf(scoped);
    }

    /**
     * Engine-neutral HNSW graph tuning. {@code 0} means "engine default"
     * (mirrors an unset {@code optional int32} in the proto hint).
     */
    public record HnswParams(int m, int efConstruction) {
        public HnswParams {
            if (m < 0 || efConstruction < 0) {
                throw new IllegalArgumentException("HNSW parameters must be >= 0");
            }
        }
    }

    /**
     * Additional index-time representation of the same value — the classic
     * "index once as text, again as keyword" pattern. {@code name} is a suffix; engines
     * join it to the parent field name using their own convention
     * (Lucene/OpenSearch {@code field.sub}, Solr {@code field_sub}).
     */
    public record SubField(IndexFieldKind type, String name, String analyzer) {
        public SubField {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(name, "name");
            if (name.isBlank()) {
                throw new IllegalArgumentException("sub-field name must not be blank");
            }
            analyzer = analyzer == null ? "" : analyzer;
        }

        public Optional<String> analyzerOverride() {
            return analyzer.isBlank() ? Optional.empty() : Optional.of(analyzer);
        }
    }

    /** Mutable companion for the 19-component record. */
    public static final class Builder {
        private IndexFieldKind type;
        private boolean stored;
        private boolean indexed;
        private String name;
        private int vectorDims;
        private VectorSimilarity vectorSimilarity;
        private VectorElementType vectorElementType;
        private HnswParams hnswParams;
        private final List<SubField> subFields;
        private String analyzer;
        private String searchAnalyzer;
        private String nullValue;
        private boolean skipIfMissing;
        private boolean sortable;
        private boolean facetable;
        private MapMode mapMode;
        private String dateFormat;
        private DateResolution dateResolution;
        private Map<String, String> engineParams;

        private Builder(ResolvedFieldHint hint) {
            this.type = hint.type;
            this.stored = hint.stored;
            this.indexed = hint.indexed;
            this.name = hint.name;
            this.vectorDims = hint.vectorDims;
            this.vectorSimilarity = hint.vectorSimilarity;
            this.vectorElementType = hint.vectorElementType;
            this.hnswParams = hint.hnswParams;
            this.subFields = new ArrayList<>(hint.subFields);
            this.analyzer = hint.analyzer;
            this.searchAnalyzer = hint.searchAnalyzer;
            this.nullValue = hint.nullValue;
            this.skipIfMissing = hint.skipIfMissing;
            this.sortable = hint.sortable;
            this.facetable = hint.facetable;
            this.mapMode = hint.mapMode;
            this.dateFormat = hint.dateFormat;
            this.dateResolution = hint.dateResolution;
            this.engineParams = hint.engineParams;
        }

        public Builder type(IndexFieldKind type) {
            this.type = type;
            return this;
        }

        public Builder stored(boolean stored) {
            this.stored = stored;
            return this;
        }

        public Builder indexed(boolean indexed) {
            this.indexed = indexed;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder vectorDims(int vectorDims) {
            this.vectorDims = vectorDims;
            return this;
        }

        public Builder vectorSimilarity(VectorSimilarity vectorSimilarity) {
            this.vectorSimilarity = vectorSimilarity;
            return this;
        }

        public Builder vectorElementType(VectorElementType vectorElementType) {
            this.vectorElementType = vectorElementType;
            return this;
        }

        public Builder hnswParams(HnswParams hnswParams) {
            this.hnswParams = hnswParams;
            return this;
        }

        public Builder addSubField(SubField subField) {
            this.subFields.add(Objects.requireNonNull(subField, "subField"));
            return this;
        }

        public Builder subFields(List<SubField> subFields) {
            this.subFields.clear();
            this.subFields.addAll(subFields);
            return this;
        }

        public Builder analyzer(String analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public Builder searchAnalyzer(String searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder skipIfMissing(boolean skipIfMissing) {
            this.skipIfMissing = skipIfMissing;
            return this;
        }

        public Builder sortable(boolean sortable) {
            this.sortable = sortable;
            return this;
        }

        public Builder facetable(boolean facetable) {
            this.facetable = facetable;
            return this;
        }

        public Builder mapMode(MapMode mapMode) {
            this.mapMode = mapMode;
            return this;
        }

        public Builder dateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }

        public Builder dateResolution(DateResolution dateResolution) {
            this.dateResolution = dateResolution;
            return this;
        }

        public Builder engineParams(Map<String, String> engineParams) {
            this.engineParams = engineParams;
            return this;
        }

        public ResolvedFieldHint build() {
            return new ResolvedFieldHint(type, stored, indexed, name, vectorDims,
                    vectorSimilarity, vectorElementType, hnswParams, subFields,
                    analyzer, searchAnalyzer, nullValue, skipIfMissing, sortable, facetable,
                    mapMode, dateFormat, dateResolution, engineParams);
        }
    }
}

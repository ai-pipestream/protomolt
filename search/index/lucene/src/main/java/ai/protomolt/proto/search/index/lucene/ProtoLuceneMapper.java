package ai.protomolt.proto.search.index.lucene;

import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapper;
import ai.protomolt.proto.search.index.spi.AnyIndexing;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.MapMode;
import ai.protomolt.proto.search.index.spi.MappingValues;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.index.spi.SearchEngineIndexer;
import com.google.protobuf.Message;
import org.apache.lucene.document.Document;

import java.util.List;
import java.util.Objects;

/**
 * Lucene document mapper driven by an {@link IndexMapping} (descriptor indexing hints).
 *
 * <p>Hint semantics specific to Lucene:
 * <ul>
 *   <li><b>Multi-fields</b>: each {@link ResolvedFieldHint.SubField} emits additional
 *       indexed-only {@code IndexableField}s named {@code field.sub} (same convention as
 *       OpenSearch).</li>
 *   <li><b>Analyzers</b>: this mapper cannot instantiate analyzers from names — hints carry
 *       them into {@link LuceneFieldSpecs}, and consumers apply them at IndexWriter level
 *       (e.g. {@code PerFieldAnalyzerWrapper}).</li>
 *   <li><b>Sortable / facetable</b>: emitted as docValues alongside the indexed field —
 *       {@code SortedDocValuesField} / {@code SortedSetDocValuesField} for string kinds,
 *       {@code NumericDocValuesField} / {@code SortedNumericDocValuesField} for numeric kinds.</li>
 *   <li><b>Maps</b>: with {@code map_mode} unset, OBJECT/NESTED-hinted map fields keep the
 *       whole-map JSON string (Lucene documents are flat). Explicit modes: FLATTEN emits one
 *       keyword field per key named {@code field.key}, ENTRIES one {@code {key,value}} JSON
 *       string per entry, JSON the whole-map string, SKIP nothing.</li>
 *   <li><b>Ranges</b>: singular bounds messages become single-dimension
 *       {@code IntRange}/{@code LongRange}/{@code FloatRange}/{@code DoubleRange} fields
 *       (DATE_RANGE is a {@code LongRange} of epoch values per the hint's resolution).
 *       Canonical {@code types.v1} ranges add open ends and per-end inclusivity: Lucene
 *       ranges are inclusive, so an excluded bound moves one step inward and an unset
 *       bound becomes the open-end sentinel; the day-grain {@code DateRange} covers whole
 *       days.</li>
 *   <li><b>Tree paths</b>: each path value emits its ancestor chain ("a", "a/b", "a/b/c")
 *       as {@code StringField} terms; docValues always use the {@code SortedSetDocValuesField}
 *       form (the chain is multi-valued by construction). Stored keeps the complete path
 *       only — ancestors are derivable.</li>
 *   <li><b>Dates</b>: Timestamp values are emitted numerically as epoch millis, or epoch
 *       seconds under {@code DATE_RESOLUTION_SECONDS}.</li>
 * </ul>
 */
public final class ProtoLuceneMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "lucene";

    private final ProtoFieldMapper fieldMapper;
    private final boolean includeDefaults;
    private final AnyIndexing anyIndexing;

    public ProtoLuceneMapper(ProtoFieldMapper fieldMapper) {
        this(fieldMapper, false);
    }

    public ProtoLuceneMapper(IndexerContext context) {
        this(context, false);
    }

    /**
     * @param includeDefaults when true, proto3 implicit-presence fields at their default value
     *        ({@code false} / {@code 0} / {@code ""}) are indexed instead of being skipped
     */
    public ProtoLuceneMapper(ProtoFieldMapper fieldMapper, boolean includeDefaults) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
        this.includeDefaults = includeDefaults;
        this.anyIndexing = AnyIndexing.from(fieldMapper);
    }

    public ProtoLuceneMapper(IndexerContext context, boolean includeDefaults) {
        this.fieldMapper = Objects.requireNonNull(context, "context").fieldMapper();
        this.includeDefaults = includeDefaults;
        this.anyIndexing = AnyIndexing.from(context);
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public Document map(Message message, IndexMapping mapping) throws MappingException {
        Objects.requireNonNull(mapping, "mapping");
        IndexMapping expanded = anyIndexing.expand(message, mapping);
        Document document = new Document();
        for (IndexMapping.IndexedField field : expanded.indexable()) {
            // A vector is one whole value; a path fanning out over a repeated ancestor
            // has no flat projection and fails loudly (per-chunk vectors are entities).
            Object value = field.type() == IndexFieldKind.VECTOR
                    ? MappingValues.readWhole(fieldMapper, message, field.path(), includeDefaults)
                    : MappingValues.read(fieldMapper, message, field.path(), includeDefaults);
            if (value == null) {
                // null_value substitutes for missing fields; otherwise absent stays absent
                // (skip_if_missing=false has no Lucene shape — documents cannot hold nulls).
                value = field.hint().missingSubstitute().orElse(null);
                if (value == null) {
                    continue;
                }
            }
            add(document, field, value);
        }
        return document;
    }

    /** Legacy projection API. Prefer {@link #map(Message, IndexMapping)}. */
    public Document map(Message message, List<FieldProjection> projections) throws MappingException {
        Document document = new Document();
        if (projections == null) {
            return document;
        }
        for (FieldProjection projection : projections) {
            Object value = fieldMapper.getValue(message, projection.path(), includeDefaults);
            if (value == null) {
                continue;
            }
            ResolvedLegacy legacy = new ResolvedLegacy(projection);
            add(document, legacy, value);
        }
        return document;
    }

    private void add(Document document, IndexMapping.IndexedField field, Object value)
            throws MappingException {
        add(document, field.fieldName(), field.path(), field.hint(), value);
    }

    private void add(Document document, ResolvedLegacy field, Object value) throws MappingException {
        IndexFieldKind kind = value instanceof String ? IndexFieldKind.TEXT : IndexFieldKind.KEYWORD;
        add(document, field.luceneFieldName(), field.projection().path(),
                new ResolvedFieldHint(kind, field.stored(), field.indexed(), "", 0), value);
    }

    private static void add(
            Document document,
            String name,
            String path,
            ResolvedFieldHint hint,
            Object value) throws MappingException {
        IndexFieldKind kind = hint.type();

        // Whole-value shapes first: vectors, ranges and map fields must not be split per element.
        if (kind == IndexFieldKind.VECTOR) {
            LuceneFieldEmitters.addVector(document, name, path, hint, value);
            return;
        }
        if (kind.isRange()) {
            LuceneFieldEmitters.addRange(document, name, path, hint, value);
            return;
        }
        if (kind == IndexFieldKind.TREE_PATH) {
            LuceneFieldEmitters.addTreePath(document, name, path, hint, value);
            return;
        }
        if (LuceneFieldEmitters.isMapEntryList(value)) {
            MapMode mode = hint.mapMode();
            if (mode == null && (kind == IndexFieldKind.OBJECT || kind == IndexFieldKind.NESTED
                    || kind == IndexFieldKind.UNSPECIFIED)) {
                mode = MapMode.JSON; // engine default: Lucene documents are flat
            }
            if (mode != null) {
                LuceneFieldEmitters.addMap(document, name, path, hint, (List<?>) value, mode);
                return;
            }
            // scalar-hinted map without an explicit mode: fall through to per-entry handling
        }
        if (value instanceof List<?> values) {
            // Lucene allows a single-valued doc-values field once per document, so a
            // multi-valued read (repeated leaf or fan-out) sorts and facets through the
            // SORTED_SET / SORTED_NUMERIC forms; the choosers pick those via facetable.
            ResolvedFieldHint elementHint = hint.sortable() && !hint.facetable()
                    ? hint.toBuilder().facetable(true).build()
                    : hint;
            for (Object element : values) {
                add(document, name, path, elementHint, element);
            }
            return;
        }
        LuceneFieldEmitters.addScalar(document, name, path, hint, value);
        for (ResolvedFieldHint.SubField sub : hint.subFields()) {
            // indexed-only companions; the parent field carries storage and docValues
            LuceneFieldEmitters.addScalar(document, name + "." + sub.name(), path,
                    new ResolvedFieldHint(sub.type(), false, true, "", 0), value);
        }
    }

    /**
     * Float vector from a {@code float[]}, a {@code double[]}, or a {@code List} of numbers
     * (a protobuf {@code repeated float} or {@code repeated double} arrives as the list form,
     * without caller conversion).
     *
     * @throws IllegalArgumentException when a list element is not numeric, or when the value
     *         is none of the supported shapes
     */
    static float[] toFloatVector(Object value) {
        return LuceneFieldEmitters.toFloatVector(value);
    }

    private record ResolvedLegacy(FieldProjection projection) {
        String luceneFieldName() {
            return projection.luceneFieldName();
        }

        boolean stored() {
            return projection.stored();
        }

        boolean indexed() {
            return projection.indexed();
        }
    }

    public record FieldProjection(String path, String luceneFieldName, boolean stored, boolean indexed) {
        public FieldProjection {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(luceneFieldName, "luceneFieldName");
        }
    }
}

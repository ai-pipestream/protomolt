package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.index.spi.DateResolution;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.MapMode;
import ai.pipestream.proto.index.spi.RangeBounds;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.index.spi.VectorElementType;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Lucene document mapper driven by an {@link IndexingPlan} (descriptor indexing hints).
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
 *       (DATE_RANGE is a {@code LongRange} of epoch values per the hint's resolution).</li>
 *   <li><b>Dates</b>: Timestamp values are emitted numerically as epoch millis, or epoch
 *       seconds under {@code DATE_RESOLUTION_SECONDS}.</li>
 * </ul>
 */
public final class ProtoLuceneMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "lucene";

    private static final Logger LOG = LoggerFactory.getLogger(ProtoLuceneMapper.class);
    // Structured JSON (map fields, vector fallbacks) is built with Jackson; message bodies
    // are rendered by protobuf JsonFormat. No JSON is hand-escaped.
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFormat.Printer COMPACT_PRINTER =
            JsonFormat.printer().omittingInsignificantWhitespace();

    private final ProtoFieldMapper fieldMapper;
    private final boolean includeDefaults;

    public ProtoLuceneMapper(ProtoFieldMapper fieldMapper) {
        this(fieldMapper, false);
    }

    /**
     * @param includeDefaults when true, proto3 implicit-presence fields at their default value
     *        ({@code false} / {@code 0} / {@code ""}) are indexed instead of being skipped
     */
    public ProtoLuceneMapper(ProtoFieldMapper fieldMapper, boolean includeDefaults) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
        this.includeDefaults = includeDefaults;
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public Document map(Message message, IndexingPlan plan) throws MappingException {
        Objects.requireNonNull(plan, "plan");
        Document document = new Document();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            Object value = hasUnsetIntermediate(message, field.path())
                    ? null // unset optional parent: no value for this field, not a mapping error
                    : fieldMapper.getValue(message, field.path(), includeDefaults);
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

    /** Legacy projection API. Prefer {@link #map(Message, IndexingPlan)}. */
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

    private void add(Document document, IndexingPlan.IndexedField field, Object value)
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
            addVector(document, name, path, hint, value);
            return;
        }
        if (kind.isRange()) {
            addRange(document, name, path, hint, value);
            return;
        }
        if (isMapEntryList(value)) {
            MapMode mode = hint.mapMode();
            if (mode == null && (kind == IndexFieldKind.OBJECT || kind == IndexFieldKind.NESTED
                    || kind == IndexFieldKind.UNSPECIFIED)) {
                mode = MapMode.JSON; // engine default: Lucene documents are flat
            }
            if (mode != null) {
                addMap(document, name, path, hint, (List<?>) value, mode);
                return;
            }
            // scalar-hinted map without an explicit mode: fall through to per-entry handling
        }
        if (value instanceof List<?> values) {
            for (Object element : values) {
                add(document, name, path, hint, element);
            }
            return;
        }
        addScalar(document, name, path, hint, value);
        for (ResolvedFieldHint.SubField sub : hint.subFields()) {
            // indexed-only companions; the parent field carries storage and docValues
            addScalar(document, name + "." + sub.name(), path,
                    new ResolvedFieldHint(sub.type(), false, true, "", 0), value);
        }
    }

    private static void addScalar(
            Document document,
            String name,
            String path,
            ResolvedFieldHint hint,
            Object value) throws MappingException {
        IndexFieldKind kind = hint.type();
        boolean stored = hint.stored();
        boolean indexed = hint.indexed();
        org.apache.lucene.document.Field.Store store =
                stored ? org.apache.lucene.document.Field.Store.YES : org.apache.lucene.document.Field.Store.NO;

        if (value instanceof ByteString || value instanceof byte[]) {
            byte[] bytes = value instanceof ByteString byteString
                    ? byteString.toByteArray()
                    : (byte[]) value;
            // Binary payloads are handled the same under BINARY or any mis-matched hint:
            // an exact-match term when indexed, raw stored bytes when only stored.
            if (indexed) {
                document.add(new StringField(name, new BytesRef(bytes), store));
            } else if (stored) {
                document.add(new StoredField(name, bytes));
            }
            return;
        }
        if (isTimestampMessage(value)) {
            long epoch = timestampValue((MessageOrBuilder) value, hint.dateResolution());
            if (indexed) {
                document.add(new LongPoint(name, epoch));
            }
            if (stored) {
                document.add(new StoredField(name, epoch));
            }
            addLongDocValues(document, name, hint, epoch);
            return;
        }

        switch (kind) {
            case TEXT -> {
                String stringValue = scalarText(value, path);
                if (indexed) {
                    document.add(new TextField(name, stringValue, store));
                } else if (stored) {
                    document.add(new StoredField(name, stringValue));
                }
                addStringDocValues(document, name, hint, stringValue);
            }
            case DATE -> {
                if (value instanceof Number number) {
                    // numeric date values are already in engine units (epoch millis by default)
                    long epoch = number.longValue();
                    if (indexed) {
                        document.add(new LongPoint(name, epoch));
                    }
                    if (stored) {
                        document.add(new StoredField(name, epoch));
                    }
                    addLongDocValues(document, name, hint, epoch);
                } else {
                    String stringValue = String.valueOf(value);
                    if (indexed) {
                        document.add(new StringField(name, stringValue, store));
                    } else if (stored) {
                        document.add(new StoredField(name, stringValue));
                    }
                    addStringDocValues(document, name, hint, stringValue);
                }
            }
            case KEYWORD, BOOLEAN -> {
                String stringValue = scalarText(value, path);
                if (indexed) {
                    document.add(new StringField(name, stringValue, store));
                } else if (stored) {
                    document.add(new StoredField(name, stringValue));
                }
                addStringDocValues(document, name, hint, stringValue);
            }
            case INT32 -> {
                int v = requireNumber(value, name, path, kind).intValue();
                if (indexed) {
                    document.add(new IntPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
                addLongDocValues(document, name, hint, v);
            }
            case INT64 -> {
                long v = requireNumber(value, name, path, kind).longValue();
                if (indexed) {
                    document.add(new LongPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
                addLongDocValues(document, name, hint, v);
            }
            case FLOAT -> {
                float v = requireNumber(value, name, path, kind).floatValue();
                if (indexed) {
                    document.add(new FloatPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
                if (hint.facetable()) {
                    document.add(new SortedNumericDocValuesField(name, NumericUtils.floatToSortableInt(v)));
                } else if (hint.sortable()) {
                    document.add(new FloatDocValuesField(name, v));
                }
            }
            case DOUBLE -> {
                double v = requireNumber(value, name, path, kind).doubleValue();
                if (indexed) {
                    document.add(new DoublePoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
                if (hint.facetable()) {
                    document.add(new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(v)));
                } else if (hint.sortable()) {
                    document.add(new DoubleDocValuesField(name, v));
                }
            }
            case BINARY -> throw new MappingException(
                    "Field '" + name + "' is hinted " + kind + " but the value is "
                            + typeName(value) + ", not binary", path);
            case OBJECT, NESTED, UNSPECIFIED -> {
                String text = value instanceof Message message
                        ? messageJson(message, path)
                        : String.valueOf(value);
                addJsonText(document, name, text, stored, indexed);
            }
            case VECTOR, SKIP, INT_RANGE, LONG_RANGE, FLOAT_RANGE, DOUBLE_RANGE, DATE_RANGE -> {
                // VECTOR and ranges are handled above; SKIP fields are filtered out of the plan.
            }
        }
    }

    /**
     * Sortable → {@code SortedDocValuesField}; facetable → {@code SortedSetDocValuesField}.
     * Lucene allows one doc-values type per field, so when both are hinted the multi-valued
     * form wins — it serves faceting directly and sorting via {@code SortedSetSortField}.
     */
    private static void addStringDocValues(
            Document document, String name, ResolvedFieldHint hint, String value) {
        if (hint.facetable()) {
            document.add(new SortedSetDocValuesField(name, new BytesRef(value)));
        } else if (hint.sortable()) {
            document.add(new SortedDocValuesField(name, new BytesRef(value)));
        }
    }

    /** Sortable → {@code NumericDocValuesField}; facetable → {@code SortedNumericDocValuesField}. */
    private static void addLongDocValues(
            Document document, String name, ResolvedFieldHint hint, long value) {
        // One doc-values type per field: when both are hinted the multi-valued form wins
        // (sorting still works via SortedNumericSortField).
        if (hint.facetable()) {
            document.add(new SortedNumericDocValuesField(name, value));
        } else if (hint.sortable()) {
            document.add(new NumericDocValuesField(name, value));
        }
    }

    /** Single-dimension Lucene range field from a (gte,lte)/(min,max) bounds message. */
    private static void addRange(
            Document document, String name, String path, ResolvedFieldHint hint, Object value)
            throws MappingException {
        if (!(value instanceof MessageOrBuilder range)) {
            throw new MappingException(
                    "Field '" + name + "' is hinted " + hint.type() + " but the value is "
                            + typeName(value) + ", not a bounds message", path);
        }
        RangeBounds bounds = RangeBounds.resolve(range.getDescriptorForType(), hint.type())
                .orElseThrow(() -> new MappingException(
                        "Message " + range.getDescriptorForType().getFullName()
                                + " declares no (gte,lte) or (min,max) pair matching " + hint.type(), path));
        Object lower = range.getField(bounds.lower());
        Object upper = range.getField(bounds.upper());
        switch (hint.type()) {
            case INT_RANGE -> document.add(new IntRange(name,
                    new int[]{((Number) lower).intValue()}, new int[]{((Number) upper).intValue()}));
            case LONG_RANGE -> document.add(new LongRange(name,
                    new long[]{((Number) lower).longValue()}, new long[]{((Number) upper).longValue()}));
            case FLOAT_RANGE -> document.add(new FloatRange(name,
                    new float[]{((Number) lower).floatValue()}, new float[]{((Number) upper).floatValue()}));
            case DOUBLE_RANGE -> document.add(new DoubleRange(name,
                    new double[]{((Number) lower).doubleValue()}, new double[]{((Number) upper).doubleValue()}));
            case DATE_RANGE -> document.add(new LongRange(name,
                    new long[]{dateBound(lower, hint.dateResolution())},
                    new long[]{dateBound(upper, hint.dateResolution())}));
            default -> throw new IllegalStateException("not a range kind: " + hint.type());
        }
    }

    /** Epoch value from a Timestamp bound (per resolution) or a raw int64 bound (as-is). */
    private static long dateBound(Object bound, DateResolution resolution) {
        if (bound instanceof MessageOrBuilder timestamp) {
            return timestampValue(timestamp, resolution);
        }
        return ((Number) bound).longValue();
    }

    private static void addMap(
            Document document,
            String name,
            String path,
            ResolvedFieldHint hint,
            List<?> entries,
            MapMode mode) throws MappingException {
        switch (mode) {
            case JSON -> addJsonText(document, name, mapJson(entries, path), hint.stored(), hint.indexed());
            case ENTRIES -> {
                for (Object element : entries) {
                    addJsonText(document, name, entryJson((Message) element, path),
                            hint.stored(), hint.indexed());
                }
            }
            case FLATTEN -> {
                // Dynamic keys: one keyword field per map key, named "<field>.<key>".
                org.apache.lucene.document.Field.Store store = hint.stored()
                        ? org.apache.lucene.document.Field.Store.YES
                        : org.apache.lucene.document.Field.Store.NO;
                for (Object element : entries) {
                    Message entry = (Message) element;
                    Descriptor descriptor = entry.getDescriptorForType();
                    String key = String.valueOf(entry.getField(descriptor.findFieldByName("key")));
                    Object entryValue = entry.getField(descriptor.findFieldByName("value"));
                    String text = scalarText(entryValue, path);
                    if (hint.indexed()) {
                        document.add(new StringField(name + "." + key, text, store));
                    } else if (hint.stored()) {
                        document.add(new StoredField(name + "." + key, text));
                    }
                }
            }
            case SKIP -> {
                // hinted away: nothing to emit
            }
        }
    }

    /**
     * Text form of a value being written to a string-shaped field. Message values render as
     * proto3 canonical JSON rather than {@code toString()}, which would emit protobuf text
     * format and diverge from every other message-valued path in this mapper.
     */
    private static String scalarText(Object value, String path) throws MappingException {
        if (value instanceof Message message) {
            return messageJson(message, path);
        }
        if (value instanceof EnumValueDescriptor ev) {
            return ev.getName();
        }
        return String.valueOf(value);
    }

    /** One {@code {key, value}} JSON object string per map entry. */
    private static String entryJson(Message entry, String path) throws MappingException {
        Descriptor descriptor = entry.getDescriptorForType();
        ObjectNode object = JSON.createObjectNode();
        object.set("key", jsonNode(entry.getField(descriptor.findFieldByName("key")), path));
        object.set("value", jsonNode(entry.getField(descriptor.findFieldByName("value")), path));
        try {
            return JSON.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new MappingException("Failed to render map entry as JSON", e, path);
        }
    }

    /** Fails loudly when a numeric hint meets a non-numeric value, naming the mismatch. */
    private static Number requireNumber(Object value, String name, String path, IndexFieldKind kind)
            throws MappingException {
        if (value instanceof Number number) {
            return number;
        }
        throw new MappingException(
                "Field '" + name + "' is hinted " + kind + " but the value is "
                        + typeName(value), path);
    }

    private static String typeName(Object value) {
        return value == null ? "null" : value.getClass().getName();
    }

    /**
     * Adds a JSON-rendered value as text, honouring {@code stored} and {@code indexed}
     * independently.
     *
     * <p>These are not alternatives. A field hinted both — the default for OBJECT and NESTED —
     * must be searchable <em>and</em> retrievable, so it becomes one {@link TextField} carrying
     * {@link org.apache.lucene.document.Field.Store#YES}; adding a separate {@link StoredField}
     * as well would duplicate the value on retrieval. A field that is only stored is not
     * searchable and needs no analysis, so it stays a plain {@link StoredField}.
     */
    private static void addJsonText(
            Document document, String name, String json, boolean stored, boolean indexed) {
        if (indexed) {
            document.add(new TextField(name, json, stored
                    ? org.apache.lucene.document.Field.Store.YES
                    : org.apache.lucene.document.Field.Store.NO));
        } else if (stored) {
            document.add(new StoredField(name, json));
        }
    }

    /** Compact proto3 canonical JSON for a nested message value. */
    private static String messageJson(Message message, String path) throws MappingException {
        try {
            return COMPACT_PRINTER.print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to render nested message as JSON", e, path);
        }
    }

    /** True for the reflection value of a protobuf map field: a list of map-entry messages. */
    private static boolean isMapEntryList(Object value) {
        return value instanceof List<?> values
                && !values.isEmpty()
                && values.get(0) instanceof Message message
                && message.getDescriptorForType().getOptions().getMapEntry();
    }

    /** One JSON object string for a whole protobuf map field. */
    private static String mapJson(List<?> entries, String path) throws MappingException {
        ObjectNode object = JSON.createObjectNode();
        for (Object element : entries) {
            Message entry = (Message) element;
            Descriptor descriptor = entry.getDescriptorForType();
            Object key = entry.getField(descriptor.findFieldByName("key"));
            Object entryValue = entry.getField(descriptor.findFieldByName("value"));
            object.set(String.valueOf(key), jsonNode(entryValue, path));
        }
        try {
            return JSON.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new MappingException("Failed to render map field as JSON", e, path);
        }
    }

    private static JsonNode jsonNode(Object value, String path) throws MappingException {
        if (value instanceof Message message) {
            try {
                return JSON.readTree(messageJson(message, path));
            } catch (JsonProcessingException e) {
                throw new MappingException("Failed to parse nested message JSON", e, path);
            }
        }
        if (value instanceof ByteString bytes) {
            return JSON.valueToTree(bytes.toByteArray());
        }
        if (value instanceof EnumValueDescriptor ev) {
            return JSON.valueToTree(ev.getName());
        }
        return JSON.valueToTree(value);
    }

    private static void addVector(
            Document document, String name, String path, ResolvedFieldHint hint, Object value)
            throws MappingException {
        var similarity = LuceneFieldSpecs.similarityFunction(hint.vectorSimilarity());
        if (hint.vectorElementType() == VectorElementType.BYTE) {
            byte[] vector = toByteVector(value);
            if (vector != null && hint.vectorDims() > 0 && vector.length == hint.vectorDims()) {
                document.add(new KnnByteVectorField(name, vector, similarity));
                return;
            }
            vectorFallback(document, name, path, hint, value, vector == null ? -1 : vector.length);
            return;
        }
        float[] vector = toFloatVector(value);
        if (vector != null && hint.vectorDims() > 0 && vector.length == hint.vectorDims()) {
            document.add(new KnnFloatVectorField(name, vector, similarity));
            return;
        }
        vectorFallback(document, name, path, hint, value, vector == null ? -1 : vector.length);
    }

    /** Warns and stores the raw value as JSON when it cannot form a KNN vector field. */
    private static void vectorFallback(
            Document document, String name, String path, ResolvedFieldHint hint, Object value,
            int actualLength) throws MappingException {
        LOG.warn("Field '{}' (path '{}') is hinted VECTOR with vectorDims={} but the value {} "
                        + "(length {}); storing it as JSON instead",
                name, path, hint.vectorDims(),
                actualLength < 0 ? "is not a numeric vector" : "has a mismatched length",
                actualLength < 0 ? "n/a" : actualLength);
        if (hint.stored()) {
            try {
                document.add(new StoredField(name, JSON.writeValueAsString(
                        value instanceof Message message
                                ? JSON.readTree(messageJson(message, path))
                                : value)));
            } catch (JsonProcessingException e) {
                throw new MappingException("Failed to render vector fallback as JSON", e, path);
            }
        }
    }

    /** Float vector from a repeated float/double value, or {@code null} when not numeric. */
    private static float[] toFloatVector(Object value) {
        if (!(value instanceof List<?> values) || values.isEmpty()) {
            return null;
        }
        float[] vector = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            if (!((values.get(i) instanceof Float) || (values.get(i) instanceof Double))) {
                return null;
            }
            vector[i] = ((Number) values.get(i)).floatValue();
        }
        return vector;
    }

    /**
     * Byte vector from a bytes value or a repeated int32/int64 value whose elements fit a
     * signed byte, or {@code null} otherwise.
     */
    private static byte[] toByteVector(Object value) {
        if (value instanceof ByteString bytes) {
            return bytes.toByteArray();
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (!(value instanceof List<?> values) || values.isEmpty()) {
            return null;
        }
        byte[] vector = new byte[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object element = values.get(i);
            if (!(element instanceof Integer || element instanceof Long)) {
                return null;
            }
            long v = ((Number) element).longValue();
            if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
                return null;
            }
            vector[i] = (byte) v;
        }
        return vector;
    }

    /**
     * Detects google.protobuf.Timestamp values by descriptor full name, so DynamicMessage
     * instances (registry-driven workflows) are recognised alongside generated {@link Timestamp}s.
     */
    private static boolean isTimestampMessage(Object value) {
        return value instanceof MessageOrBuilder messageOrBuilder
                && messageOrBuilder.getDescriptorForType().getFullName()
                        .equals(Timestamp.getDescriptor().getFullName());
    }

    /**
     * Epoch millis or seconds (per resolution) from a Timestamp-shaped value.
     *
     * <p>Uses {@link Math#floorDiv} rather than {@code /}: Java division truncates toward zero,
     * which for pre-epoch instants rounds <em>up</em>. A Timestamp of {@code seconds=-2,
     * nanos=500000000} is -1500ms, and {@code -1500 / 1000} is -1 — an instant one second later
     * than the value indexed at millisecond resolution, so the same field sorts and range-filters
     * differently depending only on its resolution.
     */
    private static long timestampValue(MessageOrBuilder value, DateResolution resolution) {
        long millis = timestampMillis(value);
        return resolution == DateResolution.SECONDS ? Math.floorDiv(millis, 1000L) : millis;
    }

    /** Epoch millis from a value recognised by {@link #isTimestampMessage(Object)}. */
    private static long timestampMillis(MessageOrBuilder value) {
        if (value instanceof Timestamp ts) {
            return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
        }
        Descriptor descriptor = value.getDescriptorForType();
        long seconds = ((Number) value.getField(descriptor.findFieldByName("seconds"))).longValue();
        long nanos = ((Number) value.getField(descriptor.findFieldByName("nanos"))).longValue();
        return seconds * 1000L + nanos / 1_000_000L;
    }

    /**
     * True when a dotted {@code path} traverses a singular message field that is not set,
     * meaning the leaf simply has no value. Anything the walk cannot positively resolve
     * (unknown field, repeated/non-message segment, Struct keys, Any unpacking) is left to
     * the field mapper so genuine path errors still surface as {@link MappingException}s.
     */
    private static boolean hasUnsetIntermediate(Message message, String path) {
        if (path.indexOf('.') < 0) {
            return false;
        }
        MessageOrBuilder current = message;
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length - 1; i++) {
            Descriptor descriptor = current.getDescriptorForType();
            if (descriptor.getFullName().equals(Struct.getDescriptor().getFullName())) {
                return false;
            }
            FieldDescriptor fd = descriptor.findFieldByName(parts[i]);
            if (fd == null || fd.isRepeated() || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                return false;
            }
            if (!current.hasField(fd)) {
                return true;
            }
            if (!(current.getField(fd) instanceof MessageOrBuilder next)
                    || next.getDescriptorForType().getFullName().equals(Any.getDescriptor().getFullName())) {
                return false;
            }
            current = next;
        }
        return false;
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

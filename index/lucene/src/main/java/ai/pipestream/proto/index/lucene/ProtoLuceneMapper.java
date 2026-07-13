package ai.pipestream.proto.index.lucene;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
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
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Lucene document mapper driven by an {@link IndexingPlan} (descriptor indexing hints).
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
            if (hasUnsetIntermediate(message, field.path())) {
                continue; // unset optional parent: no value for this field, not a mapping error
            }
            Object value = fieldMapper.getValue(message, field.path(), includeDefaults);
            if (value == null) {
                continue;
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
        boolean stored = hint.stored();
        boolean indexed = hint.indexed();

        // Whole-value shapes first: vectors and map fields must not be split per element.
        if (kind == IndexFieldKind.VECTOR) {
            addVector(document, name, path, hint, value);
            return;
        }
        if (isMapEntryList(value)
                && (kind == IndexFieldKind.OBJECT || kind == IndexFieldKind.NESTED
                        || kind == IndexFieldKind.UNSPECIFIED)) {
            addJsonText(document, name, mapJson((List<?>) value, path), stored, indexed);
            return;
        }
        if (value instanceof List<?> values) {
            for (Object element : values) {
                add(document, name, path, hint, element);
            }
            return;
        }
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
            long millis = timestampMillis((MessageOrBuilder) value);
            if (indexed) {
                document.add(new LongPoint(name, millis));
            }
            if (stored) {
                document.add(new StoredField(name, millis));
            }
            return;
        }

        switch (kind) {
            case TEXT -> {
                if (indexed) {
                    document.add(new TextField(name, String.valueOf(value), store));
                } else if (stored) {
                    document.add(new StoredField(name, String.valueOf(value)));
                }
            }
            case DATE -> {
                if (value instanceof Number number) {
                    // numeric date values are epoch millis
                    long millis = number.longValue();
                    if (indexed) {
                        document.add(new LongPoint(name, millis));
                    }
                    if (stored) {
                        document.add(new StoredField(name, millis));
                    }
                } else {
                    String stringValue = String.valueOf(value);
                    if (indexed) {
                        document.add(new StringField(name, stringValue, store));
                    } else if (stored) {
                        document.add(new StoredField(name, stringValue));
                    }
                }
            }
            case KEYWORD, BOOLEAN -> {
                String stringValue = value instanceof EnumValueDescriptor ev
                        ? ev.getName()
                        : String.valueOf(value);
                if (indexed) {
                    document.add(new StringField(name, stringValue, store));
                } else if (stored) {
                    document.add(new StoredField(name, stringValue));
                }
            }
            case INT32 -> {
                int v = requireNumber(value, name, path, kind).intValue();
                if (indexed) {
                    document.add(new IntPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
            }
            case INT64 -> {
                long v = requireNumber(value, name, path, kind).longValue();
                if (indexed) {
                    document.add(new LongPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
                }
            }
            case FLOAT -> {
                float v = requireNumber(value, name, path, kind).floatValue();
                if (indexed) {
                    document.add(new FloatPoint(name, v));
                }
                if (stored) {
                    document.add(new StoredField(name, v));
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
            case VECTOR, SKIP -> {
                // VECTOR is handled above; SKIP fields are filtered out of the plan.
            }
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

    private static void addJsonText(
            Document document, String name, String json, boolean stored, boolean indexed) {
        if (stored) {
            document.add(new StoredField(name, json));
        } else if (indexed) {
            // never silently drop a hinted field: index the JSON text when storage is off
            document.add(new TextField(name, json, org.apache.lucene.document.Field.Store.NO));
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
        float[] vector = toFloatVector(value);
        if (vector != null && hint.vectorDims() > 0 && vector.length == hint.vectorDims()) {
            document.add(new KnnFloatVectorField(name, vector));
            return;
        }
        LOG.warn("Field '{}' (path '{}') is hinted VECTOR with vectorDims={} but the value {} "
                        + "(length {}); storing it as JSON instead",
                name, path, hint.vectorDims(),
                vector == null ? "is not a repeated float/double" : "has a mismatched length",
                vector == null ? "n/a" : vector.length);
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
     * Detects google.protobuf.Timestamp values by descriptor full name, so DynamicMessage
     * instances (registry-driven workflows) are recognised alongside generated {@link Timestamp}s.
     */
    private static boolean isTimestampMessage(Object value) {
        return value instanceof MessageOrBuilder messageOrBuilder
                && messageOrBuilder.getDescriptorForType().getFullName()
                        .equals(Timestamp.getDescriptor().getFullName());
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

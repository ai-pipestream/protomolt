package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.helpers.TypeConverter;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.MapMode;
import ai.pipestream.proto.index.spi.RangeBounds;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * OpenSearch-oriented document map builder.
 * Uses a shared {@link IndexingPlan} (descriptor hints); does not emit NDJSON —
 * pair with {@code protomolt-index-ndjson} when you need bulk lines.
 *
 * <p>Message values are converted through {@link JsonFormat}, so nested values take exactly
 * the shapes the NDJSON writer emits: nested maps become JSON objects, int64 becomes a string,
 * and Timestamps become ISO-8601 strings (consistent with the Solr mapper and compatible with
 * OpenSearch {@code date} mappings), including under a DATE hint. Dates are never emitted
 * numerically here, so the hint's {@code date_resolution} does not apply.
 *
 * <p>Hint semantics specific to OpenSearch:
 * <ul>
 *   <li><b>Multi-fields</b>: index-time only — {@link OpenSearchMappingGenerator} declares
 *       them under {@code fields} and OpenSearch derives {@code field.sub} from the single
 *       document value, so this mapper never duplicates values.</li>
 *   <li><b>Null handling</b>: a {@code null_value} substitute is emitted for missing fields;
 *       {@code skip_if_missing=false} emits an explicit JSON {@code null} instead.</li>
 *   <li><b>Maps</b>: {@code map_mode} unset defaults to FLATTEN (a JSON object with dynamic
 *       keys); ENTRIES emits {@code [{key, value}]}, JSON one string, SKIP nothing.</li>
 *   <li><b>Ranges</b>: bounds messages become {@code {gte, lte}} objects for the
 *       {@code *_range} mapping types, whichever bound pair the message declares.</li>
 * </ul>
 */
public final class OpenSearchDocumentMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "opensearch";

    // Message bodies are proto3 JSON via JsonFormat; Jackson only parses that output into
    // plain maps/lists/scalars. No JSON is hand-escaped.
    private static final JsonFormat.Printer COMPACT_PRINTER =
            JsonFormat.printer().omittingInsignificantWhitespace();
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ProtoFieldMapper fieldMapper;
    private final boolean includeDefaults;

    public OpenSearchDocumentMapper(ProtoFieldMapper fieldMapper) {
        this(fieldMapper, false);
    }

    /**
     * @param includeDefaults when true, proto3 implicit-presence fields at their default value
     *        ({@code false} / {@code 0} / {@code ""}) are written to documents instead of skipped
     */
    public OpenSearchDocumentMapper(ProtoFieldMapper fieldMapper, boolean includeDefaults) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
        this.includeDefaults = includeDefaults;
    }

    /**
     * @deprecated nested message conversion now goes through {@link JsonFormat}; the
     *             {@code typeConverter} argument is ignored. Use
     *             {@link #OpenSearchDocumentMapper(ProtoFieldMapper)}.
     */
    @Deprecated
    public OpenSearchDocumentMapper(ProtoFieldMapper fieldMapper, TypeConverter typeConverter) {
        this(fieldMapper);
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public Map<String, Object> map(Message message, IndexingPlan plan) throws MappingException {
        Objects.requireNonNull(plan, "plan");
        Map<String, Object> document = new LinkedHashMap<>();
        for (IndexingPlan.IndexedField field : plan.indexable()) {
            ResolvedFieldHint hint = field.hint();
            Object value = hasUnsetIntermediate(message, field.path())
                    ? null // unset optional parent: no value for this field, not a mapping error
                    : fieldMapper.getValue(message, field.path(), includeDefaults);
            if (value == null) {
                // null_value substitutes for missing fields; skip_if_missing=false emits
                // an explicit JSON null; otherwise absent stays absent.
                var substitute = hint.missingSubstitute();
                if (substitute.isPresent()) {
                    document.put(field.fieldName(), substitute.get());
                } else if (!hint.skipIfMissing()) {
                    document.put(field.fieldName(), null);
                }
                continue;
            }
            if (hint.type().isRange()) {
                document.put(field.fieldName(), rangeObject(value, hint, field.path()));
                continue;
            }
            if (isMapEntryList(value)) {
                applyMap(document, field.fieldName(), (List<?>) value, hint, field.path());
                continue;
            }
            document.put(field.fieldName(), coerce(value, field.path()));
        }
        return document;
    }

    /** {@code {gte, lte}} object for a {@code *_range} mapping, from a bounds message. */
    private static Map<String, Object> rangeObject(Object value, ResolvedFieldHint hint, String path)
            throws MappingException {
        if (!(value instanceof Message range)) {
            throw new MappingException(
                    "Field is hinted " + hint.type() + " but the value is "
                            + value.getClass().getName() + ", not a bounds message", path);
        }
        RangeBounds bounds = RangeBounds.resolve(range.getDescriptorForType(), hint.type())
                .orElseThrow(() -> new MappingException(
                        "Message " + range.getDescriptorForType().getFullName()
                                + " declares no (gte,lte) or (min,max) pair matching " + hint.type(), path));
        Map<String, Object> object = new LinkedHashMap<>();
        object.put("gte", coerce(range.getField(bounds.lower()), path));
        object.put("lte", coerce(range.getField(bounds.upper()), path));
        return object;
    }

    private static void applyMap(
            Map<String, Object> document,
            String name,
            List<?> entries,
            ResolvedFieldHint hint,
            String path) throws MappingException {
        switch (hint.mapModeOr(MapMode.FLATTEN)) {
            case FLATTEN -> document.put(name, flattenedMap(entries, path));
            case ENTRIES -> {
                List<Object> entryObjects = new ArrayList<>(entries.size());
                for (Object element : entries) {
                    Message entry = (Message) element;
                    Descriptor descriptor = entry.getDescriptorForType();
                    Map<String, Object> object = new LinkedHashMap<>();
                    object.put("key", coerce(entry.getField(descriptor.findFieldByName("key")), path));
                    object.put("value", coerce(entry.getField(descriptor.findFieldByName("value")), path));
                    entryObjects.add(object);
                }
                document.put(name, entryObjects);
            }
            case JSON -> {
                try {
                    document.put(name, JSON.writeValueAsString(flattenedMap(entries, path)));
                } catch (JsonProcessingException e) {
                    throw new MappingException("Failed to render map field as JSON", e, path);
                }
            }
            case SKIP -> {
                // hinted away: nothing to emit
            }
        }
    }

    /** Dynamic-keys object for a whole protobuf map field. */
    private static Map<String, Object> flattenedMap(List<?> entries, String path) throws MappingException {
        Map<String, Object> flattened = new LinkedHashMap<>();
        for (Object element : entries) {
            Message entry = (Message) element;
            Descriptor descriptor = entry.getDescriptorForType();
            flattened.put(String.valueOf(entry.getField(descriptor.findFieldByName("key"))),
                    coerce(entry.getField(descriptor.findFieldByName("value")), path));
        }
        return flattened;
    }

    /** True for the reflection value of a protobuf map field: a list of map-entry messages. */
    private static boolean isMapEntryList(Object value) {
        return value instanceof List<?> values
                && !values.isEmpty()
                && values.get(0) instanceof Message message
                && message.getDescriptorForType().getOptions().getMapEntry();
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

    /** Legacy projection API (explicit paths). Prefer {@link #map(Message, IndexingPlan)}. */
    public Map<String, Object> map(Message message, List<FieldProjection> projections) throws MappingException {
        if (projections == null || projections.isEmpty()) {
            Object document = jsonShaped(message, message.getDescriptorForType().getFullName());
            if (document instanceof Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> objectDocument = (Map<String, Object>) map;
                return objectDocument;
            }
            throw new MappingException(
                    "Whole-message mapping requires a message that serializes to a JSON object, but "
                            + message.getDescriptorForType().getFullName()
                            + " serializes to a JSON value", null);
        }
        Map<String, Object> document = new LinkedHashMap<>();
        for (FieldProjection projection : projections) {
            Object value = fieldMapper.getValue(message, projection.path(), includeDefaults);
            if (value != null) {
                document.put(projection.fieldName(), coerce(value, projection.path()));
            }
        }
        return document;
    }

    private static Object coerce(Object value, String path) throws MappingException {
        if (value instanceof List<?> values) {
            List<Object> coerced = new ArrayList<>(values.size());
            for (Object element : values) {
                coerced.add(coerce(element, path));
            }
            return coerced;
        }
        if (value instanceof com.google.protobuf.ByteString bytes) {
            return bytes.toByteArray();
        }
        if (value instanceof com.google.protobuf.Descriptors.EnumValueDescriptor ev) {
            return ev.getName();
        }
        if (value instanceof Message message) {
            // Route through JsonFormat so nested shapes match the NDJSON writer exactly
            // (Timestamps become ISO-8601 strings, maps become JSON objects, int64 strings).
            return jsonShaped(message, path);
        }
        return value;
    }

    /** Plain maps/lists/scalars mirroring the message's proto3 canonical JSON form. */
    private static Object jsonShaped(Message message, String path) throws MappingException {
        String json;
        try {
            json = COMPACT_PRINTER.print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException("Failed to render message as JSON", e, path);
        }
        try {
            return JSON.readValue(json, Object.class);
        } catch (JsonProcessingException e) {
            throw new MappingException("Failed to parse JsonFormat output", e, path);
        }
    }

    public record FieldProjection(String path, String fieldName) {
        public FieldProjection {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(fieldName, "fieldName");
        }
    }
}

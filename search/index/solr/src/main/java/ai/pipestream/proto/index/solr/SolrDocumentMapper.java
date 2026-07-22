package ai.pipestream.proto.index.solr;

import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.MapMode;
import ai.pipestream.proto.index.spi.RangeBounds;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.SearchEngineIndexer;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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
 * Solr-oriented document map builder (SolrInputDocument-compatible {@link Map}).
 * Uses a shared {@link IndexingPlan} from descriptor indexing hints.
 *
 * <p>Message values are rendered through {@link JsonFormat}: object-shaped messages become
 * compact JSON strings (Solr documents are flat), while well-known types that print as JSON
 * primitives (Timestamp, Duration, wrappers, FieldMask) become the primitive itself — a
 * DATE-hinted Timestamp therefore lands as an ISO-8601 string, which Solr date fields accept.
 * Dates are never emitted numerically here, so the hint's {@code date_resolution} does not apply.
 *
 * <p>Hint semantics specific to Solr:
 * <ul>
 *   <li><b>Multi-fields</b>: index-time only — {@link SolrSchemaGenerator} declares a
 *       {@code field_sub} field plus a {@code copyField} from the parent, so this mapper
 *       never duplicates values.</li>
 *   <li><b>Null handling</b>: a {@code null_value} substitute is emitted for missing fields.
 *       Solr documents cannot hold explicit nulls, so {@code skip_if_missing=false} without a
 *       substitute still skips the field.</li>
 *   <li><b>Maps</b>: {@code map_mode} unset defaults to ENTRIES (one {@code {key,value}}
 *       JSON string per entry — today's flat shape). FLATTEN emits one {@code field_key}
 *       document field per map key, JSON one whole-map string, SKIP nothing.</li>
 *   <li><b>Ranges</b>: Solr has no native range type — bounds messages become two document
 *       fields {@code field_min} / {@code field_max}, whichever bound pair the message
 *       declares.</li>
 * </ul>
 */
public final class SolrDocumentMapper implements SearchEngineIndexer {
    public static final String ENGINE_ID = "solr";

    // Message bodies are proto3 JSON via JsonFormat; Jackson only parses that output to decide
    // between object and primitive shapes. No JSON is hand-escaped.
    private static final JsonFormat.Printer COMPACT_PRINTER =
            JsonFormat.printer().omittingInsignificantWhitespace();
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ProtoFieldMapper fieldMapper;
    private final boolean includeDefaults;

    public SolrDocumentMapper(ProtoFieldMapper fieldMapper) {
        this(fieldMapper, false);
    }

    /**
     * @param includeDefaults when true, proto3 implicit-presence fields at their default value
     *        ({@code false} / {@code 0} / {@code ""}) are written to documents instead of skipped
     */
    public SolrDocumentMapper(ProtoFieldMapper fieldMapper, boolean includeDefaults) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
        this.includeDefaults = includeDefaults;
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
                // null_value substitutes for missing fields; Solr documents cannot hold
                // explicit nulls, so anything else absent stays absent.
                hint.missingSubstitute().ifPresent(substitute -> document.put(field.fieldName(), substitute));
                continue;
            }
            if (hint.type().isRange()) {
                applyRange(document, field.fieldName(), value, hint, field.path());
                continue;
            }
            if (isMapEntryList(value)) {
                applyMap(document, field.fieldName(), (List<?>) value, hint, field.path());
                continue;
            }
            Object coerced = coerce(value, field.path());
            if (coerced != null) {
                document.put(field.fieldName(), coerced);
            }
        }
        return document;
    }

    /** Two flat fields {@code name_min} / {@code name_max} from a bounds message. */
    private static void applyRange(
            Map<String, Object> document,
            String name,
            Object value,
            ResolvedFieldHint hint,
            String path) throws MappingException {
        if (!(value instanceof Message range)) {
            throw new MappingException(
                    "Field is hinted " + hint.type() + " but the value is "
                            + value.getClass().getName() + ", not a bounds message", path);
        }
        RangeBounds bounds = RangeBounds.resolve(range.getDescriptorForType(), hint.type())
                .orElseThrow(() -> new MappingException(
                        "Message " + range.getDescriptorForType().getFullName()
                                + " declares no (gte,lte) or (min,max) pair matching " + hint.type(), path));
        document.put(name + "_min", coerce(range.getField(bounds.lower()), path));
        document.put(name + "_max", coerce(range.getField(bounds.upper()), path));
    }

    private static void applyMap(
            Map<String, Object> document,
            String name,
            List<?> entries,
            ResolvedFieldHint hint,
            String path) throws MappingException {
        switch (hint.mapModeOr(MapMode.ENTRIES)) {
            // coerce turns each object-shaped entry message into one {key,value} JSON string
            case ENTRIES -> document.put(name, coerce(entries, path));
            case FLATTEN -> {
                for (Object element : entries) {
                    Message entry = (Message) element;
                    Descriptor descriptor = entry.getDescriptorForType();
                    document.put(name + "_" + entry.getField(descriptor.findFieldByName("key")),
                            coerce(entry.getField(descriptor.findFieldByName("value")), path));
                }
            }
            case JSON -> {
                Map<String, Object> flattened = new LinkedHashMap<>();
                for (Object element : entries) {
                    Message entry = (Message) element;
                    Descriptor descriptor = entry.getDescriptorForType();
                    flattened.put(String.valueOf(entry.getField(descriptor.findFieldByName("key"))),
                            coerce(entry.getField(descriptor.findFieldByName("value")), path));
                }
                try {
                    document.put(name, JSON.writeValueAsString(flattened));
                } catch (JsonProcessingException e) {
                    throw new MappingException("Failed to render map field as JSON", e, path);
                }
            }
            case SKIP -> {
                // hinted away: nothing to emit
            }
        }
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

    /** Legacy projection API. Prefer {@link #map(Message, IndexingPlan)}. */
    public Map<String, Object> map(Message message, List<FieldProjection> projections) throws MappingException {
        Map<String, Object> document = new LinkedHashMap<>();
        if (projections == null) {
            return document;
        }
        for (FieldProjection projection : projections) {
            Object value = fieldMapper.getValue(message, projection.path(), includeDefaults);
            if (value != null) {
                Object coerced = coerce(value, projection.path());
                if (coerced != null) {
                    document.put(projection.fieldName(), coerced);
                }
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
            String json;
            try {
                json = COMPACT_PRINTER.print(message);
            } catch (InvalidProtocolBufferException e) {
                throw new MappingException("Failed to render nested message as JSON", e, path);
            }
            JsonNode node;
            try {
                node = JSON.readTree(json);
            } catch (JsonProcessingException e) {
                throw new MappingException("Failed to parse JsonFormat output", e, path);
            }
            if (node.isObject() || node.isArray()) {
                // Solr documents are flat: object-shaped messages are emitted as JSON strings.
                return json;
            }
            // Well-known types printing as JSON primitives (Timestamp, Duration, wrappers,
            // FieldMask) become the raw value: never the quoted JSON literal.
            return unwrapPrimitive(node);
        }
        return value;
    }

    private static Object unwrapPrimitive(JsonNode node) {
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isNull()) {
            return null;
        }
        return node.toString();
    }

    public record FieldProjection(String path, String fieldName) {
        public FieldProjection {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(fieldName, "fieldName");
        }
    }
}

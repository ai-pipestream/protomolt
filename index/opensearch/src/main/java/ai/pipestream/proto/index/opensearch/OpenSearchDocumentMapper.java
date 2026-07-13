package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.helpers.TypeConverter;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.index.spi.IndexingPlan;
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
 * pair with {@code proteus-index-ndjson} when you need bulk lines.
 *
 * <p>Message values are converted through {@link JsonFormat}, so nested values take exactly
 * the shapes the NDJSON writer emits: nested maps become JSON objects, int64 becomes a string,
 * and Timestamps become ISO-8601 strings (consistent with the Solr mapper and compatible with
 * OpenSearch {@code date} mappings), including under a DATE hint.
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
            if (hasUnsetIntermediate(message, field.path())) {
                continue; // unset optional parent: no value for this field, not a mapping error
            }
            Object value = fieldMapper.getValue(message, field.path(), includeDefaults);
            if (value != null) {
                document.put(field.fieldName(), coerce(value, field.path()));
            }
        }
        return document;
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

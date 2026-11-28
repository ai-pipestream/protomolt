package ai.protomolt.proto.projection;

import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapper;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Zero-regex dotted-path validation, {@code FieldMask} truncation, and typed value extraction for
 * {@link MessageProjection}.
 */
final class SourcePathSupport {

    private static final String ANY_FULL_NAME = Any.getDescriptor().getFullName();
    private static final String STRUCT_FULL_NAME = Struct.getDescriptor().getFullName();
    private static final int MAP_KEY_FIELD_NUMBER = 1;
    private static final int MAP_VALUE_FIELD_NUMBER = 2;

    private SourcePathSupport() {
    }

    /**
     * Whether a dotted path resolves against {@code type}. Surrounding whitespace is ignored; an
     * empty segment (a leading, trailing, or doubled dot) or whitespace inside a segment does not
     * resolve. Map fields end descriptor traversal once remaining segment syntax is verified.
     */
    static boolean resolves(Descriptor type, String path) {
        String trimmed = path.trim();
        Descriptor current = type;
        int start = 0;
        while (true) {
            int dot = trimmed.indexOf('.', start);
            String segment = dot < 0 ? trimmed.substring(start) : trimmed.substring(start, dot);
            if (current == null || !isValidSegment(segment)) {
                return false;
            }
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null) {
                return false;
            }
            if (field.isMapField()) {
                return dot < 0 || hasValidRemainingSegments(trimmed, dot + 1);
            }
            if (dot < 0) {
                return true;
            }
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && isStructDescriptor(field.getMessageType())) {
                return hasValidRemainingSegments(trimmed, dot + 1);
            }
            current = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    ? field.getMessageType() : null;
            start = dot + 1;
        }
    }

    /**
     * Returns the valid {@link com.google.protobuf.FieldMask} path covering {@code path} on
     * {@code type}, or empty when {@code path} does not resolve.
     *
     * <p>{@code FieldMask} paths cannot extend below a {@code map}, {@code repeated}, or
     * {@code google.protobuf.Struct} field; truncating at that boundary yields a valid mask path
     * that covers the entire collection or {@code Struct} subtree.</p>
     */
    static Optional<String> sourceMaskPath(Descriptor type, String path) {
        if (!resolves(type, path)) {
            return Optional.empty();
        }
        String trimmed = path.trim();
        Descriptor current = type;
        int start = 0;
        while (true) {
            int dot = trimmed.indexOf('.', start);
            String segment = dot < 0 ? trimmed.substring(start) : trimmed.substring(start, dot);
            FieldDescriptor field = current.findFieldByName(segment);
            int endOfSegment = dot < 0 ? trimmed.length() : dot;
            if (dot < 0
                    || field.isMapField()
                    || field.isRepeated()
                    || (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                            && isStructDescriptor(field.getMessageType()))) {
                return Optional.of(trimmed.substring(0, endOfSegment));
            }
            current = field.getMessageType();
            start = dot + 1;
        }
    }

    /**
     * Reads {@code path} from {@code source}, returning {@code null} when absent or unresolvable.
     *
     * <p>Protobuf {@code map<K, V>} fields return a {@link MapInput.ProtoEntries} wrapper when
     * non-empty and {@code null} when empty (matching proto3 implicit-presence absence on the
     * wire), keeping map sources distinct from ordinary {@code repeated} lists. Unsigned integer
     * scalars are normalized to {@link UnsignedLong} so their unsigned magnitude survives coercion.</p>
     */
    static Object tryRead(Message source, String path, ProtoFieldMapper fieldMapper) {
        String trimmed = path.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (isStructDescriptor(source.getDescriptorForType())) {
            return tryDelegateMapper(source, trimmed, fieldMapper);
        }
        Message current = source;
        int start = 0;
        while (true) {
            int dot = trimmed.indexOf('.', start);
            String segment = dot < 0 ? trimmed.substring(start) : trimmed.substring(start, dot);
            if (!isValidSegment(segment)) {
                return null;
            }
            FieldDescriptor fd = current.getDescriptorForType().findFieldByName(segment);
            if (fd == null) {
                return null;
            }
            if (dot < 0) {
                return readLeafField(current, fd, fieldMapper);
            }
            if (fd.isRepeated() || fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || !current.hasField(fd)) {
                return null;
            }
            if (isStructDescriptor(fd.getMessageType())) {
                return tryDelegateMapper(source, trimmed, fieldMapper);
            }
            Message child = (Message) current.getField(fd);
            if (isAnyDescriptor(fd.getMessageType())) {
                child = unpackAny(child, fieldMapper);
                if (child == null) {
                    return null;
                }
                if (isStructDescriptor(child.getDescriptorForType())) {
                    return tryDelegateMapper(source, trimmed, fieldMapper);
                }
            }
            current = child;
            start = dot + 1;
        }
    }

    private static Object readLeafField(
            Message current, FieldDescriptor fd, ProtoFieldMapper fieldMapper) {
        if (fd.isMapField()) {
            List<?> rawEntries = (List<?>) current.getField(fd);
            if (rawEntries.isEmpty()) {
                return null;
            }
            FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(MAP_KEY_FIELD_NUMBER);
            FieldDescriptor valFd = fd.getMessageType().findFieldByNumber(MAP_VALUE_FIELD_NUMBER);
            // Collapse wire-duplicate keys with last-entry-wins so DynamicMessage and
            // GeneratedMessage sources behave identically on raw wire bytes.
            java.util.Map<Object, Object> merged = new java.util.LinkedHashMap<>(rawEntries.size());
            for (Object item : rawEntries) {
                Message entry = (Message) item;
                Object key = normalizeScalar(keyFd, entry.getField(keyFd));
                Object val = normalizeScalar(valFd, entry.getField(valFd));
                merged.put(key, val);
            }
            List<MapInput.RawEntry> entries = new ArrayList<>(merged.size());
            for (java.util.Map.Entry<Object, Object> pair : merged.entrySet()) {
                entries.add(new MapInput.RawEntry(pair.getKey(), pair.getValue()));
            }
            return new MapInput.ProtoEntries(entries);
        }
        if (fd.isRepeated()) {
            List<?> rawList = (List<?>) current.getField(fd);
            if (!isUnsigned(fd) || rawList.isEmpty()) {
                return rawList;
            }
            List<Object> normalized = new ArrayList<>(rawList.size());
            for (Object item : rawList) {
                normalized.add(normalizeScalar(fd, item));
            }
            return normalized;
        }
        if (!current.hasField(fd)) {
            return null;
        }
        Object raw = current.getField(fd);
        if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && isAnyDescriptor(fd.getMessageType())) {
            return unpackAny((Message) raw, fieldMapper);
        }
        return normalizeScalar(fd, raw);
    }

    static Object normalizeScalar(FieldDescriptor fd, Object raw) {
        if (raw == null) {
            return null;
        }
        return switch (fd.getType()) {
            case UINT32, FIXED32 -> UnsignedLong.fromLongBits(Integer.toUnsignedLong((Integer) raw));
            case UINT64, FIXED64 -> UnsignedLong.fromLongBits((Long) raw);
            default -> raw;
        };
    }

    private static boolean isUnsigned(FieldDescriptor fd) {
        FieldDescriptor.Type type = fd.getType();
        return type == FieldDescriptor.Type.UINT32
                || type == FieldDescriptor.Type.FIXED32
                || type == FieldDescriptor.Type.UINT64
                || type == FieldDescriptor.Type.FIXED64;
    }

    private static Message unpackAny(Message rawAny, ProtoFieldMapper fieldMapper) {
        try {
            Any any = rawAny instanceof Any typed ? typed : Any.parseFrom(rawAny.toByteString());
            return fieldMapper.getAnyHandler().unpack(any);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private static Object tryDelegateMapper(
            Message source, String path, ProtoFieldMapper fieldMapper) {
        try {
            return fieldMapper.getValue(source, path, false);
        } catch (MappingException e) {
            return null;
        }
    }

    private static boolean hasValidRemainingSegments(String text, int start) {
        int pos = start;
        while (true) {
            int dot = text.indexOf('.', pos);
            String segment = dot < 0 ? text.substring(pos) : text.substring(pos, dot);
            if (!isValidSegment(segment)) {
                return false;
            }
            if (dot < 0) {
                return true;
            }
            pos = dot + 1;
        }
    }

    private static boolean isValidSegment(String segment) {
        if (segment.isEmpty()) {
            return false;
        }
        for (int i = 0; i < segment.length(); i++) {
            if (Character.isWhitespace(segment.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static boolean isStructDescriptor(Descriptor descriptor) {
        return STRUCT_FULL_NAME.equals(descriptor.getFullName());
    }

    private static boolean isAnyDescriptor(Descriptor descriptor) {
        return ANY_FULL_NAME.equals(descriptor.getFullName());
    }
}

package ai.protomolt.proto.search.index.spi;

import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapper;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Struct;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads one mapping entry's value from a document, the shared engine write-path read.
 *
 * <p>The field mapper's {@code getValue} deliberately refuses to traverse a repeated
 * intermediate — a mapping rule names one value. A mapping path is different: a
 * {@link BlockRole#CHUNKS} scope (or any expanded repeated message) legitimately puts
 * paths like {@code chunks.text} in the mapping, and the flat projection of such a path is
 * every leaf value across the repeated elements. This reader fans out over repeated
 * message intermediates depth-first — {@code google.protobuf.Any} intermediates are
 * unpacked and traversed like the mapper does — and returns the flattened leaf values as
 * a {@link List} (document order), {@code null} when no element contributes a value.
 * The remainder below a fan-out is checked against the element descriptor, so a path
 * that cannot resolve fails loudly even when the repeated field is empty.
 *
 * <p>Paths without a repeated intermediate keep the mapper's exact semantics — including
 * {@code google.protobuf.Struct} key traversal, Any unpacking, and error texts — with one
 * refinement the engines previously implemented privately: a path through an unset
 * singular message parent reads as missing ({@code null}) instead of an error. Below a
 * fan-out, an element whose singular parent (an Any included) is unset simply contributes
 * nothing, and leaf segments are always field names — never the mapper DSL's literals.
 */
public final class MappingValues {

    private MappingValues() {
    }

    /**
     * Reads {@code path} from {@code message} for an index mapping entry.
     *
     * @param includeDefaults when true, a proto3 implicit-presence leaf at its default
     *        ({@code false} / {@code 0} / {@code ""}) is returned instead of skipped
     * @return the single value, a flattened {@link List} when the path fans out over
     *         repeated intermediates, or {@code null} when nothing contributes a value
     */
    public static Object read(
            ProtoFieldMapper fieldMapper, Message message, String path, boolean includeDefaults)
            throws MappingException {
        if (!fansOut(fieldMapper, message, path)) {
            return hasUnsetIntermediate(message, path)
                    ? null
                    : fieldMapper.getValue(message, path, includeDefaults);
        }
        List<Object> out = new ArrayList<>();
        collect(message, path, fieldMapper, includeDefaults, out);
        return out.isEmpty() ? null : out;
    }

    /**
     * As {@link #read}, for whole-value kinds that cannot be split per element — a KNN
     * vector foremost. A path under a repeated ancestor has no flat single-value
     * projection (flattening sibling elements' floats would build a meaningless vector),
     * so it fails loudly here: per-element vectors index as their own entities instead,
     * through a {@link BlockRole#CHUNKS} scope on a block engine or Qdrant's
     * one-point-per-chunk mapping. Paths without a repeated intermediate read exactly
     * like {@link #read}.
     */
    public static Object readWhole(
            ProtoFieldMapper fieldMapper, Message message, String path, boolean includeDefaults)
            throws MappingException {
        if (fansOut(fieldMapper, message, path)) {
            throw new MappingException(
                    "Path '" + path + "' traverses a repeated ancestor, but the field indexes "
                            + "as one whole value; index the elements as their own entities "
                            + "instead (a CHUNKS block scope, or Qdrant's per-chunk points)",
                    path);
        }
        return hasUnsetIntermediate(message, path)
                ? null
                : fieldMapper.getValue(message, path, includeDefaults);
    }

    /**
     * Whether {@code path} reaches a repeated message intermediate on this document,
     * unpacking set Any intermediates along the way. Map fields, Struct boundaries, and
     * anything that does not resolve stay with the field mapper (which reports them in
     * its own vocabulary); an unset singular intermediate reads as missing either way.
     */
    private static boolean fansOut(ProtoFieldMapper fieldMapper, MessageOrBuilder root, String path)
            throws MappingException {
        MessageOrBuilder current = root;
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            if (dot < 0) {
                return false;
            }
            Descriptor descriptor = current.getDescriptorForType();
            if (isStruct(descriptor)) {
                return false;
            }
            FieldDescriptor field = descriptor.findFieldByName(path.substring(start, dot));
            if (field == null
                    || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || field.isMapField()) {
                return false;
            }
            if (field.isRepeated()) {
                return !isAny(field.getMessageType());
            }
            if (!current.hasField(field)) {
                return false;
            }
            MessageOrBuilder next = (MessageOrBuilder) current.getField(field);
            if (isAny(next.getDescriptorForType())) {
                Message unpacked = tryUnpack(fieldMapper, next);
                if (unpacked == null) {
                    return false;
                }
                next = unpacked;
            }
            current = next;
            start = dot + 1;
        }
    }

    /**
     * Depth-first fan-out: iterates repeated message intermediates, descends set singular
     * ones (unpacking Anys), and reads the leaf itself. Struct boundaries hand the
     * remainder to the field mapper. List results (repeated leaves, nested fan-outs) are
     * flattened; unset singular parents contribute nothing.
     */
    private static void collect(
            MessageOrBuilder current,
            String path,
            ProtoFieldMapper fieldMapper,
            boolean includeDefaults,
            List<Object> out) throws MappingException {
        Descriptor descriptor = current.getDescriptorForType();
        if (isStruct(descriptor)) {
            delegate(current, path, fieldMapper, includeDefaults, out);
            return;
        }
        int dot = path.indexOf('.');
        String segment = dot < 0 ? path : path.substring(0, dot);
        FieldDescriptor field = descriptor.findFieldByName(segment);
        if (field == null) {
            throw new MappingException(
                    "Field '" + segment + "' not found on " + descriptor.getFullName(), path);
        }
        if (dot < 0) {
            readLeaf(current, field, fieldMapper, includeDefaults, out);
            return;
        }
        String rest = path.substring(dot + 1);
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE || field.isMapField()) {
            // Struct keys and scalar mis-traversals: the mapper's own semantics and errors.
            delegate(current, path, fieldMapper, includeDefaults, out);
            return;
        }
        if (field.isRepeated()) {
            checkResolvable(field.getMessageType(), rest, path);
            int count = current.getRepeatedFieldCount(field);
            for (int i = 0; i < count; i++) {
                collect((MessageOrBuilder) current.getRepeatedField(field, i),
                        rest, fieldMapper, includeDefaults, out);
            }
            return;
        }
        if (!current.hasField(field)) {
            return;
        }
        MessageOrBuilder next = (MessageOrBuilder) current.getField(field);
        if (isAny(next.getDescriptorForType())) {
            next = unpack(fieldMapper, next, segment, path);
        }
        collect(next, rest, fieldMapper, includeDefaults, out);
    }

    /**
     * Validates that the remainder below a fan-out resolves on the element type, so a bad
     * mapping path fails even when this document has zero elements. The walk stops at Any
     * and Struct boundaries (their content is data-dependent).
     */
    private static void checkResolvable(Descriptor element, String rest, String fullPath)
            throws MappingException {
        Descriptor current = element;
        int start = 0;
        while (true) {
            if (isStruct(current) || isAny(current)) {
                return;
            }
            int dot = rest.indexOf('.', start);
            String segment = dot < 0 ? rest.substring(start) : rest.substring(start, dot);
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null) {
                throw new MappingException(
                        "Field '" + segment + "' not found on " + current.getFullName(), fullPath);
            }
            if (dot < 0) {
                return;
            }
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new MappingException(
                        "Path '" + fullPath + "' attempts to traverse through non-message field '"
                                + segment + "'", fullPath);
            }
            current = field.getMessageType();
            start = dot + 1;
        }
    }

    /**
     * Reads the final segment with the mapper's value semantics — repeated leaves stay
     * lists, Any leaves unpack, implicit-presence defaults honor {@code includeDefaults} —
     * but never its rule-literal parsing, so a field named {@code true} is still a field.
     */
    private static void readLeaf(
            MessageOrBuilder current,
            FieldDescriptor field,
            ProtoFieldMapper fieldMapper,
            boolean includeDefaults,
            List<Object> out) throws MappingException {
        if (field.isRepeated()) {
            out.addAll((List<?>) current.getField(field));
            return;
        }
        if (!current.hasField(field)) {
            if (includeDefaults && !field.hasPresence()) {
                out.add(current.getField(field));
            }
            return;
        }
        Object value = current.getField(field);
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && isAny(field.getMessageType())) {
            out.add(unpack(fieldMapper, (MessageOrBuilder) value, field.getName(), field.getName()));
            return;
        }
        out.add(value);
    }

    private static void delegate(
            MessageOrBuilder current,
            String path,
            ProtoFieldMapper fieldMapper,
            boolean includeDefaults,
            List<Object> out) throws MappingException {
        Object value = fieldMapper.getValue(current, path, includeDefaults);
        if (value instanceof List<?> values) {
            out.addAll(values);
        } else if (value != null) {
            out.add(value);
        }
    }

    private static Message unpack(
            ProtoFieldMapper fieldMapper, MessageOrBuilder value, String segment, String path)
            throws MappingException {
        Any any = AnyIndexing.toAny(value, path);
        try {
            return fieldMapper.getAnyHandler().unpack(any);
        } catch (InvalidProtocolBufferException e) {
            throw new MappingException(
                    "Failed to unpack Any field '" + segment + "' (type url '" + any.getTypeUrl()
                            + "'); register the type's descriptor with the DescriptorRegistry",
                    e,
                    path);
        }
    }

    /** Unpacks an Any value (dynamic representations included), or null when it cannot. */
    private static Message tryUnpack(ProtoFieldMapper fieldMapper, MessageOrBuilder value) {
        try {
            Message message = value instanceof Message.Builder builder
                    ? builder.build()
                    : (Message) value;
            Any any = message instanceof Any typed
                    ? typed
                    : Any.parseFrom(message.toByteString());
            return fieldMapper.getAnyHandler().unpack(any);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * True when a dotted {@code path} traverses a singular message field that is not set,
     * meaning the leaf simply has no value. Anything the walk cannot positively resolve
     * (unknown field, non-message segment, Struct keys, Any unpacking) is left to the
     * field mapper so genuine path errors still surface as {@link MappingException}s.
     */
    private static boolean hasUnsetIntermediate(Message message, String path) {
        MessageOrBuilder current = message;
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            if (dot < 0) {
                return false;
            }
            Descriptor descriptor = current.getDescriptorForType();
            if (isStruct(descriptor)) {
                return false;
            }
            FieldDescriptor field = descriptor.findFieldByName(path.substring(start, dot));
            if (field == null
                    || field.isRepeated()
                    || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                return false;
            }
            if (!current.hasField(field)) {
                return true;
            }
            if (!(current.getField(field) instanceof MessageOrBuilder next)
                    || isAny(next.getDescriptorForType())) {
                return false;
            }
            current = next;
            start = dot + 1;
        }
    }

    private static boolean isStruct(Descriptor descriptor) {
        return Struct.getDescriptor().getFullName().equals(descriptor.getFullName());
    }

    private static boolean isAny(Descriptor descriptor) {
        return Any.getDescriptor().getFullName().equals(descriptor.getFullName());
    }
}

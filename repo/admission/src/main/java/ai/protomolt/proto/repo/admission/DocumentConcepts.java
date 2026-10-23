package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Concept tags (requirements §17, "Subject tags"). A tag is a value of a
 * {@link ConceptRef} field whose field rules bind it to concept schemes
 * ({@code concept_schemes}); a ConceptRef field without that binding is not a tag.
 * <p>
 * A tag's identity is the same field path and ordinal as a link's: field names
 * joined by {@code .}, with {@code [i]} after each repeated ancestor message, and
 * the ordinal is the index inside the tag field itself, 0 when it is singular. A
 * typed {@link Any} is entered without a segment of its own, as for links; an
 * opaque one is not entered. Map values are not traversed: the validator refuses a
 * binding under one, and a tag reached from a map value through a typed Any is
 * refused here. Bindings declared
 * on descriptors rebuilt from retained schema bytes are read through the same rule
 * source the validator uses, so they are never read as absent.
 */
public final class DocumentConcepts {
    /** The full name of the tag value type. */
    public static final String CONCEPT_REF_TYPE = ConceptRef.getDescriptor().getFullName();
    private static final ProtomoltRuleSource RULES = new ProtomoltRuleSource();

    private DocumentConcepts() {}

    /**
     * One concept tag found in a message.
     *
     * @param fieldPath descriptor path of the tag field from the walked root, with the
     *        index of every enclosing repeated element
     * @param ordinal element index in a repeated tag field; 0 for a singular one
     * @param repeated whether the tag field is repeated
     * @param schemeId the tagged concept's scheme
     * @param conceptId the tagged concept's id
     */
    public record Tag(String fieldPath, int ordinal, boolean repeated, String schemeId, String conceptId) {
        public Tag {
            Objects.requireNonNull(fieldPath); Objects.requireNonNull(schemeId); Objects.requireNonNull(conceptId);
            if (fieldPath.isEmpty() || ordinal < 0 || (!repeated && ordinal != 0))
                throw new IllegalArgumentException("A tag needs a field path and an ordinal");
        }

        /** The tag's place as a violation path names it: the field path, with [ordinal] for a repeated field. */
        public String at() { return repeated ? fieldPath + "[" + ordinal + "]" : fieldPath; }
    }

    /** Content whose concept tags the repository refuses; the message names the field. */
    public static class TagRefused extends IllegalArgumentException {
        TagRefused(String field, String message) {
            super(field + ": " + message);
        }
    }

    /** A tag reached from a map value through a typed Any; names the map field. */
    public static final class TagUnderMap extends TagRefused {
        TagUnderMap(String map) {
            super(map, "a concept tag cannot sit under a map value");
        }
    }

    /**
     * Whether a field is a tag field: a singular or repeated ConceptRef bound to
     * at least one scheme.
     *
     * @param field any field
     * @return whether its values are tags
     */
    public static boolean tagField(FieldDescriptor field) {
        if (field.isMapField() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || !field.getMessageType().getFullName().equals(CONCEPT_REF_TYPE))
            return false;
        return RULES.fieldConstraints(field).map(rules -> !rules.conceptSchemes().isEmpty()).orElse(false);
    }

    /**
     * Whether any message reachable from {@code type}, map values included and
     * {@link Any} values excluded, declares a tag field.
     *
     * @param type the root message type
     * @return whether a tag field is reachable
     */
    public static boolean declaresTags(Descriptor type) {
        var seen = new HashSet<String>();
        var pending = new ArrayDeque<Descriptor>();
        pending.add(type);
        while (!pending.isEmpty()) {
            var next = pending.removeFirst();
            if (next.getFullName().equals(Any.getDescriptor().getFullName()) || !seen.add(next.getFullName())) continue;
            for (var field : next.getFields()) {
                if (tagField(field)) return true;
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) pending.add(field.getMessageType());
            }
        }
        return false;
    }

    /**
     * The tags in a message, in field number order and then element order, without
     * entering any {@link Any} value. Map values are not entered.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @return the tags found
     */
    public static List<Tag> extract(Message message, String prefix) {
        return extract(message, prefix, List.of(), (at, typeUrl, value) -> Optional.empty());
    }

    /**
     * The tags in a message, entering each typed nested {@link Any} the unpacker
     * decodes without a path segment of its own, as the link walk does; a repeated
     * Any takes {@code [i]} and an opaque one is not entered.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @param at the message's occurrence path, the steps admission recorded down to it
     * @param unpacker decodes nested typed Any values by occurrence, with their admitted schemas
     * @return the tags found
     * @throws TagRefused when a map value reaches a tag through a typed Any, or a tag
     *         value is not a valid canonical ConceptRef
     */
    static List<Tag> extract(Message message, String prefix, List<DocumentSchemaOccurrences.Step> at,
            DocumentLinks.Unpacker unpacker) {
        Objects.requireNonNull(message); Objects.requireNonNull(prefix); Objects.requireNonNull(unpacker);
        var found = new ArrayList<Tag>();
        collect(message, prefix, new ArrayList<>(at), unpacker, found);
        return List.copyOf(found);
    }

    // The occurrence steps follow the link walk, which follows DocumentPayloadCheck's
    // evidence walk: a field step for each message field, then an index or map-key step
    // for each element, and the Any's boundary before its payload.
    private static void collect(Message message, String prefix, ArrayList<DocumentSchemaOccurrences.Step> at,
            DocumentLinks.Unpacker unpacker, List<Tag> found) {
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.isMapField()) {
                requireNoTagUnderMap(field, field, (List<?>) entry.getValue(), at, unpacker);
                continue;
            }
            var path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            boolean ref = field.getMessageType().getFullName().equals(CONCEPT_REF_TYPE);
            // An unbound ConceptRef (a mapping target, a filter) is not a tag and holds no message field.
            if (ref && !tagField(field)) continue;
            var values = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
            for (int i = 0; i < values.size(); i++) {
                var value = (Message) values.get(i);
                var element = field.isRepeated() ? path + "[" + i + "]" : path;
                if (field.isRepeated()) at.add(new DocumentSchemaOccurrences.Index(i));
                if (ref) found.add(tag(field, path, field.isRepeated() ? i : 0, value));
                else if (isAny(field.getMessageType())) unpacked(value, at, unpacker).ifPresent(entered -> {
                    at.add(entered.boundary());
                    collect(entered.payload(), element, at, unpacker, found);
                    at.removeLast();
                });
                else collect(value, element, at, unpacker, found);
                if (field.isRepeated()) at.removeLast();
            }
            at.removeLast();
        }
    }

    private static boolean isAny(Descriptor type) {
        return type.getFullName().equals(Any.getDescriptor().getFullName());
    }

    /** The typed payload of the Any at this occurrence path; empty when it is opaque. */
    private static Optional<DocumentLinks.Entered> unpacked(Message any, List<DocumentSchemaOccurrences.Step> at,
            DocumentLinks.Unpacker unpacker) {
        var type = any.getDescriptorForType();
        return unpacker.unpack(List.copyOf(at), (String) any.getField(type.findFieldByName("type_url")),
                (ByteString) any.getField(type.findFieldByName("value")));
    }

    /** Refuses a tag reached from the values of a map field through a typed Any. */
    private static void requireNoTagUnderMap(FieldDescriptor map, FieldDescriptor field, List<?> entries,
            ArrayList<DocumentSchemaOccurrences.Step> at, DocumentLinks.Unpacker unpacker) {
        var keyField = field.getMessageType().findFieldByNumber(1);
        var valueField = field.getMessageType().findFieldByName("value");
        if (valueField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) return;
        at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
        for (var element : entries) {
            var entry = (Message) element;
            at.add(new DocumentSchemaOccurrences.MapKey(keyField.getType(), entry.getField(keyField)));
            requireNoTagUnder(map, (Message) entry.getField(valueField), at, unpacker);
            at.removeLast();
        }
        at.removeLast();
    }

    /** Refuses a tag reached from a map value through a typed Any. */
    private static void requireNoTagUnder(FieldDescriptor map, Message value, ArrayList<DocumentSchemaOccurrences.Step> at,
            DocumentLinks.Unpacker unpacker) {
        if (isAny(value.getDescriptorForType())) {
            var entered = unpacked(value, at, unpacker);
            if (entered.isEmpty()) return;
            if (declaresTags(entered.get().payload().getDescriptorForType())) throw new TagUnderMap(map.getFullName());
            at.add(entered.get().boundary());
            requireNoTagUnder(map, entered.get().payload(), at, unpacker);
            at.removeLast();
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.isMapField()) {
                requireNoTagUnderMap(map, field, (List<?>) entry.getValue(), at, unpacker);
                continue;
            }
            var elements = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
            for (int i = 0; i < elements.size(); i++) {
                if (field.isRepeated()) at.add(new DocumentSchemaOccurrences.Index(i));
                requireNoTagUnder(map, (Message) elements.get(i), at, unpacker);
                if (field.isRepeated()) at.removeLast();
            }
            at.removeLast();
        }
    }

    /**
     * Reads a tag value as the canonical {@link ConceptRef}, never by the field names of
     * whatever type the payload schema declares under that full name: a retained schema
     * is self-contained and can redefine ConceptRef without its rules or with other field
     * types. The value is re-parsed into the generated type and held to its own rules, so
     * a forged ConceptRef is refused as content before anything is projected.
     */
    private static Tag tag(FieldDescriptor field, String path, int ordinal, Message value) {
        final ConceptRef ref;
        if (value instanceof ConceptRef typed) {
            ref = typed;
        } else {
            try {
                ref = ConceptRef.parseFrom(value.toByteString());
            } catch (InvalidProtocolBufferException unreadable) {
                throw new TagRefused(field.getFullName(), "the tag value does not decode as " + CONCEPT_REF_TYPE);
            }
        }
        if (!ref.getUnknownFields().asMap().isEmpty())
            throw new TagRefused(field.getFullName(), "the tag value carries fields " + CONCEPT_REF_TYPE + " does not declare");
        var checked = DocumentSchemaAdmission.VALIDATOR.validate(ref);
        if (!checked.valid())
            throw new TagRefused(field.getFullName(), "the tag value fails " + checked.violations().getFirst().ruleId()
                    + " at " + checked.violations().getFirst().path());
        return new Tag(path, ordinal, field.isRepeated(), ref.getSchemeId(), ref.getConceptId());
    }
}

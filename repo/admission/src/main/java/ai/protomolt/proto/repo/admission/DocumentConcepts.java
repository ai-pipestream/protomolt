package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Concept tags (requirements §17, "Subject tags"). A tag is a value of a
 * {@link ConceptRef} field whose field rules bind it to concept schemes
 * ({@code concept_schemes}); a ConceptRef field without that binding is not a tag.
 * <p>
 * A tag's identity is the same field path and ordinal as a link's: field names
 * joined by {@code .}, with {@code [i]} after each repeated ancestor message, and
 * the ordinal is the index inside the tag field itself, 0 when it is singular. Map
 * values are not traversed (the validator refuses a binding under one), and an
 * {@link Any} value is a schema boundary the walk does not cross. Bindings declared
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
     * The tags in a message, in field number order and then element order.
     * {@link Any} values and map values are not entered.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @return the tags found
     */
    public static List<Tag> extract(Message message, String prefix) {
        Objects.requireNonNull(message); Objects.requireNonNull(prefix);
        var found = new ArrayList<Tag>();
        collect(message, prefix, found);
        return List.copyOf(found);
    }

    private static void collect(Message message, String prefix, List<Tag> found) {
        if (message.getDescriptorForType().getFullName().equals(Any.getDescriptor().getFullName())) return;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE || field.isMapField()) continue;
            var path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            boolean ref = field.getMessageType().getFullName().equals(CONCEPT_REF_TYPE);
            // An unbound ConceptRef (a mapping target, a filter) is not a tag and holds no message field.
            if (ref && !tagField(field)) continue;
            if (field.isRepeated()) {
                var values = (List<?>) entry.getValue();
                for (int i = 0; i < values.size(); i++) {
                    if (ref) found.add(tag(path, i, true, (Message) values.get(i)));
                    else collect((Message) values.get(i), path + "[" + i + "]", found);
                }
            } else if (ref) {
                found.add(tag(path, 0, false, (Message) entry.getValue()));
            } else {
                collect((Message) entry.getValue(), path, found);
            }
        }
    }

    private static Tag tag(String path, int ordinal, boolean repeated, Message value) {
        var type = value.getDescriptorForType();
        return new Tag(path, ordinal, repeated, (String) value.getField(type.findFieldByName("scheme_id")),
                (String) value.getField(type.findFieldByName("concept_id")));
    }
}

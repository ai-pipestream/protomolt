package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.WeakHashMap;

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
    private static final Map<FieldDescriptor, Boolean> TAG_FIELDS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<Descriptor, Boolean> DECLARES_TAGS =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<Descriptor, Boolean> CANONICAL_REFS =
            Collections.synchronizedMap(new WeakHashMap<>());

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
        Objects.requireNonNull(field);
        if (field.isMapField() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || !field.getMessageType().getFullName().equals(CONCEPT_REF_TYPE))
            return false;
        var cached = TAG_FIELDS.get(field);
        if (cached != null) return cached;
        boolean bound = RULES.fieldConstraints(field).map(rules -> !rules.conceptSchemes().isEmpty()).orElse(false);
        TAG_FIELDS.put(field, bound);
        return bound;
    }

    /**
     * Whether any message reachable from {@code type}, map values included and
     * {@link Any} values excluded, declares a tag field.
     *
     * @param type the root message type
     * @return whether a tag field is reachable
     */
    public static boolean declaresTags(Descriptor type) {
        Objects.requireNonNull(type);
        var cached = DECLARES_TAGS.get(type);
        if (cached != null) return cached;
        boolean declares = check(type, new SchemaChecks(() -> {}));
        DECLARES_TAGS.put(type, declares);
        return declares;
    }

    private static boolean check(Descriptor root, SchemaChecks checks) {
        var control = checks.control();
        var types = new java.util.LinkedHashMap<String, Descriptor>();
        var pending = new ArrayDeque<Descriptor>();
        pending.add(root);
        while (!pending.isEmpty()) {
            control.run();
            var type = pending.removeFirst();
            if (type.getFullName().equals(Any.getDescriptor().getFullName())
                    || types.putIfAbsent(type.getFullName(), type) != null)
                continue;
            for (var field : type.getFields()) {
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !checks.tagField(field))
                    pending.add(field.getMessageType());
            }
        }
        var holders = new java.util.HashMap<String, List<String>>();
        var reaches = new HashSet<String>();
        var frontier = new ArrayDeque<String>();
        for (var type : types.values()) {
            control.run();
            for (var field : type.getFields()) {
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                if (checks.tagField(field)) {
                    if (reaches.add(type.getFullName())) frontier.add(type.getFullName());
                } else {
                    holders.computeIfAbsent(field.getMessageType().getFullName(), name -> new ArrayList<>())
                            .add(type.getFullName());
                }
            }
        }
        while (!frontier.isEmpty()) {
            control.run();
            for (var holder : holders.getOrDefault(frontier.removeFirst(), List.of()))
                if (reaches.add(holder)) frontier.add(holder);
        }
        for (var type : types.values()) {
            boolean declares = reaches.contains(type.getFullName());
            checks.checked.put(type, declares);
            DECLARES_TAGS.put(type, declares);
        }
        return reaches.contains(root.getFullName());
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
        return extract(message, prefix, () -> {});
    }

    /**
     * The tags in a message, in field number order and then element order, without
     * entering any {@link Any} value, running {@code control} at every walk step so a
     * deadline or cancellation stops a large message.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @param control run at every walk step
     * @return the tags found
     */
    public static List<Tag> extract(Message message, String prefix, Runnable control) {
        return extract(message, prefix, List.of(), DocumentLinks.NONE, new SchemaChecks(control));
    }

    /**
     * The tags in a message without entering any {@link Any} value, reusing the
     * admission's {@link SchemaChecks}.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @param checks the tag-schema checks of this admission, and the control the walk runs
     * @return the tags found
     */
    static List<Tag> extract(Message message, String prefix, SchemaChecks checks) {
        return extract(message, prefix, List.of(), DocumentLinks.NONE, checks);
    }

    /**
     * The tags in a message, entering each typed nested {@link Any} the unpacker
     * decodes without a path segment of its own, as the link walk does; a repeated
     * Any takes {@code [i]}, an Any whose payload is itself an Any is unwrapped the same
     * way, and an opaque one is not entered.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @param at the message's occurrence path, the steps admission recorded down to it
     * @param unpacker decodes nested typed Any values by occurrence, with their admitted schemas
     * @param control run at every walk step, so a deadline or cancellation stops a large message
     * @return the tags found
     * @throws TagRefused when a map value reaches a tag through a typed Any, or a tag
     *         value is not a valid canonical ConceptRef
     */
    static List<Tag> extract(Message message, String prefix, List<DocumentSchemaOccurrences.Step> at,
            DocumentLinks.Unpacker unpacker, Runnable control) {
        return extract(message, prefix, at, unpacker, new SchemaChecks(control));
    }

    /**
     * The tags in a message, entering each typed nested {@link Any} the unpacker
     * decodes without a path segment of its own, reusing the admission's
     * {@link SchemaChecks}.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @param at the message's occurrence path, the steps admission recorded down to it
     * @param unpacker decodes nested typed Any values by occurrence, with their admitted schemas
     * @param checks the tag-schema checks of this admission, and the control the walk runs
     * @return the tags found
     * @throws TagRefused when a map value reaches a tag through a typed Any, or a tag
     *         value is not a valid canonical ConceptRef
     */
    static List<Tag> extract(Message message, String prefix, List<DocumentSchemaOccurrences.Step> at,
            DocumentLinks.Unpacker unpacker, SchemaChecks checks) {
        Objects.requireNonNull(message); Objects.requireNonNull(prefix); Objects.requireNonNull(at);
        var walk = new Walk(Objects.requireNonNull(unpacker), Objects.requireNonNull(checks));
        var found = new ArrayList<Tag>();
        collect(message, prefix, new ArrayList<>(at), walk, found);
        return List.copyOf(found);
    }

    /**
     * The tag-schema checks of one admission. Each type and field is checked on first
     * sight and remembered by identity, and the control runs inside every check and walk
     * step so a deadline or cancellation stops a large graph.
     */
    static final class SchemaChecks {
        private final Runnable control;
        private final Map<Descriptor, Boolean> checked = new IdentityHashMap<>();
        private final Map<FieldDescriptor, Boolean> fields = new IdentityHashMap<>();

        SchemaChecks(Runnable control) { this.control = Objects.requireNonNull(control); }

        boolean declares(Descriptor type) {
            var known = checked.get(type);
            if (known != null) return known;
            boolean declares = check(type, this);
            checked.put(type, declares);
            return declares;
        }

        boolean tagField(FieldDescriptor field) {
            var known = fields.get(field);
            if (known != null) return known;
            boolean bound = DocumentConcepts.tagField(field);
            fields.put(field, bound);
            return bound;
        }

        Runnable control() { return control; }
    }

    // The occurrence steps follow the link walk, which follows DocumentPayloadCheck's
    // evidence walk: a field step for each message field, then an index or map-key step
    // for each element, and the Any's boundary before its payload.
    /** One extraction: the unpacker and the admission's schema checks. */
    private record Walk(DocumentLinks.Unpacker unpacker, SchemaChecks checks) {
        boolean declares(Descriptor type) { return checks.declares(type); }

        boolean tagField(FieldDescriptor field) { return checks.tagField(field); }

        Runnable control() { return checks.control(); }
    }

    private static void collect(Message message, String prefix, ArrayList<DocumentSchemaOccurrences.Step> at,
            Walk walk, List<Tag> found) {
        walk.control().run();
        if (isAny(message.getDescriptorForType())) {
            // A typed Any adds its boundary step and no path segment; an opaque one is not entered.
            unpacked(message, at, walk).ifPresent(entered -> {
                at.add(entered.boundary());
                collect(entered.payload(), prefix, at, walk, found);
                at.removeLast();
            });
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.isMapField()) {
                requireNoTagUnderMap(field, field, (List<?>) entry.getValue(), at, walk);
                continue;
            }
            var path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            boolean ref = field.getMessageType().getFullName().equals(CONCEPT_REF_TYPE);
            // An unbound ConceptRef (a mapping target, a filter) is not a tag and holds no message field.
            if (ref && !walk.tagField(field)) continue;
            var values = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
            for (int i = 0; i < values.size(); i++) {
                var value = (Message) values.get(i);
                if (field.isRepeated()) at.add(new DocumentSchemaOccurrences.Index(i));
                if (ref) {
                    walk.control().run();
                    found.add(tag(field, path, field.isRepeated() ? i : 0, value));
                } else collect(value, field.isRepeated() ? path + "[" + i + "]" : path, at, walk, found);
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
            Walk walk) {
        var type = any.getDescriptorForType();
        return walk.unpacker().unpack(List.copyOf(at), (String) any.getField(type.findFieldByName("type_url")),
                (ByteString) any.getField(type.findFieldByName("value")));
    }

    /** Refuses a tag reached from the values of a map field through a typed Any. */
    private static void requireNoTagUnderMap(FieldDescriptor map, FieldDescriptor field, List<?> entries,
            ArrayList<DocumentSchemaOccurrences.Step> at, Walk walk) {
        var keyField = field.getMessageType().findFieldByNumber(1);
        var valueField = field.getMessageType().findFieldByName("value");
        if (valueField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) return;
        at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
        for (var element : entries) {
            var entry = (Message) element;
            at.add(new DocumentSchemaOccurrences.MapKey(keyField.getType(), entry.getField(keyField)));
            requireNoTagUnder(map, (Message) entry.getField(valueField), at, walk);
            at.removeLast();
        }
        at.removeLast();
    }

    /** Refuses a tag reached from a map value through a typed Any. */
    private static void requireNoTagUnder(FieldDescriptor map, Message value, ArrayList<DocumentSchemaOccurrences.Step> at,
            Walk walk) {
        walk.control().run();
        if (isAny(value.getDescriptorForType())) {
            var entered = unpacked(value, at, walk);
            if (entered.isEmpty()) return;
            if (walk.declares(entered.get().payload().getDescriptorForType())) throw new TagUnderMap(map.getFullName());
            at.add(entered.get().boundary());
            requireNoTagUnder(map, entered.get().payload(), at, walk);
            at.removeLast();
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.isMapField()) {
                requireNoTagUnderMap(map, field, (List<?>) entry.getValue(), at, walk);
                continue;
            }
            var elements = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
            for (int i = 0; i < elements.size(); i++) {
                if (field.isRepeated()) at.add(new DocumentSchemaOccurrences.Index(i));
                requireNoTagUnder(map, (Message) elements.get(i), at, walk);
                if (field.isRepeated()) at.removeLast();
            }
            at.removeLast();
        }
    }

    /**
     * Reads a tag value as the canonical {@link ConceptRef}, never by the field names of
     * whatever type the payload schema declares under that full name: a retained schema
     * is self-contained and can redefine ConceptRef without its rules or with other field
     * types. The value is converted into the generated type and held to its own rules, so
     * a forged ConceptRef is refused as content before anything is projected.
     */
    private static Tag tag(FieldDescriptor field, String path, int ordinal, Message value) {
        final ConceptRef ref;
        if (value instanceof ConceptRef typed) {
            if (!typed.getUnknownFields().asMap().isEmpty())
                throw new TagRefused(field.getFullName(), "the tag value carries fields " + CONCEPT_REF_TYPE + " does not declare");
            ref = typed;
        } else {
            var type = value.getDescriptorForType();
            if (!canonical(type))
                throw new TagRefused(field.getFullName(), "the tag type is not " + CONCEPT_REF_TYPE
                        + " (string scheme_id = 1, string concept_id = 2, no other fields)");
            if (!value.getUnknownFields().asMap().isEmpty())
                throw new TagRefused(field.getFullName(), "the tag value carries fields " + CONCEPT_REF_TYPE + " does not declare");
            String schemeId = (String) value.getField(type.findFieldByNumber(1));
            String conceptId = (String) value.getField(type.findFieldByNumber(2));
            ref = ConceptRef.newBuilder().setSchemeId(schemeId).setConceptId(conceptId).build();
        }
        var checked = DocumentSchemaAdmission.VALIDATOR.validate(ref);
        if (!checked.valid())
            throw new TagRefused(field.getFullName(), "the tag value fails " + checked.violations().getFirst().ruleId()
                    + " at " + checked.violations().getFirst().path());
        return new Tag(path, ordinal, field.isRepeated(), ref.getSchemeId(), ref.getConceptId());
    }

    /** Whether a type declared under ConceptRef's name has the canonical fields, and only those. */
    private static boolean canonical(Descriptor type) {
        if (type == ConceptRef.getDescriptor()) return true;
        var cached = CANONICAL_REFS.get(type);
        if (cached != null) return cached;
        var expected = ConceptRef.getDescriptor();
        boolean matches = checkCanonical(type, expected);
        CANONICAL_REFS.put(type, matches);
        return matches;
    }

    private static boolean checkCanonical(Descriptor type, Descriptor expected) {
        if (type.getFields().size() != expected.getFields().size()) return false;
        for (var want : expected.getFields()) {
            var have = type.findFieldByNumber(want.getNumber());
            if (have == null || !have.getName().equals(want.getName()) || have.getType() != want.getType()
                    || have.isRepeated() != want.isRepeated() || have.hasDefaultValue()
                    || have.getRealContainingOneof() != null)
                return false;
        }
        return true;
    }
}

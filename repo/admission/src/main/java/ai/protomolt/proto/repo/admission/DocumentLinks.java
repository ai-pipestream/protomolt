package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Link declarations and link values (requirements §17). A link is a
 * {@link DocumentLink} field whose descriptor carries the
 * {@code (ai.protomolt.proto.repo.v2.link)} option; the option names the
 * relation and the value names the target.
 * <p>
 * The schema rules hold for every message reachable from a type, through
 * singular, repeated and map-valued message fields; an {@link Any} type is a
 * schema boundary the descriptor walk does not cross, and each typed Any value is
 * checked against its own schema when the value walk enters it. A {@code DocumentLink} field
 * without the option, the option on a field of any other type, a link field
 * anywhere under a map value, and an option whose own rules fail are each a
 * {@link SchemaRefused}.
 * <p>
 * A link's identity is its field path and ordinal: field names joined by
 * {@code .}, with {@code [i]} after each repeated ancestor message (for example
 * {@code sections[0].cites}), and the ordinal is the index inside the link field
 * itself, 0 when it is singular. A typed Any is entered without a segment of its
 * own; an opaque one is not entered. Map values are not traversed, which is why no
 * link may sit under one, directly or through an Any. Descriptors built from retained schema bytes carry the
 * option as an unknown field, so options are reparsed against a registry that
 * knows it rather than read as absent.
 */
public final class DocumentLinks {
    /** The full name of the link value type. */
    public static final String LINK_TYPE = DocumentLink.getDescriptor().getFullName();
    private static final ExtensionRegistry EXTENSIONS = registry();

    private DocumentLinks() {}

    private static ExtensionRegistry registry() {
        var registry = ExtensionRegistry.newInstance();
        LinksProto.registerAllExtensions(registry);
        return registry;
    }

    /**
     * One link value found in a message.
     *
     * @param fieldPath descriptor path of the link field from the walked root, with
     *        the index or key of every enclosing repeated or map element
     * @param ordinal element index in a repeated link field; 0 for a singular one
     * @param repeated whether the link field is repeated
     * @param field the field's link declaration
     * @param link the link value
     */
    public record Link(String fieldPath, int ordinal, boolean repeated, LinkField field, DocumentLink link) {
        public Link {
            Objects.requireNonNull(fieldPath); Objects.requireNonNull(field); Objects.requireNonNull(link);
            if (fieldPath.isEmpty() || ordinal < 0 || (!repeated && ordinal != 0))
                throw new IllegalArgumentException("A link needs a field path and an ordinal");
        }

        /** The link's place as a violation path names it: the field path, with [ordinal] for a repeated field. */
        public String at() { return repeated ? fieldPath + "[" + ordinal + "]" : fieldPath; }
    }

    /** A schema that breaks the link declaration rules; names the field and the rule. */
    public static final class SchemaRefused extends IllegalArgumentException {
        private final String field;

        SchemaRefused(String field, String message) {
            super(field + ": " + message);
            this.field = field;
        }

        /** The full name of the offending field. */
        public String field() { return field; }
    }

    /**
     * The link declaration on a field, validated.
     *
     * @param field any field
     * @return the declaration, or empty when the field carries no link option
     * @throws SchemaRefused when the option does not parse or fails its own rules
     */
    public static Optional<LinkField> linkField(FieldDescriptor field) {
        var options = field.getOptions();
        LinkField declared;
        if (options.hasExtension(LinksProto.link)) {
            declared = options.getExtension(LinksProto.link);
        } else if (options.getUnknownFields().hasField(LinksProto.link.getNumber())) {
            try {
                declared = FieldOptions.parseFrom(options.toByteString(), EXTENSIONS).getExtension(LinksProto.link);
            } catch (InvalidProtocolBufferException unreadable) {
                throw new SchemaRefused(field.getFullName(), "the link option does not parse: " + unreadable.getMessage());
            }
        } else {
            return Optional.empty();
        }
        if (!declared.getUnknownFields().asMap().isEmpty())
            throw new SchemaRefused(field.getFullName(), "the link option carries fields this server does not know");
        var checked = DocumentSchemaAdmission.VALIDATOR.validate(declared);
        if (!checked.valid())
            throw new SchemaRefused(field.getFullName(), "the link option fails "
                    + checked.violations().getFirst().ruleId() + " at " + checked.violations().getFirst().path());
        return Optional.of(declared);
    }

    /**
     * Checks the link declarations of every message reachable from {@code type}.
     *
     * @param type the root message type
     * @throws SchemaRefused at the first field that breaks a rule
     */
    public static void requireLinkSchema(Descriptor type) {
        check(type, () -> {});
    }

    /**
     * Whether any message reachable from {@code type} declares a link field. The
     * declarations are checked on the way.
     *
     * @param type the root message type
     * @return whether a link field is reachable
     * @throws SchemaRefused at the first field that breaks a rule
     */
    public static boolean declaresLinks(Descriptor type) {
        return check(type, () -> {});
    }

    /**
     * Checks every field of every message reachable from {@code root}, map values
     * included, then refuses a map whose value reaches a link. Reachability runs
     * backwards from the link fields over the reversed type graph, so each type and
     * field is visited a bounded number of times and recursive types terminate. The
     * control runs once per type in each pass, so a deadline can stop a large graph.
     */
    private static boolean check(Descriptor root, Runnable control) {
        var types = new LinkedHashMap<String, Descriptor>();
        var pending = new ArrayDeque<Descriptor>();
        pending.add(root);
        while (!pending.isEmpty()) {
            control.run();
            var type = pending.removeFirst();
            if (type.getFullName().equals(Any.getDescriptor().getFullName()) || types.putIfAbsent(type.getFullName(), type) != null)
                continue;
            for (var field : type.getFields()) {
                var declared = linkField(field);
                if (field.isMapField()) {
                    if (declared.isPresent()) throw new SchemaRefused(field.getFullName(), "a map field cannot be a link");
                    var value = mapValue(field);
                    if (value != null) pending.add(value);
                    continue;
                }
                boolean link = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                        && field.getMessageType().getFullName().equals(LINK_TYPE);
                if (link && declared.isEmpty())
                    throw new SchemaRefused(field.getFullName(), "a " + LINK_TYPE + " field needs the link option");
                if (!link && declared.isPresent())
                    throw new SchemaRefused(field.getFullName(), "the link option needs a " + LINK_TYPE + " field");
                if (!link && field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) pending.add(field.getMessageType());
            }
        }
        // A type reaches a link when it holds a link field or a field whose type reaches
        // one: seed with the holders of link fields, then walk the reversed edges.
        var holders = new HashMap<String, List<String>>();
        var reaches = new HashSet<String>();
        var frontier = new ArrayDeque<String>();
        for (var type : types.values()) {
            control.run();
            for (var field : type.getFields()) {
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                var target = field.isMapField() ? mapValue(field) : field.getMessageType();
                if (target == null) continue;
                if (target.getFullName().equals(LINK_TYPE)) {
                    if (reaches.add(type.getFullName())) frontier.add(type.getFullName());
                } else {
                    holders.computeIfAbsent(target.getFullName(), name -> new ArrayList<>()).add(type.getFullName());
                }
            }
        }
        while (!frontier.isEmpty()) {
            control.run();
            for (var holder : holders.getOrDefault(frontier.removeFirst(), List.of()))
                if (reaches.add(holder)) frontier.add(holder);
        }
        for (var type : types.values()) {
            control.run();
            for (var field : type.getFields()) {
                if (!field.isMapField()) continue;
                var value = mapValue(field);
                if (value != null && (value.getFullName().equals(LINK_TYPE) || reaches.contains(value.getFullName())))
                    throw new SchemaRefused(field.getFullName(), "a link cannot sit under a map value");
            }
        }
        return reaches.contains(root.getFullName());
    }

    /** A map field's value message type; null for a scalar value. */
    private static Descriptor mapValue(FieldDescriptor field) {
        var value = field.getMessageType().findFieldByName("value");
        return value.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? value.getMessageType() : null;
    }

    /**
     * Decodes one nested {@link Any} for the link walk with the exact schema admission
     * resolved for that occurrence. Opacity and schema belong to an occurrence, not to
     * its bytes, so the Any is named by its location as well as its contents.
     */
    @FunctionalInterface interface Unpacker {
        /**
         * @param at the Any's occurrence path: the steps from the root boundary to the Any,
         *         without its own boundary
         * @param typeUrl the Any's type URL
         * @param value the Any's value bytes
         * @return the decoded payload and its boundary step, or empty when the occurrence is
         *         opaque and was not decoded
         */
        Optional<Entered> unpack(List<DocumentSchemaOccurrences.Step> at, String typeUrl, ByteString value);
    }

    /** A nested Any admission decoded: its payload and the boundary step that ends its occurrence path. */
    record Entered(Message payload, DocumentSchemaOccurrences.Boundary boundary) {
        Entered { Objects.requireNonNull(payload); Objects.requireNonNull(boundary); }
    }

    /** Enters no Any: for a CORE fragment, whose Any roots admission walks on their own. */
    private static final Unpacker NONE = (at, typeUrl, value) -> Optional.empty();

    /**
     * The links in a message, in field number order and then element order, without
     * entering any {@link Any} value. The message's type must satisfy
     * {@link #requireLinkSchema}.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @return the links found
     * @throws SchemaRefused when a declaration breaks a rule or a link value
     *         carries fields this server's {@link DocumentLink} does not know
     */
    public static List<Link> extract(Message message, String prefix) {
        return extract(message, prefix, List.of(), NONE, new SchemaChecks(() -> {}));
    }

    /**
     * The links in a message, entering each typed nested {@link Any} the unpacker
     * decodes without a path segment of its own (requirements §17): the path goes on
     * with the packed message's field names, and a repeated Any takes {@code [i]}. An
     * opaque Any is not entered. An Any whose payload is itself an Any is unwrapped
     * the same way, as the payload check does. Map values are not entered either, and
     * a link a map value reaches through an Any is refused as one under the map would be.
     * Each payload type's link schema is checked once per {@link SchemaChecks}.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @param at the message's occurrence path, the steps admission recorded down to it
     * @param unpacker decodes nested typed Any values by occurrence
     * @param checks the link-schema checks of this admission, and the control the walk runs
     * @return the links found
     * @throws SchemaRefused when a declaration breaks a rule or a link value
     *         carries fields this server's {@link DocumentLink} does not know
     */
    static List<Link> extract(Message message, String prefix, List<DocumentSchemaOccurrences.Step> at, Unpacker unpacker,
            SchemaChecks checks) {
        Objects.requireNonNull(message); Objects.requireNonNull(prefix); Objects.requireNonNull(at);
        var walk = new Walk(Objects.requireNonNull(unpacker), Objects.requireNonNull(checks));
        var found = new ArrayList<Link>();
        collect(message, prefix, new ArrayList<>(at), walk, found);
        return List.copyOf(found);
    }

    /**
     * The link-schema checks of one admission. Each type is checked on first sight and
     * remembered by identity, so the many Any values and roots of one candidate cost one
     * check per distinct schema. The control runs inside every check and walk step, so a
     * deadline or cancellation stops a large graph.
     */
    static final class SchemaChecks {
        private final Runnable control;
        private final Map<Descriptor, Boolean> checked = new IdentityHashMap<>();

        SchemaChecks(Runnable control) { this.control = Objects.requireNonNull(control); }

        /**
         * Whether a type reaches a link field, its declarations checked.
         *
         * @throws SchemaRefused at the first field that breaks a rule
         */
        boolean declares(Descriptor type) {
            var known = checked.get(type);
            if (known != null) return known;
            boolean declares = check(type, control);
            checked.put(type, declares);
            return declares;
        }

        Runnable control() { return control; }
    }

    /** One extraction: the unpacker and the admission's schema checks. */
    private record Walk(Unpacker unpacker, SchemaChecks checks) {
        boolean declares(Descriptor type) { return checks.declares(type); }

        Runnable control() { return checks.control(); }
    }

    private static boolean isAny(Descriptor type) {
        return type.getFullName().equals(Any.getDescriptor().getFullName());
    }

    // The occurrence steps follow DocumentPayloadCheck's evidence walk: a field step for
    // each message field, then an index or map-key step for each element, with no field
    // step for a map entry's value, and the Any's boundary before its payload.
    private static void collect(Message message, String prefix, ArrayList<DocumentSchemaOccurrences.Step> at,
            Walk walk, List<Link> found) {
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
            var path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            if (field.isMapField()) {
                // Map values are not entered; the schema rules keep links out of them, and an
                // Any under a map value must not bring one in.
                requireNoLinkUnderMap(field, field, (List<?>) entry.getValue(), at, walk);
                continue;
            }
            var declared = linkField(field);
            boolean link = field.getMessageType().getFullName().equals(LINK_TYPE);
            if (link != declared.isPresent())
                throw new SchemaRefused(field.getFullName(), declared.isPresent()
                        ? "the link option needs a " + LINK_TYPE + " field" : "a " + LINK_TYPE + " field needs the link option");
            var values = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
            for (int i = 0; i < values.size(); i++) {
                var value = (Message) values.get(i);
                if (field.isRepeated()) at.add(new DocumentSchemaOccurrences.Index(i));
                if (link) found.add(new Link(path, field.isRepeated() ? i : 0, field.isRepeated(), declared.get(), value(field, value)));
                else collect(value, field.isRepeated() ? path + "[" + i + "]" : path, at, walk, found);
                if (field.isRepeated()) at.removeLast();
            }
            at.removeLast();
        }
    }

    /** The typed payload of the Any at this occurrence path, its link declarations checked; empty when it is opaque. */
    private static Optional<Entered> unpacked(Message any, List<DocumentSchemaOccurrences.Step> at, Walk walk) {
        var type = any.getDescriptorForType();
        var entered = walk.unpacker().unpack(List.copyOf(at), (String) any.getField(type.findFieldByName("type_url")),
                (ByteString) any.getField(type.findFieldByName("value")));
        entered.ifPresent(decoded -> walk.declares(decoded.payload().getDescriptorForType()));
        return entered;
    }

    /** Refuses a link reached from the values of a map field through a typed Any. */
    private static void requireNoLinkUnderMap(FieldDescriptor map, FieldDescriptor field, List<?> entries,
            ArrayList<DocumentSchemaOccurrences.Step> at, Walk walk) {
        var keyField = field.getMessageType().findFieldByNumber(1);
        var valueField = field.getMessageType().findFieldByName("value");
        if (valueField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) return;
        at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
        for (var element : entries) {
            var entry = (Message) element;
            at.add(new DocumentSchemaOccurrences.MapKey(keyField.getType(), entry.getField(keyField)));
            requireNoLinkUnder(map, (Message) entry.getField(valueField), at, walk);
            at.removeLast();
        }
        at.removeLast();
    }

    /** Refuses a link reached from a map value through a typed Any. */
    private static void requireNoLinkUnder(FieldDescriptor map, Message value, ArrayList<DocumentSchemaOccurrences.Step> at,
            Walk walk) {
        walk.control().run();
        if (isAny(value.getDescriptorForType())) {
            var entered = unpacked(value, at, walk);
            if (entered.isEmpty()) return;
            if (walk.declares(entered.get().payload().getDescriptorForType()))
                throw new SchemaRefused(map.getFullName(), "a link cannot sit under a map value");
            at.add(entered.get().boundary());
            requireNoLinkUnder(map, entered.get().payload(), at, walk);
            at.removeLast();
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : value.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.isMapField()) {
                requireNoLinkUnderMap(map, field, (List<?>) entry.getValue(), at, walk);
                continue;
            }
            var elements = field.isRepeated() ? (List<?>) entry.getValue() : List.of(entry.getValue());
            at.add(new DocumentSchemaOccurrences.Field(field.getNumber()));
            for (int i = 0; i < elements.size(); i++) {
                if (field.isRepeated()) at.add(new DocumentSchemaOccurrences.Index(i));
                requireNoLinkUnder(map, (Message) elements.get(i), at, walk);
                if (field.isRepeated()) at.removeLast();
            }
            at.removeLast();
        }
    }

    private static DocumentLink value(FieldDescriptor field, Message value) {
        if (value instanceof DocumentLink typed) return typed;
        final DocumentLink link;
        try {
            link = DocumentLink.parseFrom(value.toByteString());
        } catch (InvalidProtocolBufferException unreadable) {
            throw new SchemaRefused(field.getFullName(), "the link value does not decode as " + LINK_TYPE);
        }
        if (!unknown(link).isEmpty())
            throw new SchemaRefused(field.getFullName(), "the link value carries fields this server does not know");
        return link;
    }

    private static List<Object> unknown(Message message) {
        var found = new ArrayList<Object>();
        if (!message.getUnknownFields().asMap().isEmpty()) found.add(message);
        for (var value : message.getAllFields().values()) {
            if (value instanceof Message nested) found.addAll(unknown(nested));
            else if (value instanceof List<?> list)
                for (var element : list) if (element instanceof Message nested) found.addAll(unknown(nested));
        }
        return found;
    }

    /** The same element subscript the validator writes into violation paths. */
    static String subscript(Object key) {
        if (key instanceof String text)
            return "[\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\"]";
        return "[" + key + "]";
    }
}

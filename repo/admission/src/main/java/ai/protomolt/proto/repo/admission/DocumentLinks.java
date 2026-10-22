package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
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
 * singular, repeated and map-valued message fields; an {@link Any} value is a
 * schema boundary and is not entered here. A {@code DocumentLink} field
 * without the option, the option on a field of any other type, a link field
 * anywhere under a map value, and an option whose own rules fail are each a
 * {@link SchemaRefused}.
 * <p>
 * A link's identity is its field path and ordinal: field names joined by
 * {@code .}, with {@code [i]} after each repeated ancestor message (for example
 * {@code sections[0].cites}), and the ordinal is the index inside the link field
 * itself, 0 when it is singular. Map values are not traversed, which is why no
 * link may sit under one. Descriptors built from retained schema bytes carry the
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
     * @param field the field's link declaration
     * @param link the link value
     */
    public record Link(String fieldPath, int ordinal, LinkField field, DocumentLink link) {
        public Link {
            Objects.requireNonNull(fieldPath); Objects.requireNonNull(field); Objects.requireNonNull(link);
            if (fieldPath.isEmpty() || ordinal < 0) throw new IllegalArgumentException("A link needs a field path and an ordinal");
        }
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
        check(type);
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
        return check(type);
    }

    /**
     * Checks every field of every message reachable from {@code root}, map values
     * included, then refuses a map whose value reaches a link. Reachability is a
     * fixed point over the type graph, so recursive types terminate.
     */
    private static boolean check(Descriptor root) {
        var types = new LinkedHashMap<String, Descriptor>();
        var pending = new ArrayDeque<Descriptor>();
        pending.add(root);
        while (!pending.isEmpty()) {
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
        // A type reaches a link when it holds a link field or a field whose type reaches one.
        var reaches = new HashSet<String>();
        for (boolean changed = true; changed; ) {
            changed = false;
            for (var type : types.values()) {
                if (reaches.contains(type.getFullName())) continue;
                for (var field : type.getFields()) {
                    if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
                    var target = field.isMapField() ? mapValue(field) : field.getMessageType();
                    if (target != null && (target.getFullName().equals(LINK_TYPE) || reaches.contains(target.getFullName()))) {
                        reaches.add(type.getFullName());
                        changed = true;
                        break;
                    }
                }
            }
        }
        for (var type : types.values())
            for (var field : type.getFields()) {
                if (!field.isMapField()) continue;
                var value = mapValue(field);
                if (value != null && (value.getFullName().equals(LINK_TYPE) || reaches.contains(value.getFullName())))
                    throw new SchemaRefused(field.getFullName(), "a link cannot sit under a map value");
            }
        return reaches.contains(root.getFullName());
    }

    /** A map field's value message type; null for a scalar value. */
    private static Descriptor mapValue(FieldDescriptor field) {
        var value = field.getMessageType().findFieldByName("value");
        return value.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? value.getMessageType() : null;
    }

    /**
     * The links in a message, in field number order and then element order. The
     * message's type must satisfy {@link #requireLinkSchema}; {@link Any} values
     * are not entered.
     *
     * @param message a decoded message
     * @param prefix the message's path in the document, or empty for the root
     * @return the links found
     * @throws SchemaRefused when a declaration breaks a rule or a link value
     *         carries fields this server's {@link DocumentLink} does not know
     */
    public static List<Link> extract(Message message, String prefix) {
        Objects.requireNonNull(message); Objects.requireNonNull(prefix);
        var found = new ArrayList<Link>();
        collect(message, prefix, found);
        return List.copyOf(found);
    }

    private static void collect(Message message, String prefix, List<Link> found) {
        if (message.getDescriptorForType().getFullName().equals(Any.getDescriptor().getFullName())) return;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            var field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            var path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            // Map values are not traversed; the schema rules keep links out of them.
            if (field.isMapField()) continue;
            var declared = linkField(field);
            boolean link = field.getMessageType().getFullName().equals(LINK_TYPE);
            if (link != declared.isPresent())
                throw new SchemaRefused(field.getFullName(), declared.isPresent()
                        ? "the link option needs a " + LINK_TYPE + " field" : "a " + LINK_TYPE + " field needs the link option");
            if (field.isRepeated()) {
                var values = (List<?>) entry.getValue();
                for (int i = 0; i < values.size(); i++) {
                    if (link) found.add(new Link(path, i, declared.get(), value(field, (Message) values.get(i))));
                    else collect((Message) values.get(i), path + "[" + i + "]", found);
                }
            } else if (link) {
                found.add(new Link(path, 0, declared.get(), value(field, (Message) entry.getValue())));
            } else {
                collect((Message) entry.getValue(), path, found);
            }
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

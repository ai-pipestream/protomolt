package ai.protomolt.proto.sources;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.squareup.wire.schema.Options;
import com.squareup.wire.schema.ProtoMember;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-encodes a Wire linked {@link Options} map into canonical descriptor bytes.
 *
 * <p>Wire's {@code SchemaEncoder} encodes message structure correctly but mangles options: it
 * converts the linked options to a JSON-style map keyed by the option field's <em>simple</em>
 * name, so custom option extensions that share a field name (this repo's annotation families
 * all declare extensions named {@code field} / {@code message} under different numbers)
 * collide — only the last family survives with content and the rest are emitted as empty
 * extension entries. The linked model itself ({@link Options#getMap()}, keyed by
 * {@link ProtoMember}) is fully intact; this class maps it straight to descriptor bytes with
 * no JSON anywhere.</p>
 *
 * <p>Value shapes follow Wire's {@code Options.kt} canonicalization: scalars and enum
 * identifiers are the source text as a {@code String}, message-typed fields are nested
 * {@code Map<ProtoMember, Object>}, repeated fields are {@code List}s of the element shape,
 * and map fields are {@code List}s of single-entry {@code Map}s. Every coercion failure is
 * loud — an {@link OptionEncodingException} naming the element and option; nothing is dropped
 * or defaulted.</p>
 *
 * <p>Package-private: the single caller is {@link LinkedOptionsRepair}. Instances are
 * immutable and thread-safe.</p>
 */
final class LinkedOptionsEncoder {

    /** Custom option extensions by qualified name, e.g. {@code ai.protomolt.proto.meta.v1.field}. */
    private final Map<String, FieldDescriptor> extensions;

    /**
     * Every message type in the compiled universe by full name. Options messages are built
     * against these descriptors — not protobuf-java's generated {@code *Options.getDescriptor()}
     * — so extension fields and their containing type come from the same instance universe
     * ({@code DynamicMessage} verifies containing-type identity).
     */
    private final Map<String, Descriptor> messageTypes;

    private LinkedOptionsEncoder(Map<String, FieldDescriptor> extensions,
                                 Map<String, Descriptor> messageTypes) {
        this.extensions = extensions;
        this.messageTypes = messageTypes;
    }

    /**
     * Indexes every extension and message type declared in {@code files} by qualified name. The
     * descriptors' structure is enough — option <em>values</em> on those files are irrelevant
     * here.
     */
    static LinkedOptionsEncoder over(List<FileDescriptor> files) {
        Map<String, FieldDescriptor> index = new HashMap<>();
        Map<String, Descriptor> messages = new HashMap<>();
        for (FileDescriptor file : files) {
            for (FieldDescriptor extension : file.getExtensions()) {
                index.put(extension.getFullName(), extension);
            }
            for (Descriptor message : file.getMessageTypes()) {
                collectMessage(message, index, messages);
            }
        }
        return new LinkedOptionsEncoder(index, messages);
    }

    private static void collectMessage(Descriptor message, Map<String, FieldDescriptor> index,
                                       Map<String, Descriptor> messages) {
        messages.put(message.getFullName(), message);
        for (FieldDescriptor extension : message.getExtensions()) {
            index.put(extension.getFullName(), extension);
        }
        for (Descriptor nested : message.getNestedTypes()) {
            collectMessage(nested, index, messages);
        }
    }

    boolean hasOptions(Options options) {
        return options != null && !options.getMap().isEmpty();
    }

    /**
     * Encodes {@code options} as a message of {@code optionsType} (the full name of one of the
     * {@code google.protobuf.*Options} types) and returns the serialized bytes.
     *
     * @param context names the element holding the options, for error messages
     *     (e.g. {@code "field example.Doc.name in doc.proto"})
     */
    byte[] encode(Options options, String optionsType, String context) throws OptionEncodingException {
        Descriptor type = messageTypes.get(optionsType);
        if (type == null) {
            throw new OptionEncodingException(context + ": the compiled set has no descriptor for "
                    + optionsType + " — cannot repair options without the extendee's file");
        }
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        for (Map.Entry<ProtoMember, Object> entry : options.getMap().entrySet()) {
            String member = entry.getKey().getMember();
            FieldDescriptor field = resolve(type, member, context);
            builder.setField(field, coerce(field, entry.getValue(),
                    context + " option (" + member + ")"));
        }
        return builder.build().toByteArray();
    }

    /**
     * Resolves one option member to a field of {@code type}. Extension members carry their
     * qualified name (proto field names cannot contain dots, so a dot always means an
     * extension); plain names are declared fields. A plain name that is not declared falls
     * back to the extension index so a root-package extension still resolves.
     */
    private FieldDescriptor resolve(Descriptor type, String member, String context)
            throws OptionEncodingException {
        if (member.indexOf('.') >= 0) {
            return resolveExtension(type, member, context);
        }
        FieldDescriptor field = type.findFieldByName(member);
        if (field != null) {
            return field;
        }
        return resolveExtension(type, member, context);
    }

    private FieldDescriptor resolveExtension(Descriptor type, String member, String context)
            throws OptionEncodingException {
        FieldDescriptor extension = extensions.get(member);
        if (extension == null) {
            throw new OptionEncodingException(context + ": unknown option extension (" + member
                    + ") on " + type.getFullName());
        }
        if (!extension.getContainingType().getFullName().equals(type.getFullName())) {
            throw new OptionEncodingException(context + ": option extension (" + member
                    + ") extends " + extension.getContainingType().getFullName()
                    + ", not " + type.getFullName());
        }
        return extension;
    }

    private Object coerce(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        if (value == null) {
            throw new OptionEncodingException(context + ": null value for " + field.getFullName());
        }
        if (field.isMapField()) {
            return coerceMapEntries(field, value, context);
        }
        if (field.isRepeated()) {
            List<Object> coerced = new ArrayList<>();
            for (Object element : requireList(field, value, context)) {
                coerced.add(coerceSingle(field, element, context));
            }
            return coerced;
        }
        return coerceSingle(field, value, context);
    }

    /**
     * Wire canonicalizes a map option to a list of single-entry {@code {key: value}} maps with
     * the key and value already canonicalized against the entry type's key/value fields.
     */
    private Object coerceMapEntries(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        Descriptor entryType = field.getMessageType();
        FieldDescriptor keyField = entryType.findFieldByNumber(1);
        FieldDescriptor valueField = entryType.findFieldByNumber(2);
        List<Object> entries = new ArrayList<>();
        for (Object element : requireList(field, value, context)) {
            if (!(element instanceof Map<?, ?> single) || single.size() != 1) {
                throw new OptionEncodingException(context + ": expected a single-entry map for "
                        + field.getFullName() + ", got " + describe(element));
            }
            Map.Entry<?, ?> entry = single.entrySet().iterator().next();
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new OptionEncodingException(context + ": incomplete map entry for "
                        + field.getFullName() + " (key or value missing)");
            }
            entries.add(DynamicMessage.newBuilder(entryType)
                    .setField(keyField, coerceSingle(keyField, entry.getKey(), context))
                    .setField(valueField, coerceSingle(valueField, entry.getValue(), context))
                    .build());
        }
        return entries;
    }

    private Object coerceSingle(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        return switch (field.getJavaType()) {
            case MESSAGE -> coerceMessage(field, value, context);
            case ENUM -> coerceEnum(field, value, context);
            default -> coerceScalar(field, requireString(field, value, context), context);
        };
    }

    private Object coerceMessage(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        if (!(value instanceof Map<?, ?> members)) {
            throw new OptionEncodingException(context + ": expected a member map for message field "
                    + field.getFullName() + ", got " + describe(value));
        }
        Descriptor type = field.getMessageType();
        DynamicMessage.Builder nested = DynamicMessage.newBuilder(type);
        for (Map.Entry<?, ?> member : members.entrySet()) {
            if (!(member.getKey() instanceof ProtoMember protoMember)) {
                throw new OptionEncodingException(context + ": unexpected key " + describe(member.getKey())
                        + " in value of message field " + field.getFullName());
            }
            FieldDescriptor memberField = resolve(type, protoMember.getMember(), context);
            nested.setField(memberField, coerce(memberField, member.getValue(),
                    context + " option (" + protoMember.getMember() + ")"));
        }
        return nested.build();
    }

    private Object coerceEnum(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        String text = requireString(field, value, context);
        // The source text may be package-qualified; identifiers resolve by simple name.
        String simple = text.substring(text.lastIndexOf('.') + 1);
        EnumValueDescriptor enumValue = field.getEnumType().findValueByName(simple);
        if (enumValue == null) {
            throw new OptionEncodingException(context + ": unknown identifier '" + text
                    + "' for enum " + field.getEnumType().getFullName());
        }
        return enumValue;
    }

    private Object coerceScalar(FieldDescriptor field, String text, String context)
            throws OptionEncodingException {
        try {
            return switch (field.getType()) {
                case INT32, SINT32, SFIXED32 -> Integer.valueOf(text);
                case UINT32, FIXED32 -> Integer.valueOf(Integer.parseUnsignedInt(text));
                case INT64, SINT64, SFIXED64 -> Long.valueOf(text);
                case UINT64, FIXED64 -> Long.valueOf(Long.parseUnsignedLong(text));
                case FLOAT -> parseFloat(text);
                case DOUBLE -> parseDouble(text);
                case BOOL -> parseBoolean(text, context);
                case STRING -> text;
                case BYTES -> ByteString.copyFromUtf8(text);
                default -> throw new OptionEncodingException(context + ": cannot coerce '" + text
                        + "' to " + field.getType() + " of " + field.getFullName());
            };
        } catch (NumberFormatException e) {
            throw new OptionEncodingException(context + ": '" + text + "' is not a valid "
                    + field.getType() + " for " + field.getFullName(), e);
        }
    }

    private static Float parseFloat(String text) {
        return switch (text.toLowerCase(java.util.Locale.ROOT)) {
            case "inf", "infinity" -> Float.POSITIVE_INFINITY;
            case "-inf", "-infinity" -> Float.NEGATIVE_INFINITY;
            case "nan" -> Float.NaN;
            default -> Float.valueOf(text);
        };
    }

    private static Double parseDouble(String text) {
        return switch (text.toLowerCase(java.util.Locale.ROOT)) {
            case "inf", "infinity" -> Double.POSITIVE_INFINITY;
            case "-inf", "-infinity" -> Double.NEGATIVE_INFINITY;
            case "nan" -> Double.NaN;
            default -> Double.valueOf(text);
        };
    }

    private static Boolean parseBoolean(String text, String context) throws OptionEncodingException {
        return switch (text) {
            case "true" -> Boolean.TRUE;
            case "false" -> Boolean.FALSE;
            default -> throw new OptionEncodingException(context + ": '" + text + "' is not a boolean");
        };
    }

    private static List<?> requireList(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        if (!(value instanceof List<?> list)) {
            throw new OptionEncodingException(context + ": expected a list for repeated field "
                    + field.getFullName() + ", got " + describe(value));
        }
        return list;
    }

    private static String requireString(FieldDescriptor field, Object value, String context)
            throws OptionEncodingException {
        if (!(value instanceof String text)) {
            throw new OptionEncodingException(context + ": expected source text for "
                    + field.getFullName() + ", got " + describe(value));
        }
        return text;
    }

    private static String describe(Object value) {
        if (value == null) {
            return "null";
        }
        String text = String.valueOf(value);
        if (text.length() > 80) {
            text = text.substring(0, 77) + "...";
        }
        return value.getClass().getSimpleName() + " " + text;
    }
}

package ai.protomolt.proto.validate.protovalidate;

import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import build.buf.validate.FieldRules;
import build.buf.validate.Rule;
import build.buf.validate.ValidateProto;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Predefined rules: {@code (buf.validate.predefined)} CEL expressions attached as extensions to
 * fields of the {@code buf.validate.<T>Rules} messages. A schema that declares one is saying "any
 * field carrying my extension also carries these CEL rules", so the extensions visible from a file
 * have to be found before its fields can be translated.
 *
 * <p>Two things make this more than a lookup. The extensions live on <em>other</em> files — the
 * file being translated plus its transitive imports — so the index is built per file and cached.
 * And a user extension is unknown to the generated {@code buf.validate} types, so a rule's
 * configured value survives only as an unknown field and has to be recovered by reparsing the
 * sub-rules bytes against the extension's own descriptor.
 *
 * <p>The recovered value binds as {@code rule} in the CEL expression, and the constraint carries
 * an extension-shaped rule path ({@code <type>.[<ext.full.name>]}) so a violation points at the
 * declaration rather than at the expression text.
 */
final class PredefinedRules {

    // Predefined-rule extension index per proto file (built from the file and its transitive
    // imports). Simple clear-on-threshold bound; wiped and repopulated on demand when full.
    private static final int MAX_CACHED_FILES = 64;
    private static final Map<FileDescriptor, PredefinedIndex> PREDEFINED_INDEXES =
            new ConcurrentHashMap<>();

    private PredefinedRules() {
    }

    /** The predefined extensions visible from {@code file} (itself plus transitive imports). */
    static PredefinedIndex indexFor(FileDescriptor file) {
        PredefinedIndex existing = PREDEFINED_INDEXES.get(file);
        if (existing != null) {
            return existing;
        }
        if (PREDEFINED_INDEXES.size() >= MAX_CACHED_FILES) {
            PREDEFINED_INDEXES.clear();
        }
        return PREDEFINED_INDEXES.computeIfAbsent(file, PredefinedRules::buildPredefinedIndex);
    }

    private static PredefinedIndex buildPredefinedIndex(FileDescriptor file) {
        Map<String, Map<Integer, PredefinedIndex.Ext>> index = new HashMap<>();
        collectPredefined(file, new HashSet<>(), index);
        return index.isEmpty() ? PredefinedIndex.EMPTY : new PredefinedIndex(index);
    }

    private static void collectPredefined(
            FileDescriptor file,
            Set<String> visited,
            Map<String, Map<Integer, PredefinedIndex.Ext>> index) {
        if (!visited.add(file.getFullName())) {
            return;
        }
        indexExtensions(file.getExtensions(), index);
        for (Descriptor message : file.getMessageTypes()) {
            indexNested(message, index);
        }
        for (FileDescriptor dep : file.getDependencies()) {
            collectPredefined(dep, visited, index);
        }
    }

    private static void indexNested(
            Descriptor message, Map<String, Map<Integer, PredefinedIndex.Ext>> index) {
        indexExtensions(message.getExtensions(), index);
        for (Descriptor nested : message.getNestedTypes()) {
            indexNested(nested, index);
        }
    }

    private static void indexExtensions(
            List<FieldDescriptor> extensions,
            Map<String, Map<Integer, PredefinedIndex.Ext>> index) {
        for (FieldDescriptor ext : extensions) {
            String containing = ext.getContainingType().getFullName();
            if (!containing.startsWith("buf.validate.")) {
                continue;
            }
            List<Rule> rules = predefinedRules(ext);
            if (rules.isEmpty()) {
                continue;
            }
            index.computeIfAbsent(containing, k -> new HashMap<>())
                    .put(ext.getNumber(), new PredefinedIndex.Ext(ext, rules));
        }
    }

    /** Reads the {@code (buf.validate.predefined).cel} rules off an extension's options. */
    private static List<Rule> predefinedRules(FieldDescriptor ext) {
        DescriptorProtos.FieldOptions options = OptionReparse.recover(ext.getOptions(),
                ValidateProto.predefined.getNumber(), DescriptorProtos.FieldOptions::parseFrom,
                "(buf.validate.predefined)");
        if (!options.hasExtension(ValidateProto.predefined)) {
            return List.of();
        }
        var rules = options.getExtension(ValidateProto.predefined);
        RulePayloads.requireSupported(rules, PredefinedIndex.EMPTY);
        return rules.getCelList();
    }

    /**
     * Appends a CEL constraint for every predefined extension set on {@code rules}' active
     * sub-rules message, with the extension's configured value carried as the {@code rule}
     * binding and an extension-shaped rule path ({@code <type>.[<ext.full.name>]}).
     */
    static void addPredefinedCel(
            FieldConstraints.Builder builder, FieldRules rules, PredefinedIndex predefined) {
        if (predefined.isEmpty()) {
            return;
        }
        Message subRules = activeSubRules(rules);
        if (subRules == null) {
            return;
        }
        Map<Integer, PredefinedIndex.Ext> byNumber =
                predefined.forType(subRules.getDescriptorForType().getFullName());
        if (byNumber.isEmpty()) {
            return;
        }
        String subField = typeFieldName(rules.getTypeCase());
        for (int number : extensionNumbersSetOn(subRules)) {
            PredefinedIndex.Ext ext = byNumber.get(number);
            if (ext == null) {
                continue;
            }
            Object ruleValue = ruleValue(subRules, ext.descriptor());
            String rulePath = subField + ".[" + ext.descriptor().getFullName() + "]";
            for (Rule rule : ext.rules()) {
                builder.addCel(new CelConstraint(
                        rule.getId(), rule.getExpression(), rule.getMessage(),
                        "cel", rulePath, ruleValue));
            }
        }
    }

    /**
     * The field numbers of extensions set on {@code subRules}: unknown fields (the common case —
     * user extensions are unknown to the generated {@code buf.validate} types) plus any extension
     * fields known to the message's registry.
     */
    private static List<Integer> extensionNumbersSetOn(Message subRules) {
        TreeSet<Integer> numbers =
                new TreeSet<>(subRules.getUnknownFields().asMap().keySet());
        for (FieldDescriptor fd : subRules.getAllFields().keySet()) {
            if (fd.isExtension()) {
                numbers.add(fd.getNumber());
            }
        }
        return List.copyOf(numbers);
    }

    /** The set type sub-rules message (int32/float/…/repeated/map), or null when none is set. */
    private static Message activeSubRules(FieldRules rules) {
        FieldDescriptor typeField = rules.getDescriptorForType()
                .findFieldByName(typeFieldName(rules.getTypeCase()));
        if (typeField == null || !rules.hasField(typeField)) {
            return null;
        }
        return (Message) rules.getField(typeField);
    }

    private static String typeFieldName(FieldRules.TypeCase typeCase) {
        return switch (typeCase) {
            case TYPE_NOT_SET -> "";
            default -> typeCase.name().toLowerCase(Locale.ROOT);
        };
    }

    /**
     * Reads the value the predefined extension is set to on {@code subRules}, as the CEL value the
     * rule's {@code rule} variable binds to. The extension is typically an unknown field on our
     * generated sub-rules message, so the message bytes are re-parsed against the extension's own
     * descriptor to recover a typed value. An undecodable value is a schema error.
     */
    private static Object ruleValue(Message subRules, FieldDescriptor ext) {
        try {
            ExtensionRegistry registry = ExtensionRegistry.newInstance();
            if (ext.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                registry.add(ext, DynamicMessage.getDefaultInstance(ext.getMessageType()));
            } else {
                registry.add(ext);
            }
            DynamicMessage parsed = DynamicMessage.parseFrom(
                    ext.getContainingType(), subRules.toByteString(), registry);
            if (parsed.getUnknownFields().hasField(ext.getNumber())
                    || (!ext.isRepeated() && !parsed.hasField(ext))) {
                throw new RuleCompilationException("invalid wire encoding for predefined rule " + ext.getFullName());
            }
            return celValue(ext, parsed.getField(ext));
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            throw new RuleCompilationException(
                    "cannot decode predefined rule value " + ext.getFullName() + ": " + e.getMessage(), e);
        }
    }

    /** Converts a protobuf field value to the Java type CEL expects for that proto type. */
    private static Object celValue(FieldDescriptor fd, Object value) {
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            for (Object element : list) {
                converted.add(scalarCelValue(fd, element));
            }
            return converted;
        }
        return scalarCelValue(fd, value);
    }

    private static Object scalarCelValue(FieldDescriptor fd, Object value) {
        return switch (fd.getType()) {
            case UINT32, FIXED32 -> UnsignedLong.fromLongBits(Integer.toUnsignedLong((Integer) value));
            case UINT64, FIXED64 -> UnsignedLong.fromLongBits((Long) value);
            case INT32, SINT32, SFIXED32 -> ((Integer) value).longValue();
            case FLOAT -> ((Float) value).doubleValue();
            case ENUM -> (long) ((EnumValueDescriptor) value).getNumber();
            case MESSAGE, GROUP -> messageCelValue((Message) value);
            default -> value;
        };
    }

    /** Well-known temporal messages map to their CEL temporal representation. */
    private static Object messageCelValue(Message value) {
        Descriptor descriptor = value.getDescriptorForType();
        return switch (descriptor.getFullName()) {
            case "google.protobuf.Duration" -> Duration.ofSeconds(
                    (Long) value.getField(descriptor.findFieldByName("seconds")),
                    (Integer) value.getField(descriptor.findFieldByName("nanos")));
            case "google.protobuf.Timestamp" -> Instant.ofEpochSecond(
                    (Long) value.getField(descriptor.findFieldByName("seconds")),
                    (Integer) value.getField(descriptor.findFieldByName("nanos")));
            default -> value;
        };
    }
}

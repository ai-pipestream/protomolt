package ai.pipestream.proto.validate.conformance;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.validate.cel.ValidationCelFunctions;
import build.buf.validate.FieldPath;
import build.buf.validate.FieldPathElement;
import build.buf.validate.FieldRules;
import build.buf.validate.RepeatedRules;
import build.buf.validate.MapRules;
import build.buf.validate.Rule;
import build.buf.validate.ValidateProto;
import build.buf.validate.Violation;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates protovalidate <em>predefined</em> rules — custom rule extensions declared with
 * {@code (buf.validate.predefined).cel} on a field of a {@code buf.validate.<T>Rules} message and
 * used via {@code (buf.validate.field).<type>.(<ext>) = <value>}.
 *
 * <p>These extensions are defined in the request's descriptor set, so our generated
 * {@code build.buf.validate} types see them only as unknown fields. This engine indexes the
 * extensions from the linked files, reads the set rule value back off the unknown fields by
 * re-parsing the sub-rules message against the extension descriptor, then evaluates each rule's CEL
 * with {@code this} bound to the field value and {@code rule} to the extension value — the same
 * contract protovalidate defines. It runs as a post-pass alongside the standard validator and emits
 * fully-formed {@link Violation}s, including the extension-shaped rule path
 * ({@code <type>.[<ext.full.name>]}, prefixed with {@code repeated.items}/{@code map.*} for element
 * rules).
 */
final class PredefinedRules {

    /** One predefined extension: its descriptor and the CEL rules attached to it. */
    private record Ext(FieldDescriptor descriptor, List<Rule> rules) {
    }

    private final ExtensionRegistry registry;
    // rulesTypeFullName (e.g. "buf.validate.Int32Rules") -> fieldNumber -> extension.
    private final Map<String, Map<Integer, Ext>> index;
    private final CelEvaluator cel;

    private PredefinedRules(ExtensionRegistry registry, Map<String, Map<Integer, Ext>> index) {
        this.registry = registry;
        this.index = index;
        this.cel = new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("this")
                .addVar("rule")
                .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings())
                .build());
    }

    /** Builds an engine from the linked files, or null when no predefined extensions are present. */
    static PredefinedRules from(List<FileDescriptor> files, ExtensionRegistry registry) {
        Map<String, Map<Integer, Ext>> index = new HashMap<>();
        for (FileDescriptor file : files) {
            indexExtensions(file.getExtensions(), index, registry);
            for (Descriptor message : file.getMessageTypes()) {
                indexNested(message, index, registry);
            }
        }
        return index.isEmpty() ? null : new PredefinedRules(registry, index);
    }

    private static void indexNested(
            Descriptor message, Map<String, Map<Integer, Ext>> index, ExtensionRegistry registry) {
        indexExtensions(message.getExtensions(), index, registry);
        for (Descriptor nested : message.getNestedTypes()) {
            indexNested(nested, index, registry);
        }
    }

    private static void indexExtensions(
            List<FieldDescriptor> extensions,
            Map<String, Map<Integer, Ext>> index,
            ExtensionRegistry registry) {
        for (FieldDescriptor ext : extensions) {
            String containing = ext.getContainingType().getFullName();
            if (!containing.startsWith("buf.validate.")) {
                continue;
            }
            List<Rule> rules = predefinedRules(ext, registry);
            if (rules.isEmpty()) {
                continue;
            }
            index.computeIfAbsent(containing, k -> new HashMap<>())
                    .put(ext.getNumber(), new Ext(ext, rules));
        }
    }

    /** Reads the {@code (buf.validate.predefined).cel} rules off an extension's options. */
    private static List<Rule> predefinedRules(FieldDescriptor ext, ExtensionRegistry registry) {
        FieldOptions options = ext.getOptions();
        if (!options.hasExtension(ValidateProto.predefined)) {
            // Custom options on a dynamically linked descriptor may survive only as unknown fields;
            // re-parse them with a registry that knows the predefined extension.
            try {
                options = FieldOptions.parseFrom(options.toByteString(), registry);
            } catch (Exception e) {
                return List.of();
            }
        }
        if (!options.hasExtension(ValidateProto.predefined)) {
            return List.of();
        }
        return options.getExtension(ValidateProto.predefined).getCelList();
    }

    // ---- evaluation ----

    /** Evaluates every predefined rule reachable from {@code message}, collecting violations. */
    List<Violation> evaluate(Message message) {
        List<Violation> out = new ArrayList<>();
        walkMessage(message, List.of(), out);
        return out;
    }

    /** Walks every field of {@code message}, prefixing produced field paths with {@code prefix}. */
    private void walkMessage(Message message, List<FieldPathElement> prefix, List<Violation> out) {
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            walkField(message, field, fieldRules(field), prefix, out);
        }
    }

    private FieldRules fieldRules(FieldDescriptor field) {
        FieldOptions options = field.getOptions();
        if (!options.hasExtension(ValidateProto.field)) {
            return null;
        }
        return options.getExtension(ValidateProto.field);
    }

    private void walkField(
            Message message, FieldDescriptor field, FieldRules rules,
            List<FieldPathElement> prefix, List<Violation> out) {
        if (field.isMapField()) {
            if (rules != null) {
                evalMap(message, field, rules, prefix, out);
            }
            return;
        }
        if (field.isRepeated()) {
            if (rules != null) {
                evalRepeated(message, field, rules, prefix, out);
            }
            return;
        }
        // Skip unpopulated message/optional fields; implicit scalars validate at their zero value.
        if (field.hasPresence() && !message.hasField(field)) {
            return;
        }
        Object value = message.getField(field);
        if (rules != null) {
            // The field-level rules attach to the type sub-rules (or the wrapper's scalar sub-rules).
            FieldPath.Builder fieldPath = FieldPath.newBuilder()
                    .addAllElements(prefix).addElements(fieldElement(field, null));
            applyRules(rules, unwrap(field, value), fieldPath, List.of(), out);
        }
        // Recurse into a nested non-wrapper message so predefined rules on its fields are reached.
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && value instanceof Message nested
                && wrapperValueField(nested.getDescriptorForType()) == null) {
            List<FieldPathElement> childPrefix = new ArrayList<>(prefix);
            childPrefix.add(fieldElement(field, null));
            walkMessage(nested, childPrefix, out);
        }
    }

    private void evalRepeated(
            Message message, FieldDescriptor field, FieldRules rules,
            List<FieldPathElement> pathPrefix, List<Violation> out) {
        // Rules on the repeated field itself (e.g. repeated.at_least_five): this = the whole list.
        List<Object> list = new ArrayList<>();
        int count = message.getRepeatedFieldCount(field);
        for (int i = 0; i < count; i++) {
            list.add(unwrapElement(field, message.getRepeatedField(field, i)));
        }
        FieldPath.Builder fieldPath = FieldPath.newBuilder()
                .addAllElements(pathPrefix).addElements(fieldElement(field, null));
        applyRules(rules, list, fieldPath, List.of(), out);

        // Rules on each element (repeated.items.<type>): this = the element value.
        if (rules.hasRepeated() && rules.getRepeated().hasItems()) {
            FieldRules items = rules.getRepeated().getItems();
            List<FieldPathElement> prefix = List.of(
                    ruleElement(FieldRules.getDescriptor(), "repeated"),
                    ruleElement(RepeatedRules.getDescriptor(), "items"));
            for (int i = 0; i < count; i++) {
                Object element = unwrapElement(field, message.getRepeatedField(field, i));
                FieldPath.Builder elementPath = FieldPath.newBuilder()
                        .addAllElements(pathPrefix).addElements(fieldElement(field, i));
                applyRules(items, element, elementPath, prefix, out);
            }
        }
    }

    private void evalMap(
            Message message, FieldDescriptor field, FieldRules rules,
            List<FieldPathElement> pathPrefix, List<Violation> out) {
        FieldDescriptor keyField = field.getMessageType().findFieldByNumber(1);
        FieldDescriptor valueField = field.getMessageType().findFieldByNumber(2);
        int count = message.getRepeatedFieldCount(field);

        // Rules on the map itself (e.g. map.at_least_five): this = the whole map.
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            map.put(celValue(keyField, entry.getField(keyField)),
                    celValue(valueField, entry.getField(valueField)));
        }
        FieldPath.Builder fieldPath = FieldPath.newBuilder()
                .addAllElements(pathPrefix).addElements(fieldElement(field, null));
        applyRules(rules, map, fieldPath, List.of(), out);

        // Rules on keys/values.
        MapRules mapRules = rules.hasMap() ? rules.getMap() : null;
        if (mapRules == null) {
            return;
        }
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            Object key = entry.getField(keyField);
            if (mapRules.hasKeys()) {
                FieldPath.Builder p = FieldPath.newBuilder()
                        .addAllElements(pathPrefix).addElements(mapElement(field, keyField, valueField, key));
                applyRules(mapRules.getKeys(), celValue(keyField, key), p,
                        List.of(ruleElement(FieldRules.getDescriptor(), "map"),
                                ruleElement(MapRules.getDescriptor(), "keys")),
                        out, true);
            }
            if (mapRules.hasValues()) {
                FieldPath.Builder p = FieldPath.newBuilder()
                        .addAllElements(pathPrefix).addElements(mapElement(field, keyField, valueField, key));
                applyRules(mapRules.getValues(), celValue(valueField, entry.getField(valueField)), p,
                        List.of(ruleElement(FieldRules.getDescriptor(), "map"),
                                ruleElement(MapRules.getDescriptor(), "values")),
                        out);
            }
        }
    }

    private void applyRules(
            FieldRules rules, Object thisValue, FieldPath.Builder fieldPath,
            List<FieldPathElement> rulePrefix, List<Violation> out) {
        applyRules(rules, thisValue, fieldPath, rulePrefix, out, false);
    }

    /**
     * Finds every predefined extension set on {@code rules}' active sub-rules message and evaluates
     * it against {@code thisValue}.
     */
    private void applyRules(
            FieldRules rules, Object thisValue, FieldPath.Builder fieldPath,
            List<FieldPathElement> rulePrefix, List<Violation> out, boolean forKey) {
        Message subRules = activeSubRules(rules);
        if (subRules == null) {
            return;
        }
        String subRulesType = subRules.getDescriptorForType().getFullName();
        Map<Integer, Ext> byNumber = index.get(subRulesType);
        if (byNumber == null || byNumber.isEmpty()) {
            return;
        }
        FieldPathElement subElement = subRulesElement(subRulesType);
        subRules.getUnknownFields().asMap().forEach((number, unknownField) -> {
            Ext ext = byNumber.get(number);
            if (ext == null) {
                return;
            }
            Object ruleValue = ruleValue(subRules, ext.descriptor());
            for (Rule rule : ext.rules()) {
                evalRule(rule, thisValue, ruleValue, fieldPath, rulePrefix, subElement, ext, forKey, out);
            }
        });
    }

    private void evalRule(
            Rule rule, Object thisValue, Object ruleValue, FieldPath.Builder fieldPath,
            List<FieldPathElement> rulePrefix, FieldPathElement subElement, Ext ext,
            boolean forKey, List<Violation> out) {
        if (rule.getExpression().isBlank()) {
            return;
        }
        Object result;
        try {
            Map<String, Object> bindings = new HashMap<>();
            bindings.put("this", thisValue);
            bindings.put("rule", ruleValue);
            result = cel.evaluateValue(rule.getExpression(), bindings);
        } catch (RuntimeException e) {
            // A failed predefined evaluation should not crash the case; skip this rule.
            return;
        }
        String message = violationMessage(result, rule);
        if (message == null) {
            return;
        }
        FieldPath rulePath = buildRulePath(rulePrefix, subElement, ext.descriptor());
        Violation.Builder v = Violation.newBuilder()
                .setRuleId(rule.getId())
                .setMessage(message)
                .setField(fieldPath.build())
                .setRule(rulePath);
        if (forKey) {
            v.setForKey(true);
        }
        out.add(v.build());
    }

    /** A CEL predicate violates when it returns {@code false} or a non-empty string message. */
    private static String violationMessage(Object result, Rule rule) {
        if (result instanceof Boolean ok) {
            return ok ? null : (rule.getMessage().isBlank() ? "CEL rule failed" : rule.getMessage());
        }
        if (result instanceof String text) {
            return text.isEmpty() ? null : text;
        }
        return null;
    }

    // ---- rule value decoding ----

    /**
     * Reads the value the predefined extension is set to on {@code subRules}. The extension is an
     * unknown field on our generated sub-rules message, so re-parse the message bytes against the
     * extension's own descriptor (from the request's file set) to recover a typed value.
     */
    private Object ruleValue(Message subRules, FieldDescriptor ext) {
        try {
            ExtensionRegistry reg = ExtensionRegistry.newInstance();
            if (ext.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                reg.add(ext, DynamicMessage.getDefaultInstance(ext.getMessageType()));
            } else {
                reg.add(ext);
            }
            DynamicMessage dm = DynamicMessage.parseFrom(
                    ext.getContainingType(), subRules.toByteString(), reg);
            Object raw = dm.getField(ext);
            return celValue(ext, raw);
        } catch (Exception e) {
            return null;
        }
    }

    // ---- CEL value conversion ----

    /** Converts a protobuf field value to the Java type CEL expects for that proto type. */
    @SuppressWarnings("unchecked")
    private Object celValue(FieldDescriptor fd, Object value) {
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            for (Object element : list) {
                converted.add(scalarCelValue(fd, element));
            }
            return converted;
        }
        return scalarCelValue(fd, value);
    }

    private Object scalarCelValue(FieldDescriptor fd, Object value) {
        return switch (fd.getType()) {
            case UINT32, FIXED32 -> UnsignedLong.fromLongBits(Integer.toUnsignedLong((Integer) value));
            case UINT64, FIXED64 -> UnsignedLong.fromLongBits((Long) value);
            case INT32, SINT32, SFIXED32 -> ((Integer) value).longValue();
            case INT64, SINT64, SFIXED64 -> value;
            case FLOAT -> ((Float) value).doubleValue();
            case DOUBLE, BOOL, STRING -> value;
            case BYTES -> value;
            case ENUM -> (long) ((EnumValueDescriptor) value).getNumber();
            case MESSAGE, GROUP -> messageCelValue((Message) value);
        };
    }

    /** Well-known wrapper/temporal messages map to their CEL scalar or temporal representation. */
    private Object messageCelValue(Message value) {
        String type = value.getDescriptorForType().getFullName();
        return switch (type) {
            case "google.protobuf.Duration" -> java.time.Duration.ofSeconds(
                    (Long) value.getField(value.getDescriptorForType().findFieldByName("seconds")),
                    (Integer) value.getField(value.getDescriptorForType().findFieldByName("nanos")));
            case "google.protobuf.Timestamp" -> java.time.Instant.ofEpochSecond(
                    (Long) value.getField(value.getDescriptorForType().findFieldByName("seconds")),
                    (Integer) value.getField(value.getDescriptorForType().findFieldByName("nanos")));
            default -> value;
        };
    }

    /** For a singular wrapper field, unwrap to the inner scalar; otherwise pass the value through. */
    private Object unwrap(FieldDescriptor field, Object value) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            Message msg = (Message) value;
            FieldDescriptor wrapped = wrapperValueField(msg.getDescriptorForType());
            if (wrapped != null) {
                return scalarCelValue(wrapped, msg.getField(wrapped));
            }
            return messageCelValue(msg);
        }
        return scalarCelValue(field, value);
    }

    private Object unwrapElement(FieldDescriptor field, Object value) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && value instanceof Message msg) {
            FieldDescriptor wrapped = wrapperValueField(msg.getDescriptorForType());
            if (wrapped != null) {
                return scalarCelValue(wrapped, msg.getField(wrapped));
            }
            return messageCelValue(msg);
        }
        return scalarCelValue(field, value);
    }

    private static FieldDescriptor wrapperValueField(Descriptor descriptor) {
        String name = descriptor.getFullName();
        if (name.startsWith("google.protobuf.") && name.endsWith("Value")
                && descriptor.getFields().size() == 1) {
            return descriptor.findFieldByNumber(1);
        }
        return null;
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
            default -> typeCase.name().toLowerCase(java.util.Locale.ROOT);
        };
    }

    // ---- field / rule path construction ----

    private static FieldPathElement fieldElement(FieldDescriptor field, Integer index) {
        FieldPathElement.Builder b = FieldPathElement.newBuilder()
                .setFieldNumber(field.getNumber())
                .setFieldName(field.getName())
                .setFieldType(field.getType().toProto());
        if (index != null) {
            b.setIndex(index);
        }
        return b.build();
    }

    private static FieldPathElement mapElement(
            FieldDescriptor field, FieldDescriptor keyField, FieldDescriptor valueField, Object key) {
        FieldPathElement.Builder b = FieldPathElement.newBuilder()
                .setFieldNumber(field.getNumber())
                .setFieldName(field.getName())
                .setFieldType(field.getType().toProto())
                .setKeyType(keyField.getType().toProto())
                .setValueType(valueField.getType().toProto());
        switch (keyField.getType()) {
            case BOOL -> b.setBoolKey((Boolean) key);
            case STRING -> b.setStringKey((String) key);
            case UINT32, FIXED32 -> b.setUintKey(Integer.toUnsignedLong((Integer) key));
            case UINT64, FIXED64 -> b.setUintKey((Long) key);
            case INT32, SINT32, SFIXED32 -> b.setIntKey(((Integer) key).longValue());
            default -> b.setIntKey(((Number) key).longValue());
        }
        return b.build();
    }

    private static FieldPathElement ruleElement(Descriptor owner, String fieldName) {
        FieldDescriptor fd = owner.findFieldByName(fieldName);
        return FieldPathElement.newBuilder()
                .setFieldNumber(fd.getNumber())
                .setFieldName(fd.getName())
                .setFieldType(fd.getType().toProto())
                .build();
    }

    /** The FieldRules element for the sub-rules message an extension lives on (int32/repeated/…). */
    private static FieldPathElement subRulesElement(String subRulesTypeFullName) {
        for (FieldDescriptor fd : FieldRules.getDescriptor().getFields()) {
            if (fd.getType() == FieldDescriptor.Type.MESSAGE
                    && fd.getMessageType().getFullName().equals(subRulesTypeFullName)) {
                return FieldPathElement.newBuilder()
                        .setFieldNumber(fd.getNumber())
                        .setFieldName(fd.getName())
                        .setFieldType(fd.getType().toProto())
                        .build();
            }
        }
        throw new IllegalStateException("no FieldRules field for sub-rules " + subRulesTypeFullName);
    }

    private static FieldPath buildRulePath(
            List<FieldPathElement> prefix, FieldPathElement subElement, FieldDescriptor ext) {
        FieldPath.Builder b = FieldPath.newBuilder().addAllElements(prefix);
        // repeated.at_least_five etc. live directly on RepeatedRules/MapRules: the sub element IS the
        // repeated/map element and there is no additional type element.
        boolean subIsContainer = !prefix.isEmpty()
                && subElement.getFieldName().equals(prefix.get(0).getFieldName());
        if (!subIsContainer) {
            b.addElements(subElement);
        }
        b.addElements(FieldPathElement.newBuilder()
                .setFieldNumber(ext.getNumber())
                .setFieldName("[" + ext.getFullName() + "]")
                .setFieldType(ext.getType().toProto())
                .build());
        return b.build();
    }
}

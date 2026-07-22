package ai.pipestream.proto.shapes;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Reverse-engineers a proto definition from data-rich JSON: given one or more sample
 * {@code Struct}s, infers a message type — objects become nested messages, arrays become
 * repeated fields with element inference, and JSON numbers become {@code int64} when they
 * are integral across <em>every</em> sample, {@code double} otherwise. Anything genuinely
 * dynamic (mixed-type values, empty objects, empty or mixed arrays, null-only keys) falls
 * back to {@code google.protobuf.Value} rather than guessing.
 *
 * <p>Keys are sanitized to field identifiers; when sanitization changes a key, the field
 * carries {@code json_name} with the original, so the inferred schema round-trips the very
 * documents it was inferred from. More samples make a better schema: keys union, and the
 * numeric heuristic sees every occurrence.</p>
 */
public final class SchemaInferrer {

    /** Guards against adversarially deep documents. */
    private static final int MAX_DEPTH = 32;

    private final ShapeSynthesizer synthesizer = new ShapeSynthesizer();

    public ShapeSynthesizer.SynthesizedShape infer(String fullName, List<Struct> samples) {
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("Inference needs at least one sample");
        }
        DescriptorProto.Builder message = inferMessage(
                simpleName(fullName), samples, 0);
        // Import only what the shape actually uses: struct.proto for Value fallbacks,
        // metadata.proto when sanitized keys carry the json_name annotation.
        List<com.google.protobuf.Descriptors.FileDescriptor> dependencies =
                new ArrayList<>();
        if (usesValue(message)) {
            dependencies.add(Value.getDescriptor().getFile());
        }
        if (usesMetaOption(message)) {
            dependencies.add(ai.pipestream.proto.meta.MetadataProto.getDescriptor());
        }
        return synthesizer.linkSynthetic(fullName, message, dependencies, List.of());
    }

    private DescriptorProto.Builder inferMessage(String name, List<Struct> samples,
                                                 int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("Sample nests deeper than " + MAX_DEPTH
                    + " levels");
        }
        // Union of keys in first-seen order; every observed value per key.
        Map<String, List<Value>> observations = new LinkedHashMap<>();
        for (Struct sample : samples) {
            sample.getFieldsMap().forEach((key, value) ->
                    observations.computeIfAbsent(key, k -> new ArrayList<>()).add(value));
        }
        if (observations.isEmpty()) {
            throw new IllegalArgumentException("'" + name + "' has no keys in any sample; "
                    + "an empty object infers nothing");
        }
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName(name);
        Set<String> fieldNames = new LinkedHashSet<>();
        Set<String> nestedNames = new LinkedHashSet<>();
        int number = 1;
        for (Map.Entry<String, List<Value>> entry : observations.entrySet()) {
            String key = entry.getKey();
            String fieldName = fieldName(key, fieldNames);
            FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(number++)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
            if (!fieldName.equals(key)) {
                // Both spellings: the real json_name (used directly by JSON parsers) and
                // the meta.v1 annotation, which survives text round-trips that drop
                // json_name and is materialized back by ProtoMolt loaders.
                field.setJsonName(key);
                field.getOptionsBuilder().setExtension(
                        ai.pipestream.proto.meta.MetadataProto.field,
                        ai.pipestream.proto.meta.FieldMeta.newBuilder()
                                .setJsonName(key).build());
            }
            applyKind(field, kindOf(entry.getValue()), fieldName, entry.getValue(),
                    message, nestedNames, depth);
            message.addField(field);
        }
        return message;
    }

    /** The JSON kinds a key was observed with; null means absent and never narrows. */
    private enum Kind { NULL, BOOL, INT, DOUBLE, STRING, OBJECT, LIST, MIXED }

    private static Kind kindOf(List<Value> values) {
        Kind kind = Kind.NULL;
        for (Value value : values) {
            Kind next = switch (value.getKindCase()) {
                case NULL_VALUE, KIND_NOT_SET -> Kind.NULL;
                case BOOL_VALUE -> Kind.BOOL;
                case NUMBER_VALUE -> integral(value.getNumberValue()) ? Kind.INT : Kind.DOUBLE;
                case STRING_VALUE -> Kind.STRING;
                case STRUCT_VALUE -> Kind.OBJECT;
                case LIST_VALUE -> Kind.LIST;
            };
            kind = widen(kind, next);
        }
        return kind;
    }

    private static Kind widen(Kind current, Kind next) {
        if (current == next || next == Kind.NULL) {
            return current;
        }
        if (current == Kind.NULL) {
            return next;
        }
        // The one numeric widening; everything else mixed is Value territory.
        if ((current == Kind.INT && next == Kind.DOUBLE)
                || (current == Kind.DOUBLE && next == Kind.INT)) {
            return Kind.DOUBLE;
        }
        return Kind.MIXED;
    }

    private static boolean integral(double number) {
        return number == Math.rint(number) && !Double.isInfinite(number)
                && Math.abs(number) <= 9.007199254740992E15; // 2^53: exact in a JSON double
    }

    private void applyKind(FieldDescriptorProto.Builder field, Kind kind, String fieldName,
                           List<Value> values, DescriptorProto.Builder parent,
                           Set<String> nestedNames, int depth) {
        switch (kind) {
            case BOOL -> field.setType(FieldDescriptorProto.Type.TYPE_BOOL);
            case INT -> field.setType(FieldDescriptorProto.Type.TYPE_INT64);
            case DOUBLE -> field.setType(FieldDescriptorProto.Type.TYPE_DOUBLE);
            case STRING -> field.setType(FieldDescriptorProto.Type.TYPE_STRING);
            case OBJECT -> {
                List<Struct> nested = values.stream()
                        .filter(Value::hasStructValue)
                        .map(Value::getStructValue)
                        .toList();
                if (nested.stream().allMatch(struct -> struct.getFieldsCount() == 0)) {
                    // An object with no keys in any sample carries no signal, like an empty
                    // array: it degrades to Value instead of aborting the whole inference.
                    valueFallback(field);
                } else {
                    String typeName = nestedTypeName(fieldName, nestedNames);
                    parent.addNestedType(inferMessage(typeName, nested, depth + 1));
                    field.setType(FieldDescriptorProto.Type.TYPE_MESSAGE);
                    field.setTypeName(typeName); // relative: resolves to the nested type
                }
            }
            case LIST -> {
                field.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
                List<Value> elements = new ArrayList<>();
                for (Value value : values) {
                    if (value.hasListValue()) {
                        elements.addAll(value.getListValue().getValuesList());
                    }
                }
                Kind elementKind = kindOf(elements);
                if (elementKind == Kind.LIST || elements.isEmpty()) {
                    // Nested lists have no direct proto shape; empty lists carry no signal.
                    valueFallback(field);
                } else {
                    applyKind(field, elementKind, fieldName, elements, parent,
                            nestedNames, depth);
                    field.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
                }
            }
            case NULL, MIXED -> valueFallback(field);
        }
    }

    private static void valueFallback(FieldDescriptorProto.Builder field) {
        field.setType(FieldDescriptorProto.Type.TYPE_MESSAGE);
        field.setTypeName("." + Value.getDescriptor().getFullName());
    }

    private static String fieldName(String key, Set<String> taken) {
        String name = key.replaceAll("[^A-Za-z0-9_]", "_");
        if (name.isEmpty() || Character.isDigit(name.charAt(0))) {
            name = "_" + name;
        }
        name = name.toLowerCase(Locale.ROOT);
        String candidate = name;
        int n = 2;
        while (!taken.add(candidate)) {
            candidate = name + "_" + n++;
        }
        return candidate;
    }

    private static String nestedTypeName(String fieldName, Set<String> taken) {
        StringBuilder out = new StringBuilder();
        boolean upper = true;
        for (char c : fieldName.toCharArray()) {
            if (c == '_') {
                upper = true;
            } else {
                out.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        String candidate = out.toString();
        int n = 2;
        while (!taken.add(candidate)) {
            candidate = out + "_" + n++;
        }
        return candidate;
    }

    private static boolean usesMetaOption(DescriptorProto.Builder message) {
        for (FieldDescriptorProto field : message.getFieldList()) {
            if (field.getOptions().hasExtension(
                    ai.pipestream.proto.meta.MetadataProto.field)) {
                return true;
            }
        }
        for (DescriptorProto.Builder nested : message.getNestedTypeBuilderList()) {
            if (usesMetaOption(nested)) {
                return true;
            }
        }
        return false;
    }

    private static boolean usesValue(DescriptorProto.Builder message) {
        for (FieldDescriptorProto field : message.getFieldList()) {
            if (field.getTypeName().equals("." + Value.getDescriptor().getFullName())) {
                return true;
            }
        }
        for (DescriptorProto.Builder nested : message.getNestedTypeBuilderList()) {
            if (usesValue(nested)) {
                return true;
            }
        }
        return false;
    }

    private static String simpleName(String fullName) {
        int dot = fullName.lastIndexOf('.');
        return dot < 0 ? fullName : fullName.substring(dot + 1);
    }
}

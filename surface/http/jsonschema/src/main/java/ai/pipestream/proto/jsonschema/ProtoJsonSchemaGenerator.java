package ai.pipestream.proto.jsonschema;

import ai.pipestream.proto.validate.model.BoolConstraints;
import ai.pipestream.proto.validate.model.CelConstraint;
import ai.pipestream.proto.validate.model.EnumConstraints;
import ai.pipestream.proto.validate.model.FieldConstraints;
import ai.pipestream.proto.validate.model.FloatingConstraints;
import ai.pipestream.proto.validate.model.IntegralConstraints;
import ai.pipestream.proto.validate.model.MapConstraints;
import ai.pipestream.proto.validate.model.RepeatedConstraints;
import ai.pipestream.proto.validate.model.StringConstraints;
import ai.pipestream.proto.validate.model.StringFormat;
import ai.pipestream.proto.validate.spi.ValidationRuleSource;
import ai.pipestream.proto.validate.spi.ValidationRuleSources;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Generates a JSON Schema (draft 2020-12) for a protobuf message type, describing its
 * canonical proto3 JSON encoding and enriched with the validation constraints read
 * through the configured {@link ValidationRuleSource} chain. Because the generator
 * consumes the neutral constraint model, it works for any annotation dialect a source
 * can read — not just the Pipestream {@code validate.v1} options.
 *
 * <p>Shape: the root schema {@code $ref}s into {@code $defs}, where every reachable
 * message type is defined once by full name — recursion-safe. Properties use proto3
 * JSON names (lowerCamelCase). {@code required: true} constraints become the JSON
 * Schema {@code required} array; CEL rules are not expressible in JSON Schema and are
 * surfaced verbatim under the {@code x-pipestream-cel} vendor keyword.
 *
 * <p>Known gaps (documented, not silent): bytes length rules are not mapped (JSON
 * carries base64 text, not raw lengths), and timestamp/duration bounds are not mapped
 * (JSON Schema cannot compare {@code date-time} values). Floating {@code finite} needs
 * no mapping — JSON numbers are always finite.
 */
public final class ProtoJsonSchemaGenerator {

    private static final String CEL_KEYWORD = "x-pipestream-cel";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<ValidationRuleSource> sources;

    private ProtoJsonSchemaGenerator(List<ValidationRuleSource> sources) {
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
    }

    /** Uses the default rule-source chain ({@link ValidationRuleSources#defaults()}). */
    public static ProtoJsonSchemaGenerator create() {
        return new ProtoJsonSchemaGenerator(ValidationRuleSources.defaults());
    }

    /** As {@link #create()} but with an explicit rule-source chain. */
    public static ProtoJsonSchemaGenerator create(List<ValidationRuleSource> sources) {
        return new ProtoJsonSchemaGenerator(sources);
    }

    /** Generates the schema as an ordered JSON-shaped map (maps, lists, scalars). */
    public Map<String, Object> generate(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return new Generation().run(descriptor);
    }

    /** Generates the schema as pretty-printed JSON text. */
    public String generateJson(Descriptor descriptor) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(generate(descriptor));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JSON Schema", e);
        }
    }

    /** Per-call state: the $defs under construction and the worklist of message types. */
    private final class Generation {

        private final Map<String, Object> defs = new LinkedHashMap<>();
        private final Deque<Descriptor> queue = new ArrayDeque<>();

        Map<String, Object> run(Descriptor root) {
            enqueue(root);
            while (!queue.isEmpty()) {
                Descriptor next = queue.poll();
                String name = next.getFullName();
                if (!(defs.get(name) instanceof PendingDef)) {
                    continue;
                }
                defs.put(name, messageDef(next));
            }
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
            schema.put("$ref", "#/$defs/" + root.getFullName());
            schema.put("$defs", defs);
            return schema;
        }

        private void enqueue(Descriptor descriptor) {
            defs.computeIfAbsent(descriptor.getFullName(), name -> {
                queue.add(descriptor);
                return PendingDef.INSTANCE;
            });
        }

        private Map<String, Object> messageDef(Descriptor descriptor) {
            Map<String, Object> properties = new LinkedHashMap<>();
            List<String> required = new ArrayList<>();
            for (FieldDescriptor field : descriptor.getFields()) {
                List<FieldConstraints> constraints = constraintsFor(field);
                properties.put(field.getJsonName(), fieldSchema(field, constraints));
                if (constraints.stream().anyMatch(FieldConstraints::required)) {
                    required.add(field.getJsonName());
                }
            }
            Map<String, Object> def = new LinkedHashMap<>();
            def.put("title", descriptor.getName());
            def.put("type", "object");
            def.put("properties", properties);
            if (!required.isEmpty()) {
                def.put("required", required);
            }
            for (ValidationRuleSource source : sources) {
                source.messageConstraints(descriptor)
                        .ifPresent(m -> celInto(def, m.cel()));
            }
            return def;
        }

        private List<FieldConstraints> constraintsFor(FieldDescriptor field) {
            List<FieldConstraints> collected = new ArrayList<>(sources.size());
            for (ValidationRuleSource source : sources) {
                source.fieldConstraints(field).ifPresent(collected::add);
            }
            return collected;
        }

        private Map<String, Object> fieldSchema(
                FieldDescriptor field, List<FieldConstraints> constraints) {
            if (field.isMapField()) {
                return mapSchema(field, constraints);
            }
            if (field.isRepeated()) {
                return repeatedSchema(field, constraints);
            }
            return scalarSchema(field, constraints);
        }

        private Map<String, Object> scalarSchema(
                FieldDescriptor field, List<FieldConstraints> constraints) {
            Map<String, Object> schema = baseSchema(field);
            for (FieldConstraints c : constraints) {
                merge(schema, overlayFor(field, c));
                celInto(schema, c.cel());
            }
            return schema;
        }

        private Map<String, Object> repeatedSchema(
                FieldDescriptor field, List<FieldConstraints> constraints) {
            Map<String, Object> items = baseSchema(field);
            for (FieldConstraints c : constraints) {
                c.repeated().flatMap(RepeatedConstraints::items).ifPresent(itemRules -> {
                    merge(items, overlayFor(field, itemRules));
                    celInto(items, itemRules.cel());
                });
                // Field-level CEL runs per element in the validator; document it there.
                celInto(items, c.cel());
            }
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("type", "array");
            schema.put("items", items);
            for (FieldConstraints c : constraints) {
                c.repeated().ifPresent(r -> {
                    Map<String, Object> overlay = new LinkedHashMap<>();
                    r.minItems().ifPresent(v -> overlay.put("minItems", v));
                    r.maxItems().ifPresent(v -> overlay.put("maxItems", v));
                    if (r.unique()) {
                        overlay.put("uniqueItems", true);
                    }
                    merge(schema, overlay);
                });
            }
            return schema;
        }

        private Map<String, Object> mapSchema(
                FieldDescriptor field, List<FieldConstraints> constraints) {
            Descriptor entry = field.getMessageType();
            FieldDescriptor keyField = entry.findFieldByNumber(1);
            FieldDescriptor valueField = entry.findFieldByNumber(2);

            Map<String, Object> valueSchema = baseSchema(valueField);
            Map<String, Object> keyOverlay = new LinkedHashMap<>();
            for (FieldConstraints c : constraints) {
                MapConstraints m = c.map().orElse(null);
                if (m != null) {
                    m.values().ifPresent(values -> {
                        merge(valueSchema, overlayFor(valueField, values));
                        celInto(valueSchema, values.cel());
                    });
                    if (keyField.getJavaType() == FieldDescriptor.JavaType.STRING) {
                        m.keys().ifPresent(keys ->
                                merge(keyOverlay, overlayFor(keyField, keys)));
                    }
                }
                // Field-level CEL runs per entry value in the validator.
                celInto(valueSchema, c.cel());
            }

            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("type", "object");
            if (!keyOverlay.isEmpty()) {
                schema.put("propertyNames", keyOverlay);
            }
            schema.put("additionalProperties", valueSchema);
            for (FieldConstraints c : constraints) {
                c.map().ifPresent(m -> {
                    Map<String, Object> overlay = new LinkedHashMap<>();
                    m.minPairs().ifPresent(v -> overlay.put("minProperties", v));
                    m.maxPairs().ifPresent(v -> overlay.put("maxProperties", v));
                    merge(schema, overlay);
                });
            }
            return schema;
        }

        /** Schema for the field's canonical proto3 JSON encoding, before constraints. */
        private Map<String, Object> baseSchema(FieldDescriptor field) {
            return switch (field.getType()) {
                case INT32, SINT32, SFIXED32 -> schemaOf("type", "integer");
                case UINT32, FIXED32 -> schemaOf("type", "integer", "minimum", 0L);
                // Proto3 JSON encodes 64-bit ints as strings and accepts both forms.
                case INT64, SINT64, SFIXED64 -> longSchema("^-?[0-9]+$", false);
                case UINT64, FIXED64 -> longSchema("^[0-9]+$", true);
                case FLOAT, DOUBLE -> schemaOf("type", "number");
                case BOOL -> schemaOf("type", "boolean");
                case STRING -> schemaOf("type", "string");
                case BYTES -> schemaOf("type", "string", "contentEncoding", "base64");
                case ENUM -> enumBase(field.getEnumType());
                case MESSAGE, GROUP -> messageRef(field.getMessageType());
            };
        }

        private Map<String, Object> longSchema(String pattern, boolean unsigned) {
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("type", List.of("integer", "string"));
            schema.put("pattern", pattern);
            if (unsigned) {
                schema.put("minimum", 0L);
            }
            return schema;
        }

        /** Proto3 JSON accepts an enum as its declared name or as a bare number. */
        private Map<String, Object> enumBase(EnumDescriptor type) {
            List<String> names = type.getValues().stream()
                    .map(EnumValueDescriptor::getName)
                    .toList();
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("anyOf", List.of(
                    schemaOf("type", "string", "enum", names),
                    schemaOf("type", "integer")));
            return schema;
        }

        private Map<String, Object> messageRef(Descriptor type) {
            return switch (type.getFullName()) {
                case "google.protobuf.Timestamp" ->
                        schemaOf("type", "string", "format", "date-time");
                case "google.protobuf.Duration" ->
                        schemaOf("type", "string", "pattern", "^-?[0-9]+(\\.[0-9]{1,9})?s$");
                case "google.protobuf.Struct" -> schemaOf("type", "object");
                case "google.protobuf.Value" -> new LinkedHashMap<>();
                case "google.protobuf.ListValue" -> schemaOf("type", "array");
                case "google.protobuf.FieldMask", "google.protobuf.StringValue" ->
                        schemaOf("type", "string");
                case "google.protobuf.BytesValue" ->
                        schemaOf("type", "string", "contentEncoding", "base64");
                case "google.protobuf.BoolValue" -> schemaOf("type", "boolean");
                case "google.protobuf.Int32Value" -> schemaOf("type", "integer");
                case "google.protobuf.UInt32Value" ->
                        schemaOf("type", "integer", "minimum", 0L);
                case "google.protobuf.Int64Value" -> longSchema("^-?[0-9]+$", false);
                case "google.protobuf.UInt64Value" -> longSchema("^[0-9]+$", true);
                case "google.protobuf.FloatValue", "google.protobuf.DoubleValue" ->
                        schemaOf("type", "number");
                case "google.protobuf.Any" -> schemaOf("type", "object");
                default -> {
                    enqueue(type);
                    yield schemaOf("$ref", "#/$defs/" + type.getFullName());
                }
            };
        }

        /** Constraint keywords for the field's type, from one source's constraints. */
        private Map<String, Object> overlayFor(FieldDescriptor field, FieldConstraints c) {
            return switch (field.getJavaType()) {
                case STRING -> c.string().map(Generation::stringOverlay)
                        .orElseGet(LinkedHashMap::new);
                // 64-bit integers are printed as JSON strings by JsonFormat, so their
                // constraints must cover both accepted spellings; 32-bit stay numeric.
                case INT -> c.integral().map(n -> integralOverlay(n, false))
                        .orElseGet(LinkedHashMap::new);
                case LONG -> c.integral().map(n -> integralOverlay(n, true))
                        .orElseGet(LinkedHashMap::new);
                case FLOAT, DOUBLE -> c.floating().map(Generation::floatingOverlay)
                        .orElseGet(LinkedHashMap::new);
                case BOOLEAN -> c.bool().map(Generation::boolOverlay)
                        .orElseGet(LinkedHashMap::new);
                case ENUM -> c.enumeration()
                        .map(e -> enumOverlay(field.getEnumType(), e))
                        .orElseGet(LinkedHashMap::new);
                // Bytes length rules count raw bytes, not base64 text; timestamp and
                // duration bounds are not comparable in JSON Schema. Left unmapped.
                case BYTE_STRING, MESSAGE -> new LinkedHashMap<>();
            };
        }

        private static Map<String, Object> stringOverlay(StringConstraints s) {
            Map<String, Object> o = new LinkedHashMap<>();
            s.constant().ifPresent(v -> o.put("const", v));
            s.len().ifPresent(v -> {
                o.put("minLength", v);
                o.put("maxLength", v);
            });
            s.minLen().ifPresent(v -> o.putIfAbsent("minLength", v));
            s.maxLen().ifPresent(v -> o.putIfAbsent("maxLength", v));

            List<String> patterns = new ArrayList<>();
            s.pattern().ifPresent(patterns::add);
            s.prefix().ifPresent(v -> patterns.add("^" + escapeRegex(v)));
            s.suffix().ifPresent(v -> patterns.add(escapeRegex(v) + "$"));
            s.contains().ifPresent(v -> patterns.add(escapeRegex(v)));
            if (patterns.size() == 1) {
                o.put("pattern", patterns.get(0));
            } else {
                for (String pattern : patterns) {
                    addAllOf(o, schemaOf("pattern", pattern));
                }
            }

            List<Object> nots = new ArrayList<>();
            s.notContains().ifPresent(v -> nots.add(schemaOf("pattern", escapeRegex(v))));
            if (!s.notIn().isEmpty()) {
                nots.add(schemaOf("enum", List.copyOf(s.notIn())));
            }
            if (nots.size() == 1) {
                o.put("not", nots.get(0));
            } else if (nots.size() > 1) {
                o.put("not", schemaOf("anyOf", nots));
            }

            if (!s.in().isEmpty()) {
                o.put("enum", List.copyOf(s.in()));
            }

            boolean formatUsed = false;
            for (StringFormat format : s.formats()) {
                Object keyword = formatKeyword(format);
                if (keyword instanceof String name) {
                    if (!formatUsed) {
                        o.put("format", name);
                        formatUsed = true;
                    } else {
                        addAllOf(o, schemaOf("format", name));
                    }
                } else {
                    addAllOf(o, keyword);
                }
            }
            return o;
        }

        /** JSON Schema format name, or a composite schema for formats without one. */
        private static Object formatKeyword(StringFormat format) {
            return switch (format) {
                case EMAIL -> "email";
                case UUID -> "uuid";
                case HOSTNAME -> "hostname";
                case URI -> "uri";
                case URI_REF -> "uri-reference";
                case IPV4 -> "ipv4";
                case IPV6 -> "ipv6";
                case IP -> schemaOf("anyOf", List.of(
                        schemaOf("format", "ipv4"), schemaOf("format", "ipv6")));
                // No standard JSON Schema format keyword: surface the format name descriptively.
                case TUUID, ULID, ADDRESS, IP_PREFIX, IPV4_PREFIX, IPV6_PREFIX, HOST_AND_PORT,
                        IP_WITH_PREFIXLEN, IPV4_WITH_PREFIXLEN, IPV6_WITH_PREFIXLEN,
                        PROTOBUF_FQN, PROTOBUF_DOT_FQN ->
                        schemaOf("x-pipestream-format", format.ruleId());
            };
        }

        /**
         * Constraint keywords for an integral field. For 64-bit fields ({@code bothSpellings}),
         * JsonFormat prints the value as a JSON string and accepts both the numeric and the
         * string spelling, so every constraint must match both forms: const/in/not_in list the
         * two spellings side by side (the way {@code nameAndNumber} handles enums), and range
         * bounds become an {@code anyOf} of numeric keywords and a decimal-range pattern.
         */
        private static Map<String, Object> integralOverlay(
                IntegralConstraints n, boolean bothSpellings) {
            if (!bothSpellings) {
                Map<String, Object> o = new LinkedHashMap<>();
                n.constant().ifPresent(v -> o.put("const", integral(n, v)));
                n.gte().ifPresent(v -> o.put("minimum", integral(n, v)));
                n.gt().ifPresent(v -> o.put("exclusiveMinimum", integral(n, v)));
                n.lte().ifPresent(v -> o.put("maximum", integral(n, v)));
                n.lt().ifPresent(v -> o.put("exclusiveMaximum", integral(n, v)));
                if (!n.in().isEmpty()) {
                    o.put("enum", n.in().stream().map(v -> integral(n, v)).toList());
                }
                if (!n.notIn().isEmpty()) {
                    o.put("not", schemaOf(
                            "enum", n.notIn().stream().map(v -> integral(n, v)).toList()));
                }
                return o;
            }

            Map<String, Object> o = new LinkedHashMap<>();
            n.constant().ifPresent(v -> o.put("enum", spellings(n, v)));
            if (!n.in().isEmpty()) {
                List<Object> allowed = new ArrayList<>();
                n.in().forEach(v -> allowed.addAll(spellings(n, v)));
                merge(o, schemaOf("enum", allowed));
            }
            if (!n.notIn().isEmpty()) {
                List<Object> forbidden = new ArrayList<>();
                n.notIn().forEach(v -> forbidden.addAll(spellings(n, v)));
                o.put("not", schemaOf("enum", forbidden));
            }

            BigInteger lo = null;
            BigInteger hi = null;
            if (n.gte().isPresent()) {
                lo = big(n, n.gte().getAsLong());
            }
            if (n.gt().isPresent()) {
                BigInteger candidate = big(n, n.gt().getAsLong()).add(BigInteger.ONE);
                lo = lo == null ? candidate : lo.max(candidate);
            }
            if (n.lte().isPresent()) {
                hi = big(n, n.lte().getAsLong());
            }
            if (n.lt().isPresent()) {
                BigInteger candidate = big(n, n.lt().getAsLong()).subtract(BigInteger.ONE);
                hi = hi == null ? candidate : hi.min(candidate);
            }
            if (lo != null || hi != null) {
                if (n.unsigned() && lo == null) {
                    lo = BigInteger.ZERO; // unsigned fields are implicitly bounded below
                }
                Map<String, Object> numeric = new LinkedHashMap<>();
                numeric.put("type", "integer");
                n.gte().ifPresent(v -> numeric.put("minimum", integral(n, v)));
                n.gt().ifPresent(v -> numeric.put("exclusiveMinimum", integral(n, v)));
                n.lte().ifPresent(v -> numeric.put("maximum", integral(n, v)));
                n.lt().ifPresent(v -> numeric.put("exclusiveMaximum", integral(n, v)));
                Map<String, Object> stringForm = schemaOf(
                        "type", "string",
                        "pattern", DecimalRangePattern.range(lo, hi));
                merge(o, schemaOf("anyOf", List.of(numeric, stringForm)));
            }
            return o;
        }

        /** Both accepted JSON spellings of a 64-bit integer: number and decimal string. */
        private static List<Object> spellings(IntegralConstraints rules, long value) {
            return List.of(integral(rules, value), decimal(rules, value));
        }

        private static String decimal(IntegralConstraints rules, long value) {
            return rules.unsigned() ? Long.toUnsignedString(value) : Long.toString(value);
        }

        private static BigInteger big(IntegralConstraints rules, long value) {
            return rules.unsigned()
                    ? new BigInteger(Long.toUnsignedString(value))
                    : BigInteger.valueOf(value);
        }

        /** Renders an unsigned 64-bit value that overflows long as a BigInteger. */
        private static Object integral(IntegralConstraints rules, long value) {
            if (rules.unsigned() && value < 0) {
                return new BigInteger(Long.toUnsignedString(value));
            }
            return value;
        }

        private static Map<String, Object> floatingOverlay(FloatingConstraints n) {
            Map<String, Object> o = new LinkedHashMap<>();
            n.constant().ifPresent(v -> o.put("const", v));
            n.gte().ifPresent(v -> o.put("minimum", v));
            n.gt().ifPresent(v -> o.put("exclusiveMinimum", v));
            n.lte().ifPresent(v -> o.put("maximum", v));
            n.lt().ifPresent(v -> o.put("exclusiveMaximum", v));
            if (!n.in().isEmpty()) {
                o.put("enum", List.copyOf(n.in()));
            }
            if (!n.notIn().isEmpty()) {
                o.put("not", schemaOf("enum", List.copyOf(n.notIn())));
            }
            // finite: JSON numbers are always finite — nothing to emit.
            return o;
        }

        private static Map<String, Object> boolOverlay(BoolConstraints b) {
            Map<String, Object> o = new LinkedHashMap<>();
            b.constant().ifPresent(v -> o.put("const", v));
            return o;
        }

        private Map<String, Object> enumOverlay(EnumDescriptor type, EnumConstraints e) {
            Map<String, Object> o = new LinkedHashMap<>();
            e.constant().ifPresent(v -> o.put("enum", nameAndNumber(type, v)));
            if (e.definedOnly()) {
                List<String> names = type.getValues().stream()
                        .map(EnumValueDescriptor::getName)
                        .toList();
                List<Object> numbers = type.getValues().stream()
                        .map(v -> (Object) (long) v.getNumber())
                        .toList();
                // ANDs with the open base via allOf on merge collision.
                o.put("anyOf", List.of(
                        schemaOf("type", "string", "enum", names),
                        schemaOf("type", "integer", "enum", numbers)));
            }
            if (!e.in().isEmpty()) {
                List<Object> allowed = new ArrayList<>();
                e.in().forEach(v -> allowed.addAll(nameAndNumber(type, v)));
                merge(o, schemaOf("enum", allowed));
            }
            if (!e.notIn().isEmpty()) {
                List<Object> forbidden = new ArrayList<>();
                e.notIn().forEach(v -> forbidden.addAll(nameAndNumber(type, v)));
                o.put("not", schemaOf("enum", forbidden));
            }
            return o;
        }

        /** Both accepted JSON spellings of an enum number: declared name and number. */
        private static List<Object> nameAndNumber(EnumDescriptor type, int number) {
            EnumValueDescriptor value = type.findValueByNumber(number);
            return value == null
                    ? List.of((long) number)
                    : List.of(value.getName(), (long) number);
        }

        private static void celInto(Map<String, Object> schema, List<CelConstraint> cel) {
            if (cel.isEmpty()) {
                return;
            }
            @SuppressWarnings("unchecked")
            List<Object> rules = (List<Object>) schema
                    .computeIfAbsent(CEL_KEYWORD, k -> new ArrayList<>());
            for (CelConstraint rule : cel) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("id", rule.id());
                entry.put("expression", rule.expression());
                if (!rule.message().isBlank()) {
                    entry.put("message", rule.message());
                }
                rules.add(entry);
            }
        }

        /**
         * Merges constraint keywords into a schema. A keyword the schema already uses
         * moves into {@code allOf} instead of overwriting — conjunction is always the
         * correct combination for constraints from multiple sources.
         */
        private static void merge(Map<String, Object> schema, Map<String, Object> overlay) {
            for (Map.Entry<String, Object> entry : overlay.entrySet()) {
                if (schema.containsKey(entry.getKey())) {
                    addAllOf(schema, schemaOf(entry.getKey(), entry.getValue()));
                } else {
                    schema.put(entry.getKey(), entry.getValue());
                }
            }
        }

        private static void addAllOf(Map<String, Object> schema, Object subschema) {
            @SuppressWarnings("unchecked")
            List<Object> allOf = (List<Object>) schema
                    .computeIfAbsent("allOf", k -> new ArrayList<>());
            allOf.add(subschema);
        }

        private static Map<String, Object> schemaOf(Object... pairs) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < pairs.length; i += 2) {
                map.put((String) pairs[i], pairs[i + 1]);
            }
            return map;
        }

        private static String escapeRegex(String literal) {
            StringBuilder sb = new StringBuilder(literal.length() + 4);
            for (int i = 0; i < literal.length(); i++) {
                char c = literal.charAt(i);
                if ("\\^$.|?*+()[]{}".indexOf(c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
            return sb.toString();
        }
    }

    /** Marker parked in $defs while a message is queued, replaced by its real schema. */
    private enum PendingDef {
        INSTANCE
    }
}

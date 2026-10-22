package ai.protomolt.proto.http.jsonschema;

import ai.protomolt.proto.validate.model.BoolConstraints;
import ai.protomolt.proto.validate.model.EnumConstraints;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.FloatingConstraints;
import ai.protomolt.proto.validate.model.IgnoreMode;
import ai.protomolt.proto.validate.model.IntegralConstraints;
import ai.protomolt.proto.validate.model.StringConstraints;
import ai.protomolt.proto.validate.model.StringFormat;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static ai.protomolt.proto.http.jsonschema.ProtoTypeSchemas.addAllOf;
import static ai.protomolt.proto.http.jsonschema.ProtoTypeSchemas.celInto;
import static ai.protomolt.proto.http.jsonschema.ProtoTypeSchemas.merge;
import static ai.protomolt.proto.http.jsonschema.ProtoTypeSchemas.schemaOf;

/**
 * Package-private translation of neutral validation constraints and runtime-only rule
 * obligations into JSON Schema keyword overlays for {@link ProtoJsonSchemaGenerator}.
 *
 * <p>Decomposes scalar, integral (32-bit vs 64-bit dual-spelling), floating, boolean,
 * enum (mixed vs type-uniform), repeated/map cardinality, and runtime-rule diagnostics
 * into cohesive helper methods without altering schema output.
 */
final class ConstraintOverlays {

    static final String RUNTIME_RULES_KEYWORD = "x-protomolt-runtime-rules";

    private ConstraintOverlays() {
    }

    /**
     * Checks whether a field's containing message or ignore mode short-circuits static
     * validation keywords into an immediate {@code x-protomolt-runtime-rules} schema.
     * Returns {@code null} when normal field constraint translation should proceed.
     */
    static Map<String, Object> earlyFieldRuntimeSchema(
            FieldDescriptor field,
            List<ValidationRuleSource> sources,
            List<FieldConstraints> constraints) {
        for (ValidationRuleSource source : sources) {
            var message = source.messageConstraints(field.getContainingType());
            if (message.isPresent()) {
                if (!message.get().skipWhen().isBlank()) {
                    return Map.of(RUNTIME_RULES_KEYWORD, List.of("skip-when"));
                }
                if (message.get().oneofs().stream().anyMatch(o -> o.fields().contains(field.getName()))) {
                    return Map.of(RUNTIME_RULES_KEYWORD, List.of("message-oneof"));
                }
            }
        }
        if (constraints.stream().anyMatch(c -> c.ignore() == IgnoreMode.ALWAYS)) {
            return Map.of(RUNTIME_RULES_KEYWORD, List.of("ignore-always"));
        }
        if (constraints.stream().anyMatch(c -> c.ignore() == IgnoreMode.IF_ZERO_VALUE)) {
            // A plain bound would reject zero values that the runtime deliberately skips.
            return Map.of(RUNTIME_RULES_KEYWORD, List.of("ignore-if-zero-value"));
        }
        return null;
    }

    /** Collects non-short-circuiting runtime-only rule obligations on {@code field}. */
    static List<String> fieldRuntimeRules(FieldDescriptor field, List<FieldConstraints> constraints) {
        List<String> runtime = new ArrayList<>();
        for (FieldConstraints c : constraints) {
            if (c.bytes().isPresent()) runtime.add("bytes");
            if (c.timestamp().isPresent()) runtime.add("timestamp");
            if (c.duration().isPresent()) runtime.add("duration");
            if (c.any().isPresent()) runtime.add("any");
            if (c.fieldMask().isPresent()) runtime.add("field-mask");
            if (c.taxonomy().isPresent()) runtime.add("taxonomy");
            if (!c.conceptSchemes().isEmpty()) runtime.add("concept-schemes");
            if (c.inspectOnly()) runtime.add("inspect-only");
        }
        if (!field.hasPresence() && constraints.stream().anyMatch(FieldConstraints::required)) {
            // A protobuf implicit-presence scalar is absent at its zero value even if the
            // JSON property was supplied; OAS required checks only property existence.
            runtime.add(field.isRepeated() ? "required-empty-collection" : "required-implicit-zero");
        }
        return runtime.stream().distinct().toList();
    }

    /** Populates message-level CEL and returns distinct message-level runtime rule names. */
    static List<String> collectMessageRuntimeRules(
            Descriptor descriptor,
            List<ValidationRuleSource> sources,
            Map<String, Object> schema) {
        List<String> runtime = new ArrayList<>();
        for (ValidationRuleSource source : sources) {
            source.messageConstraints(descriptor).ifPresent(c -> {
                celInto(schema, c.cel());
                if (!c.oneofs().isEmpty()) runtime.add("message-oneof");
                if (!c.requiredOneofs().isEmpty()) runtime.add("required-oneof");
                if (!c.skipWhen().isBlank()) runtime.add("skip-when");
            });
        }
        return runtime.stream().distinct().toList();
    }

    /** Constraint keywords for the field's type, from one source's constraints. */
    static Map<String, Object> overlayFor(
            FieldDescriptor field,
            FieldConstraints c,
            boolean validationOnly,
            boolean typeUniformEnums) {
        if (validationOnly && c.ignore() != IgnoreMode.UNSPECIFIED) {
            return schemaOf(RUNTIME_RULES_KEYWORD, List.of(
                    c.ignore() == IgnoreMode.ALWAYS ? "ignore-always" : "ignore-if-zero-value"));
        }
        return switch (field.getJavaType()) {
            case STRING -> c.string().map(ConstraintOverlays::stringOverlay)
                    .orElseGet(LinkedHashMap::new);
            // 64-bit integers are printed as JSON strings by JsonFormat, so their
            // constraints must cover both accepted spellings; 32-bit stay numeric.
            case INT -> c.integral().map(n -> integralOverlay(n, false, typeUniformEnums))
                    .orElseGet(LinkedHashMap::new);
            case LONG -> c.integral().map(n -> integralOverlay(n, true, typeUniformEnums))
                    .orElseGet(LinkedHashMap::new);
            case FLOAT, DOUBLE -> c.floating().map(ConstraintOverlays::floatingOverlay)
                    .orElseGet(LinkedHashMap::new);
            case BOOLEAN -> c.bool().map(ConstraintOverlays::boolOverlay)
                    .orElseGet(LinkedHashMap::new);
            case ENUM -> c.enumeration()
                    .map(e -> enumOverlay(field.getEnumType(), e, typeUniformEnums))
                    .orElseGet(LinkedHashMap::new);
            // Bytes length rules count raw bytes, not base64 text; timestamp and
            // duration bounds are not comparable in JSON Schema. Left unmapped.
            case BYTE_STRING, MESSAGE -> new LinkedHashMap<>();
        };
    }

    static void applyRepeatedBounds(Map<String, Object> schema, List<FieldConstraints> constraints) {
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
    }

    static void applyMapBounds(Map<String, Object> schema, List<FieldConstraints> constraints) {
        for (FieldConstraints c : constraints) {
            c.map().ifPresent(m -> {
                Map<String, Object> overlay = new LinkedHashMap<>();
                m.minPairs().ifPresent(v -> overlay.put("minProperties", v));
                m.maxPairs().ifPresent(v -> overlay.put("maxProperties", v));
                merge(schema, overlay);
            });
        }
    }

    static Map<String, Object> stringOverlay(StringConstraints s) {
        Map<String, Object> o = new LinkedHashMap<>();
        s.constant().ifPresent(v -> o.put("const", v));
        s.len().ifPresent(v -> {
            o.put("minLength", v);
            o.put("maxLength", v);
        });
        s.minLen().ifPresent(v -> o.putIfAbsent("minLength", v));
        s.maxLen().ifPresent(v -> o.putIfAbsent("maxLength", v));

        applyStringPatterns(o, s);
        applyStringNegations(o, s);

        if (!s.in().isEmpty()) {
            o.put("enum", List.copyOf(s.in()));
        }

        applyStringFormats(o, s);
        return o;
    }

    private static void applyStringPatterns(Map<String, Object> o, StringConstraints s) {
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
    }

    private static void applyStringNegations(Map<String, Object> o, StringConstraints s) {
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
    }

    private static void applyStringFormats(Map<String, Object> o, StringConstraints s) {
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
    }

    /** JSON Schema format name, or a composite schema for formats without one. */
    private static Object formatKeyword(StringFormat format) {
        return switch (format) {
            case EMAIL -> "email";
            case UUID -> "uuid";
            case HOSTNAME -> "hostname";
            case URI -> "uri";
            case URI_REF -> "uri-reference";
            case DATE -> "date";
            case DATE_TIME -> "date-time";
            case IPV4 -> "ipv4";
            case IPV6 -> "ipv6";
            case IP -> schemaOf("anyOf", List.of(
                    schemaOf("format", "ipv4"), schemaOf("format", "ipv6")));
            // No standard JSON Schema format keyword: surface the format name descriptively.
            case TUUID, ULID, ADDRESS, IP_PREFIX, IPV4_PREFIX, IPV6_PREFIX, HOST_AND_PORT,
                    IP_WITH_PREFIXLEN, IPV4_WITH_PREFIXLEN, IPV6_WITH_PREFIXLEN,
                    PROTOBUF_FQN, PROTOBUF_DOT_FQN, MIME_TYPE, LANGUAGE_TAG, CURRENCY_CODE,
                    PHONE_NUMBER, SHA256_HEX, SHA1_HEX, HEX, BASE64, SLUG, REGION_CODE,
                    PATH_SAFE_NAME, GTIN, DECIMAL, ENDPOINT_ADDRESS, IDENTIFIER ->
                    schemaOf("x-protomolt-format", format.ruleId());
        };
    }

    /**
     * Constraint keywords for an integral field. For 64-bit fields ({@code bothSpellings}),
     * JsonFormat prints the value as a JSON string and accepts both the numeric and the
     * string spelling, so every constraint must match both forms: const/in/not_in list the
     * two spellings side by side (the way {@code nameAndNumber} handles enums), and range
     * bounds become an {@code anyOf} of numeric keywords and a decimal-range pattern. When
     * {@code typeUniformEnums} is set, the two spellings render as separate type-uniform
     * branches instead of one mixed-type enum array.
     */
    static Map<String, Object> integralOverlay(
            IntegralConstraints n, boolean bothSpellings, boolean typeUniformEnums) {
        if (!bothSpellings) {
            return numericIntegralOverlay(n);
        }
        Map<String, Object> o = new LinkedHashMap<>();
        applyLongMembership(o, n, typeUniformEnums);
        applyLongBounds(o, n);
        return o;
    }

    private static Map<String, Object> numericIntegralOverlay(IntegralConstraints n) {
        Map<String, Object> o = new LinkedHashMap<>();
        n.constant().ifPresent(v -> o.put("const", integral(n, v)));
        BigInteger lower = n.gt().isPresent() ? big(n, n.gt().getAsLong())
                : n.gte().isPresent() ? big(n, n.gte().getAsLong()) : null;
        BigInteger upper = n.lt().isPresent() ? big(n, n.lt().getAsLong())
                : n.lte().isPresent() ? big(n, n.lte().getAsLong()) : null;
        if (lower != null && upper != null && upper.compareTo(lower) < 0) {
            Map<String, Object> low = new LinkedHashMap<>();
            Map<String, Object> high = new LinkedHashMap<>();
            addIntegralBounds(low, n, true);
            addIntegralBounds(high, n, true, false);
            o.put("anyOf", List.of(low, high));
        } else {
            addIntegralBounds(o, n, false);
        }
        if (!n.in().isEmpty()) {
            o.put("enum", n.in().stream().map(v -> integral(n, v)).toList());
        }
        if (!n.notIn().isEmpty()) {
            o.put("not", schemaOf(
                    "enum", n.notIn().stream().map(v -> integral(n, v)).toList()));
        }
        return o;
    }

    private static void applyLongMembership(
            Map<String, Object> o, IntegralConstraints n, boolean typeUniformEnums) {
        if (typeUniformEnums) {
            // Both spellings stay accepted, but each enum array is single-type:
            // strict tool-schema validators reject mixed-type enums outright.
            n.constant().ifPresent(v -> merge(o, schemaOf("anyOf", List.of(
                    schemaOf("enum", List.of(integral(n, v))),
                    schemaOf("enum", List.of(decimal(n, v)))))));
            if (!n.in().isEmpty()) {
                merge(o, schemaOf("anyOf", List.of(
                        schemaOf("type", "integer", "enum",
                                n.in().stream().map(v -> integral(n, v)).toList()),
                        schemaOf("type", "string", "enum",
                                n.in().stream().map(v -> decimal(n, v)).toList()))));
            }
            if (!n.notIn().isEmpty()) {
                merge(o, schemaOf("allOf", List.of(
                        schemaOf("not", schemaOf("enum",
                                n.notIn().stream().map(v -> integral(n, v)).toList())),
                        schemaOf("not", schemaOf("enum",
                                n.notIn().stream().map(v -> decimal(n, v)).toList())))));
            }
            return;
        }
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
    }

    private static void applyLongBounds(Map<String, Object> o, IntegralConstraints n) {
        BigInteger rawLower = n.gt().isPresent() ? big(n, n.gt().getAsLong())
                : n.gte().isPresent() ? big(n, n.gte().getAsLong()) : null;
        BigInteger rawUpper = n.lt().isPresent() ? big(n, n.lt().getAsLong())
                : n.lte().isPresent() ? big(n, n.lte().getAsLong()) : null;
        BigInteger lo = null;
        BigInteger hi = null;
        if (n.gt().isPresent()) {
            lo = big(n, n.gt().getAsLong()).add(BigInteger.ONE);
        } else if (n.gte().isPresent()) {
            lo = big(n, n.gte().getAsLong());
        }
        if (n.lt().isPresent()) {
            hi = big(n, n.lt().getAsLong()).subtract(BigInteger.ONE);
        } else if (n.lte().isPresent()) {
            hi = big(n, n.lte().getAsLong());
        }
        if (lo == null && hi == null) {
            return;
        }
        if (n.unsigned() && lo == null) {
            lo = BigInteger.ZERO; // unsigned fields are implicitly bounded below
        }
        Map<String, Object> numeric = new LinkedHashMap<>();
        numeric.put("type", "integer");
        addIntegralBounds(numeric, n, false);
        if (rawLower != null && rawUpper != null && rawUpper.compareTo(rawLower) < 0) {
            Map<String, Object> lowerNumeric = schemaOf("type", "integer");
            Map<String, Object> upperNumeric = schemaOf("type", "integer");
            addIntegralBounds(lowerNumeric, n, true);
            addIntegralBounds(upperNumeric, n, true, false);
            merge(o, schemaOf("anyOf", List.of(lowerNumeric, upperNumeric,
                    schemaOf("type", "string", "pattern", DecimalRangePattern.range(lo, null)),
                    schemaOf("type", "string", "pattern", DecimalRangePattern.range(null, hi)))));
        } else {
            Map<String, Object> stringForm = schemaOf(
                    "type", "string", "pattern", DecimalRangePattern.range(lo, hi));
            merge(o, schemaOf("anyOf", List.of(numeric, stringForm)));
        }
    }

    private static void addIntegralBounds(
            Map<String, Object> out, IntegralConstraints n, boolean lowerOnly) {
        addIntegralBounds(out, n, lowerOnly, true);
    }

    private static void addIntegralBounds(
            Map<String, Object> out, IntegralConstraints n, boolean oneSide, boolean lower) {
        if (!oneSide || lower) {
            if (n.gt().isPresent()) out.put("exclusiveMinimum", integral(n, n.gt().getAsLong()));
            else n.gte().ifPresent(v -> out.put("minimum", integral(n, v)));
        }
        if (!oneSide || !lower) {
            if (n.lt().isPresent()) out.put("exclusiveMaximum", integral(n, n.lt().getAsLong()));
            else n.lte().ifPresent(v -> out.put("maximum", integral(n, v)));
        }
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

    static Map<String, Object> floatingOverlay(FloatingConstraints n) {
        Map<String, Object> o = new LinkedHashMap<>();
        n.constant().ifPresent(v -> o.put("const", v));
        double lower = n.gt().isPresent() ? n.gt().getAsDouble()
                : n.gte().isPresent() ? n.gte().getAsDouble() : Double.NaN;
        double upper = n.lt().isPresent() ? n.lt().getAsDouble()
                : n.lte().isPresent() ? n.lte().getAsDouble() : Double.NaN;
        if (!Double.isNaN(lower) && !Double.isNaN(upper) && upper < lower) {
            Map<String, Object> low = new LinkedHashMap<>();
            Map<String, Object> high = new LinkedHashMap<>();
            if (n.gt().isPresent()) low.put("exclusiveMinimum", lower);
            else low.put("minimum", lower);
            if (n.lt().isPresent()) high.put("exclusiveMaximum", upper);
            else high.put("maximum", upper);
            o.put("anyOf", List.of(low, high));
        } else {
            if (n.gt().isPresent()) o.put("exclusiveMinimum", lower);
            else n.gte().ifPresent(v -> o.put("minimum", v));
            if (n.lt().isPresent()) o.put("exclusiveMaximum", upper);
            else n.lte().ifPresent(v -> o.put("maximum", v));
        }
        if (!n.in().isEmpty()) {
            o.put("enum", List.copyOf(n.in()));
        }
        if (!n.notIn().isEmpty()) {
            o.put("not", schemaOf("enum", List.copyOf(n.notIn())));
        }
        // finite: JSON numbers are always finite — nothing to emit.
        return o;
    }

    static Map<String, Object> boolOverlay(BoolConstraints b) {
        Map<String, Object> o = new LinkedHashMap<>();
        b.constant().ifPresent(v -> o.put("const", v));
        return o;
    }

    static Map<String, Object> enumOverlay(
            EnumDescriptor type, EnumConstraints e, boolean typeUniformEnums) {
        if (typeUniformEnums) {
            return typeUniformEnumOverlay(type, e);
        }
        return mixedEnumOverlay(type, e);
    }

    private static Map<String, Object> typeUniformEnumOverlay(
            EnumDescriptor type, EnumConstraints e) {
        Map<String, Object> o = new LinkedHashMap<>();
        // Type-uniform branches: strict tool-schema validators reject a single
        // enum array that mixes names and numbers.
        e.constant().ifPresent(v -> merge(o, enumSpellings(type, List.of(v), false)));
        if (e.definedOnly()) {
            // ANDs with the open base via allOf on merge collision.
            merge(o, definedEnumAnyOf(type));
        }
        if (!e.in().isEmpty()) {
            merge(o, enumSpellings(type, e.in(), false));
        }
        if (!e.notIn().isEmpty()) {
            merge(o, enumSpellings(type, e.notIn(), true));
        }
        return o;
    }

    private static Map<String, Object> mixedEnumOverlay(
            EnumDescriptor type, EnumConstraints e) {
        Map<String, Object> o = new LinkedHashMap<>();
        e.constant().ifPresent(v -> o.put("enum", nameAndNumber(type, v)));
        if (e.definedOnly()) {
            // ANDs with the open base via allOf on merge collision.
            o.put("anyOf", definedEnumAnyOf(type).get("anyOf"));
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

    private static Map<String, Object> definedEnumAnyOf(EnumDescriptor type) {
        List<String> names = type.getValues().stream()
                .map(EnumValueDescriptor::getName)
                .toList();
        List<Object> numbers = type.getValues().stream()
                .map(v -> (Object) (long) v.getNumber())
                .toList();
        return schemaOf("anyOf", List.of(
                schemaOf("type", "string", "enum", names),
                schemaOf("type", "integer", "enum", numbers)));
    }

    /**
     * The membership constraint over {@code values} as type-uniform branches: one
     * string-typed branch of the declared names, one integer-typed branch of the
     * numbers, combined with {@code anyOf} (or, when {@code negated}, one
     * {@code not}-wrapped branch per spelling combined with {@code allOf}). A value
     * with no declared name appears only in the numeric branch.
     */
    private static Map<String, Object> enumSpellings(
            EnumDescriptor type, Collection<Integer> values, boolean negated) {
        List<Object> names = new ArrayList<>();
        for (int value : values) {
            EnumValueDescriptor declared = type.findValueByNumber(value);
            if (declared != null) {
                names.add(declared.getName());
            }
        }
        List<Object> numbers = values.stream().map(v -> (Object) (long) v).toList();
        List<Object> branches = new ArrayList<>();
        if (!names.isEmpty()) {
            branches.add(negated
                    ? schemaOf("not", schemaOf("enum", names))
                    : schemaOf("type", "string", "enum", names));
        }
        branches.add(negated
                ? schemaOf("not", schemaOf("enum", numbers))
                : schemaOf("type", "integer", "enum", numbers));
        return schemaOf(negated ? "allOf" : "anyOf", branches);
    }

    /** Both accepted JSON spellings of an enum number: declared name and number. */
    private static List<Object> nameAndNumber(EnumDescriptor type, int number) {
        EnumValueDescriptor value = type.findValueByNumber(number);
        return value == null
                ? List.of((long) number)
                : List.of(value.getName(), (long) number);
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

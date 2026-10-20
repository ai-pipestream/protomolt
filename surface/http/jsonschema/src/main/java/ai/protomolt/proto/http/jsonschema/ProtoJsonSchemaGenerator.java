package ai.protomolt.proto.http.jsonschema;

import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.IgnoreMode;
import ai.protomolt.proto.validate.model.MapConstraints;
import ai.protomolt.proto.validate.model.RepeatedConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static ai.protomolt.proto.http.jsonschema.ProtoTypeSchemas.celInto;
import static ai.protomolt.proto.http.jsonschema.ProtoTypeSchemas.merge;

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
 * surfaced verbatim under the {@code x-protomolt-cel} vendor keyword.
 *
 * <p>{@link #generateRooted(Descriptor)} produces the same schema with the root message
 * described in place rather than behind a reference. Tool-calling clients, MCP among
 * them, require the root to declare {@code "type": "object"} and read its properties
 * from the top level; a reference at the root leaves them nothing to read.
 *
 * <p>Known gaps (documented, not silent): bytes length rules are not mapped (JSON
 * carries base64 text, not raw lengths), and timestamp/duration bounds are not mapped
 * (JSON Schema cannot compare {@code date-time} values). Floating {@code finite} needs
 * no mapping — JSON numbers are always finite.
 */
public final class ProtoJsonSchemaGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<ValidationRuleSource> sources;
    private final boolean typeUniformEnums;

    private ProtoJsonSchemaGenerator(List<ValidationRuleSource> sources, boolean typeUniformEnums) {
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        this.typeUniformEnums = typeUniformEnums;
    }

    /** Uses the default rule-source chain ({@link ValidationRuleSources#defaults()}). */
    public static ProtoJsonSchemaGenerator create() {
        return new ProtoJsonSchemaGenerator(ValidationRuleSources.defaults(), false);
    }

    /** As {@link #create()} but with an explicit rule-source chain. */
    public static ProtoJsonSchemaGenerator create(List<ValidationRuleSource> sources) {
        return new ProtoJsonSchemaGenerator(sources, false);
    }

    /**
     * As {@link #create()} but enum and 64-bit integer membership constraints render as
     * type-uniform branches (one string-typed, one number-typed) instead of a single
     * mixed-type enum array. The accepted value sets are identical; the split exists for
     * tool-schema validators that reject mixed-type enums.
     */
    public static ProtoJsonSchemaGenerator createTypeUniform() {
        return new ProtoJsonSchemaGenerator(ValidationRuleSources.defaults(), true);
    }

    /** Generates the schema as an ordered JSON-shaped map (maps, lists, scalars). */
    public Map<String, Object> generate(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return new Generation().run(descriptor);
    }

    /**
     * Validation keywords without a protobuf JSON type schema. Other schema dialects
     * can reuse the rule translation while retaining their own encoding of types.
     * Conditional and unrepresentable rules are identified as runtime obligations.
     */
    public Map<String, Object> fieldValidationSchema(FieldDescriptor field) {
        Objects.requireNonNull(field, "field");
        Generation generation = new Generation(true);
        List<FieldConstraints> constraints = generation.constraintsFor(field);
        Map<String, Object> early =
                ConstraintOverlays.earlyFieldRuntimeSchema(field, sources, constraints);
        if (early != null) {
            return early;
        }
        List<String> runtime = ConstraintOverlays.fieldRuntimeRules(field, constraints);
        Map<String, Object> schema = generation.fieldSchema(field, constraints);
        if (!runtime.isEmpty()) {
            schema.put(ConstraintOverlays.RUNTIME_RULES_KEYWORD, runtime);
        }
        return schema;
    }

    /** Message validation shared by JSON Schema consumers and OpenAPI publication. */
    public Map<String, Object> messageValidationSchema(Descriptor descriptor) {
        Map<String, Object> schema = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();
        Generation generation = new Generation();
        for (FieldDescriptor field : descriptor.getFields()) {
            List<FieldConstraints> rules = generation.constraintsFor(field);
            if (field.isRequired() || (rules.stream().anyMatch(FieldConstraints::required)
                    && rules.stream().noneMatch(c -> c.ignore() == IgnoreMode.ALWAYS))) {
                required.add(field.getJsonName());
            }
        }
        if (!required.isEmpty()) {
            schema.put("required", required);
        }
        List<String> runtime =
                ConstraintOverlays.collectMessageRuntimeRules(descriptor, sources, schema);
        if (runtime.contains("skip-when")) {
            schema.remove("required");
        }
        if (!runtime.isEmpty()) {
            schema.put(ConstraintOverlays.RUNTIME_RULES_KEYWORD, runtime);
        }
        return schema;
    }

    /**
     * The schema with the root message described at the top level.
     *
     * <p>Identical to {@link #generate(Descriptor)} except that the root's own
     * {@code type}, {@code properties} and {@code required} sit on the returned schema
     * instead of behind a {@code $ref}. Nested types stay in {@code $defs} and keep their
     * references, so recursion is still safe and each type is still defined once.
     *
     * <p>This is the form a tool manifest needs: a client that cannot follow a root
     * reference sees an object schema it can read directly.
     */
    public Map<String, Object> generateRooted(Descriptor descriptor) {
        Map<String, Object> schema = generate(descriptor);
        Object defsValue = schema.get("$defs");
        if (!(defsValue instanceof Map<?, ?> defs)) {
            return schema;
        }
        Object rootDef = defs.get(descriptor.getFullName());
        if (!(rootDef instanceof Map<?, ?> root)) {
            return schema;
        }
        Map<String, Object> rooted = new LinkedHashMap<>();
        rooted.put("$schema", schema.get("$schema"));
        root.forEach((key, value) -> rooted.put(String.valueOf(key), value));
        // $defs keeps every type, the root included. A message can reach itself, directly
        // or through a nested one, and those references point into $defs; removing the
        // root to avoid stating it twice would leave them pointing at nothing.
        rooted.put("$defs", defs);
        return rooted;
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

        private final boolean validationOnly;
        private final Map<String, Object> defs = new LinkedHashMap<>();
        private final Deque<Descriptor> queue = new ArrayDeque<>();

        Generation() {
            this(false);
        }

        Generation(boolean validationOnly) {
            this.validationOnly = validationOnly;
        }

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
            ConstraintOverlays.applyRepeatedBounds(schema, constraints);
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
            ConstraintOverlays.applyMapBounds(schema, constraints);
            return schema;
        }

        private Map<String, Object> baseSchema(FieldDescriptor field) {
            return ProtoTypeSchemas.baseSchema(field, validationOnly, this::enqueue);
        }

        private Map<String, Object> overlayFor(FieldDescriptor field, FieldConstraints c) {
            return ConstraintOverlays.overlayFor(field, c, validationOnly, typeUniformEnums);
        }
    }

    /** Marker parked in $defs while a message is queued, replaced by its real schema. */
    private enum PendingDef {
        INSTANCE
    }
}

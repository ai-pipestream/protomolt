package ai.protomolt.proto.validate;

import ai.protomolt.proto.cel.CelCompilationException;
import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.validate.cel.ValidationCelFunctions;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.IgnoreMode;
import ai.protomolt.proto.validate.model.MapConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.model.RepeatedConstraints;
import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import dev.cel.bundle.Cel;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.CelType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static ai.protomolt.proto.validate.CelValues.celListValue;
import static ai.protomolt.proto.validate.CelValues.celMapValue;
import static ai.protomolt.proto.validate.CelValues.celScalar;
import static ai.protomolt.proto.validate.CelValues.evalCel;
import static ai.protomolt.proto.validate.ValueChecks.applyFieldConstraints;
import static ai.protomolt.proto.validate.ValueChecks.compiledPattern;
import static ai.protomolt.proto.validate.Violations.violation;

/**
 * Validates protobuf messages against constraint annotations. The validator core
 * evaluates a neutral {@link FieldConstraints}/{@link MessageConstraints} model; the
 * mapping from a specific annotation dialect (Pipestream {@code validate.v1},
 * {@code buf.validate}, …) onto that model lives behind {@link ValidationRuleSource}.
 * Standard constraints run in-process; custom rules use CEL with {@code this}.
 *
 * <p>By default the built-in Pipestream reader is used plus any {@link ValidationRuleSource}
 * discovered on the classpath via {@link java.util.ServiceLoader}. Every configured source
 * is consulted per field/message and all violations are merged.
 *
 * <p>The translated rule model is compiled once per message type and cached: regex patterns
 * and CEL programs are compiled eagerly when a descriptor's rules are first assembled, so
 * schema errors ({@link RuleCompilationException}) surface deterministically — even for
 * fields the validated message leaves unset. Value-dependent failures (a CEL runtime error,
 * undecodable bytes) throw {@link RuleEvaluationException} instead.
 *
 * <p>Presence semantics: standard rules run only when the field is present (proto3
 * semantics — non-zero/non-empty, or {@code hasField} for explicit presence). Repeated
 * and map fields are the exception: collection rules ({@code repeated.min_items},
 * {@code map.min_pairs}, …) also apply when the collection is empty.
 *
 * <p>Violation paths use protobuf field names, {@code [i]} subscripts for repeated
 * elements, {@code ["key"]} subscripts for map entries, and a {@code #key} suffix for
 * violations against a map key itself.
 *
 * <p>This class owns rule compilation and the descriptor walk — which values are reached, at
 * which path, and whether a collection or nesting level is entered at all. The checks a single
 * value then faces live in {@link ValueChecks}, the CEL bindings and failure classification in
 * {@link CelValues}, and the well-known conversions both of those share in
 * {@link WellKnownValues}.
 */
public final class ProtoValidator {

    private static final String TREE_PATH_TYPE = "ai.protomolt.proto.types.v1.TreePath";

    /** Maximum message nesting the recursive walk follows before failing the evaluation. */
    private static final int MAX_NESTING_DEPTH = 500;
    // Cache bounds. All caches are simple clear-on-threshold: when full they are wiped and
    // repopulated on demand, which keeps them thread-safe and dependency-free while preventing
    // unbounded growth for callers that validate many distinct (e.g. dynamically built) types.
    private static final int MAX_CACHED_TYPES = 256;

    /**
     * A CEL environment paired with its evaluator. The raw {@link Cel} handle enables static
     * checks (compile errors, result type) when the environment was built by this class; it is
     * null for caller-supplied evaluators, where compilation is triggered through the evaluator.
     */
    private record CelHandle(Cel cel, CelEvaluator evaluator) {
    }

    /**
     * The fully translated and eagerly compiled rules for one message type: per-field
     * constraints from every source, the non-empty message-level constraints, and the names of
     * fields governed by a message-level oneof rule.
     */
    private record CompiledRules(
            Map<FieldDescriptor, List<FieldConstraints>> fields,
            List<MessageConstraints> messages,
            Set<String> oneofMembers) {
    }

    private final CelHandle fieldCel;
    private final List<ValidationRuleSource> sources;
    // The mounted taxonomies behind the `taxonomy` field rule. Rules compile statically
    // (the binding is schema truth); the catalog is consulted per validation, so a live
    // mount swap changes verdicts without touching the compiled rule model.
    private final TaxonomyCatalog taxonomies;
    // The mounted postal-code grammars behind the google.type.PostalAddress check. No
    // schema declaration exists here — the type is the binding — so an unmounted region
    // leaves the postal code unchecked (the data-free default), unlike the taxonomy
    // rule's fail-closed stance.
    private final ai.protomolt.proto.validate.spi.PostalCodeCatalog postalCodes;
    // Message-level CEL is compiled with `this` typed as the message under validation, so a rule on
    // a nested message sees its own fields. Evaluators are built lazily and cached per descriptor.
    private final Map<Descriptor, CelHandle> messageCelByType =
            new java.util.concurrent.ConcurrentHashMap<>();
    // Translated + compiled rule model per message type (see CompiledRules).
    private final Map<Descriptor, CompiledRules> rulesByType =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Uses the default rule-source chain ({@link ValidationRuleSources#defaults()}). */
    public ProtoValidator(CelEvaluator fieldCel) {
        this(fieldCel, ValidationRuleSources.defaults());
    }

    /**
     * {@code fieldCel} evaluates field-level rules. Message-level rules cannot share it: their
     * environment types {@code this} as the message being validated, so it is built per message
     * type (see {@link #messageCelFor}).
     */
    public ProtoValidator(CelEvaluator fieldCel, List<ValidationRuleSource> sources) {
        this(new CelHandle(null, Objects.requireNonNull(fieldCel, "fieldCel")), sources,
                TaxonomyCatalog.empty(),
                ai.protomolt.proto.validate.spi.PostalCodeCatalog.empty());
    }

    private ProtoValidator(
            CelHandle fieldCel, List<ValidationRuleSource> sources, TaxonomyCatalog taxonomies,
            ai.protomolt.proto.validate.spi.PostalCodeCatalog postalCodes) {
        this.fieldCel = fieldCel;
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
        this.taxonomies = Objects.requireNonNull(taxonomies, "taxonomies");
        this.postalCodes = Objects.requireNonNull(postalCodes, "postalCodes");
    }

    /**
     * Default CEL environments: {@code this} is DYN for field rules and typed as the message
     * under validation for message rules. No taxonomies are mounted: a schema declaring a
     * {@code taxonomy} rule refuses fail-closed until a validator is built over a catalog.
     */
    public static ProtoValidator create() {
        return create(ValidationRuleSources.defaults());
    }

    /** As {@link #create()} but with an explicit rule-source chain. */
    public static ProtoValidator create(List<ValidationRuleSource> sources) {
        return create(sources, TaxonomyCatalog.empty());
    }

    /** As {@link #create()} but consulting {@code taxonomies} for {@code taxonomy} rules. */
    public static ProtoValidator create(TaxonomyCatalog taxonomies) {
        return create(ValidationRuleSources.defaults(), taxonomies);
    }

    /** As {@link #create()} but with an explicit rule-source chain and taxonomy catalog. */
    public static ProtoValidator create(
            List<ValidationRuleSource> sources, TaxonomyCatalog taxonomies) {
        return create(sources, taxonomies,
                ai.protomolt.proto.validate.spi.PostalCodeCatalog.empty());
    }

    /**
     * As {@link #create()} but additionally consulting {@code postalCodes} for the
     * {@code google.type.PostalAddress} postal-code grammar check.
     */
    public static ProtoValidator create(
            List<ValidationRuleSource> sources, TaxonomyCatalog taxonomies,
            ai.protomolt.proto.validate.spi.PostalCodeCatalog postalCodes) {
        Cel fieldEnv = celEnv().build();
        return new ProtoValidator(
                new CelHandle(fieldEnv, new CelEvaluator(fieldEnv)), sources, taxonomies,
                postalCodes);
    }

    /**
     * A CEL environment with {@code this}, {@code now} and {@code rule} bound and the format
     * standard-library functions (isHostname/isEmail/isIp/isIpPrefix/isUri/isUriRef/
     * isHostAndPort/isNan/isInf) registered. {@code rule} carries a predefined rule's
     * configured value; it is unbound for ordinary custom rules.
     */
    private static CelEnvironmentFactory celEnv() {
        return CelEnvironmentFactory.builder()
                .addVar("this")
                .addVar("now")
                .addVar("rule")
                .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings());
    }

    /** A message-level CEL environment whose {@code this} is typed as {@code descriptor}. */
    private CelHandle messageCelFor(Descriptor descriptor) {
        return cached(messageCelByType, MAX_CACHED_TYPES, descriptor, d -> {
            Cel env = celEnv().addMessageVar("this", d).build();
            return new CelHandle(env, new CelEvaluator(env));
        });
    }

    /**
     * Builds a validator whose message-level CEL knows {@code descriptor}'s type
     * (field access like {@code this.age}).
     */
    public static ProtoValidator forMessageType(Descriptor descriptor) {
        return forMessageType(descriptor, ValidationRuleSources.defaults());
    }

    /** As {@link #forMessageType(Descriptor)} but with an explicit rule-source chain. */
    public static ProtoValidator forMessageType(
            Descriptor descriptor, List<ValidationRuleSource> sources) {
        Objects.requireNonNull(descriptor, "descriptor");
        // Declaring `this` as the concrete message type lets message-level CEL type-check field
        // access (this.foo), surfacing type/field mismatches as compilation errors. That happens
        // per message type in messageCelFor, which covers nested messages too, so no environment
        // is pinned to `descriptor` here.
        return create(sources);
    }

    public ValidationResult validate(Message message) {
        Objects.requireNonNull(message, "message");
        List<ValidationResult.Violation> violations = new ArrayList<>();
        Descriptor descriptor = message.getDescriptorForType();
        CompiledRules rules = rulesFor(descriptor);
        if (!skipFieldRules(message, descriptor, rules)) {
            for (FieldDescriptor field : descriptor.getFields()) {
                validateField(message, rules, field, field.getName(), 0, violations);
            }
        }
        validateMessageRules(message, descriptor, rules, "", violations);
        return violations.isEmpty()
                ? ValidationResult.ok()
                : ValidationResult.failed(violations);
    }

    /**
     * Whether the message declares the skip-when escape channel: any source's message constraints
     * may name a singular boolean field, and when that field is true the entire field walk —
     * including recursion into nested messages — is suspended. Message-level rules are not
     * affected; they are what polices the declaration itself (e.g. requiring a reason).
     */
    private static boolean skipFieldRules(Message message, Descriptor descriptor, CompiledRules rules) {
        for (MessageConstraints constraints : rules.messages()) {
            if (!constraints.skipWhen().isEmpty()
                    && (Boolean) message.getField(descriptor.findFieldByName(constraints.skipWhen()))) {
                return true;
            }
        }
        return false;
    }

    // ---- rule model assembly and eager compilation ----

    private CompiledRules rulesFor(Descriptor descriptor) {
        return cached(rulesByType, MAX_CACHED_TYPES, descriptor, this::compileRules);
    }

    /**
     * Translates every source's rules for {@code descriptor} and compiles them up front:
     * regex patterns and CEL programs are compiled here, and oneof rules are checked against
     * the descriptor, so malformed rules throw {@link RuleCompilationException} on the first
     * validation of the type regardless of which fields the message populates.
     */
    private CompiledRules compileRules(Descriptor descriptor) {
        Map<FieldDescriptor, List<FieldConstraints>> fields = new java.util.LinkedHashMap<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            List<FieldConstraints> collected = sources.stream()
                    .map(source -> source.fieldConstraints(field))
                    .flatMap(Optional::stream)
                    .toList();
            for (FieldConstraints constraints : collected) {
                compileFieldConstraints(constraints);
                checkTaxonomy(field, constraints);
            }
            fields.put(field, collected);
        }
        List<MessageConstraints> messages = new ArrayList<>();
        Set<String> oneofMembers = new HashSet<>();
        for (ValidationRuleSource source : sources) {
            MessageConstraints constraints = source.messageConstraints(descriptor).orElse(null);
            if (constraints == null || constraints.isEmpty()) {
                continue;
            }
            messages.add(constraints);
            // Unknown names in oneof rules are schema errors: silently treating them as
            // unpopulated (or ignoring the rule) would hide typos in third-party rule sources.
            for (MessageConstraints.Oneof oneof : constraints.oneofs()) {
                for (String name : oneof.fields()) {
                    if (descriptor.findFieldByName(name) == null) {
                        throw new RuleCompilationException(
                                "field " + name + " not found in message " + descriptor.getFullName());
                    }
                    oneofMembers.add(name);
                }
            }
            for (String oneofName : constraints.requiredOneofs()) {
                if (descriptor.getRealOneofs().stream().noneMatch(o -> o.getName().equals(oneofName))) {
                    throw new RuleCompilationException(
                            "oneof " + oneofName + " not found in message " + descriptor.getFullName());
                }
            }
            // skip_when names the boolean field that suspends field-level rules. As with oneof
            // member names, an unknown or wrongly-typed name is a schema error: silently ignoring
            // it would let a producer think it declared incompleteness while the validator still
            // held every field to the rules.
            if (!constraints.skipWhen().isEmpty()) {
                FieldDescriptor skipField = descriptor.findFieldByName(constraints.skipWhen());
                if (skipField == null) {
                    throw new RuleCompilationException(
                            "skip_when field " + constraints.skipWhen()
                                    + " not found in message " + descriptor.getFullName());
                }
                if (skipField.isRepeated() || skipField.getJavaType() != FieldDescriptor.JavaType.BOOLEAN) {
                    throw new RuleCompilationException(
                            "skip_when field " + constraints.skipWhen() + " in message "
                                    + descriptor.getFullName() + " must be a singular boolean field");
                }
            }
            CelHandle handle = messageCelFor(descriptor);
            for (CelConstraint rule : constraints.cel()) {
                compileCel(handle, rule);
            }
        }
        return new CompiledRules(fields, List.copyOf(messages), Set.copyOf(oneofMembers));
    }

    /** Compiles every pattern and CEL rule in {@code constraints}, including nested element rules. */
    private void compileFieldConstraints(FieldConstraints constraints) {
        constraints.string().ifPresent(s -> s.pattern().ifPresent(ValueChecks::compiledPattern));
        constraints.bytes().ifPresent(b -> b.pattern().ifPresent(ValueChecks::compiledPattern));
        for (CelConstraint rule : constraints.cel()) {
            compileCel(fieldCel, rule);
        }
        constraints.repeated().flatMap(RepeatedConstraints::items)
                .ifPresent(this::compileFieldConstraints);
        constraints.map().ifPresent(m -> {
            m.keys().ifPresent(this::compileFieldConstraints);
            m.values().ifPresent(this::compileFieldConstraints);
        });
    }

    /**
     * Compiles a CEL rule eagerly. With a {@link Cel} handle the expression is type-checked and
     * its static result type verified (protovalidate rejects rules that return neither bool nor
     * string at compile time); with only an evaluator, compilation is triggered through it and
     * evaluation errors from unbound variables are ignored — they are not compile errors.
     */
    private static void compileCel(CelHandle handle, CelConstraint rule) {
        if (rule.expression().isBlank()) {
            return;
        }
        if (handle.cel() != null) {
            CelType result;
            try {
                result = handle.cel().compile(rule.expression()).getAst().getResultType();
            } catch (CelValidationException e) {
                throw new RuleCompilationException("Invalid CEL expression: " + e.getMessage(), e);
            }
            CelKind kind = result.kind();
            if (kind != CelKind.BOOL && kind != CelKind.STRING
                    && kind != CelKind.DYN && kind != CelKind.ANY && kind != CelKind.ERROR) {
                throw new RuleCompilationException(
                        "CEL rule must return bool or string, got " + result.name());
            }
        } else {
            try {
                handle.evaluator().evaluateValue(rule.expression(), Map.of());
            } catch (CelCompilationException e) {
                throw new RuleCompilationException(e.getMessage(), e);
            } catch (CelEvaluationException ignored) {
                // Compiled fine; failing on unbound `this`/`now`/`rule` here is expected.
            }
        }
    }

    /**
     * A {@code taxonomy} rule binds only to a singular or repeated field of the canonical
     * TreePath type, and only on the field itself. Declaring one anywhere else is a schema
     * error, not a silently ignored rule: a producer that thinks its paths are checked while
     * the validator skips them is the failure mode that reports success.
     */
    private static void checkTaxonomy(FieldDescriptor field, FieldConstraints constraints) {
        if (nestedTaxonomy(constraints)) {
            throw new RuleCompilationException(
                    "taxonomy on field " + field.getFullName() + " must be declared on the"
                            + " TreePath field itself, not on repeated items or map entries");
        }
        String name = constraints.taxonomy().orElse(null);
        if (name == null) {
            return;
        }
        if (field.isMapField() || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || !TREE_PATH_TYPE.equals(field.getMessageType().getFullName())) {
            throw new RuleCompilationException(
                    "taxonomy \"" + name + "\" declared on field " + field.getFullName()
                            + ": only fields of " + TREE_PATH_TYPE + " take a taxonomy rule");
        }
    }

    /** Whether any nested (items/keys/values) constraints declare a taxonomy. */
    private static boolean nestedTaxonomy(FieldConstraints constraints) {
        return constraints.repeated().flatMap(RepeatedConstraints::items)
                .map(ProtoValidator::declaresTaxonomy).orElse(false)
                || constraints.map().map(m ->
                        m.keys().map(ProtoValidator::declaresTaxonomy).orElse(false)
                                || m.values().map(ProtoValidator::declaresTaxonomy).orElse(false))
                        .orElse(false);
    }

    private static boolean declaresTaxonomy(FieldConstraints constraints) {
        return constraints.taxonomy().isPresent() || nestedTaxonomy(constraints);
    }

    /** Clear-on-threshold cache lookup (see the cache-bounds note on the constants). */
    private static <K, V> V cached(
            Map<K, V> cache, int maxSize, K key, java.util.function.Function<K, V> compute) {
        V existing = cache.get(key);
        if (existing != null) {
            return existing;
        }
        if (cache.size() >= maxSize) {
            cache.clear();
        }
        return cache.computeIfAbsent(key, compute);
    }

    // ---- field walk ----

    private void validateField(
            Message message,
            CompiledRules rules,
            FieldDescriptor field,
            String path,
            int depth,
            List<ValidationResult.Violation> violations) {
        List<FieldConstraints> constraints = rules.fields().get(field);
        IgnoreMode ignore = effectiveIgnore(constraints);
        if (ignore == IgnoreMode.ALWAYS) {
            return;
        }
        boolean hasField = isPresent(message, field);

        if (constraints.stream().anyMatch(FieldConstraints::required) && !hasField) {
            violations.add(new ValidationResult.Violation(path, "required", "field is required"));
            return;
        }

        // Fields that track presence (message, optional, oneof) and fields marked ignore-if-zero are
        // skipped when unpopulated. Implicit-presence scalars/collections are validated even at their
        // zero value, so min_items / bounds on a zero apply. Members of a message-level oneof rule are
        // treated as presence-tracking too: their field-level rules only apply when populated.
        boolean skipWhenEmpty = field.hasPresence() || ignore == IgnoreMode.IF_ZERO_VALUE
                || rules.oneofMembers().contains(field.getName());
        if (skipWhenEmpty && !hasField) {
            return;
        }

        if (field.isMapField()) {
            validateMap(message, field, constraints, path, depth, violations);
            // A field-level CEL rule on a map binds `this` to the whole map, evaluated once.
            Object celMap = celMapValue(message, field);
            for (FieldConstraints c : constraints) {
                runFieldCel(c, celMap, path, violations);
            }
            return;
        }
        if (field.isRepeated()) {
            validateRepeated(message, field, constraints, path, depth, violations);
            // A field-level CEL rule on a repeated field binds `this` to the whole list.
            Object celList = celListValue(message, field);
            for (FieldConstraints c : constraints) {
                runFieldCel(c, celList, path, violations);
            }
            return;
        }

        Object value = message.getField(field);
        for (FieldConstraints c : constraints) {
            applyFieldConstraints(field, c, value, path, violations);
            applyTaxonomy(c, value, path, violations);
            runFieldCel(c, celScalar(field, value), path, violations);
        }
        // A field declared inspect-only carries a document the receiver examines rather than
        // a value it consumes, so the walk stops at it. Its own rules have already run: it can
        // still be required, and it still had to parse as its declared type.
        if (value instanceof Message nested && !inspectOnly(constraints)) {
            validateChildren(nested, path, depth, violations);
        }
    }

    /** Whether any source declares the field inspect-only. */
    private static boolean inspectOnly(List<FieldConstraints> constraints) {
        return constraints.stream().anyMatch(FieldConstraints::inspectOnly);
    }

    private void validateChildren(
            Message nested, String path, int depth, List<ValidationResult.Violation> violations) {
        if (depth >= MAX_NESTING_DEPTH) {
            throw new RuleEvaluationException(
                    "message nesting exceeds " + MAX_NESTING_DEPTH + " levels at " + path);
        }
        Descriptor descriptor = nested.getDescriptorForType();
        CompiledRules rules = rulesFor(descriptor);
        if (!skipFieldRules(nested, descriptor, rules)) {
            for (FieldDescriptor child : descriptor.getFields()) {
                validateField(nested, rules, child, path + "." + child.getName(), depth + 1, violations);
            }
        }
        validateMessageRules(nested, descriptor, rules, path, violations);
    }

    private void validateRepeated(
            Message message,
            FieldDescriptor field,
            List<FieldConstraints> constraints,
            String path,
            int depth,
            List<ValidationResult.Violation> violations) {
        int count = message.getRepeatedFieldCount(field);
        for (FieldConstraints c : constraints) {
            RepeatedConstraints r = c.repeated().orElse(null);
            if (r == null) {
                continue;
            }
            if (r.minItems().isPresent() && count < r.minItems().getAsLong()) {
                violations.add(violation(path, "repeated.min_items",
                        "must have at least " + r.minItems().getAsLong() + " items"));
            }
            if (r.maxItems().isPresent() && count > r.maxItems().getAsLong()) {
                violations.add(violation(path, "repeated.max_items",
                        "must have at most " + r.maxItems().getAsLong() + " items"));
            }
            if (r.unique()) {
                Set<Object> seen = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    Object element = uniqueKey(message.getRepeatedField(field, i));
                    if (element != null && !seen.add(element)) {
                        // A single violation on the repeated field itself, not per duplicate element.
                        violations.add(violation(path, "repeated.unique",
                                "repeated values must be unique"));
                        break;
                    }
                }
            }
        }
        for (int i = 0; i < count; i++) {
            Object element = message.getRepeatedField(field, i);
            String elementPath = path + "[" + i + "]";
            boolean skipElement = false;
            for (FieldConstraints c : constraints) {
                var items = c.repeated().flatMap(RepeatedConstraints::items).orElse(null);
                if (items == null) {
                    continue;
                }
                if (skipValue(items, element, field)) {
                    // An item ignored by its own rule (IGNORE_ALWAYS) also skips embedded validation.
                    skipElement = true;
                    continue;
                }
                applyFieldConstraints(field, items, element, elementPath, violations);
                runFieldCel(items, celScalar(field, element), elementPath, violations);
            }
            if (!skipElement) {
                for (FieldConstraints c : constraints) {
                    // A taxonomy on a repeated TreePath field binds every element.
                    applyTaxonomy(c, element, elementPath, violations);
                }
                if (element instanceof Message nested && !inspectOnly(constraints)) {
                    validateChildren(nested, elementPath, depth, violations);
                }
            }
        }
    }

    /**
     * The taxonomy membership rule on a populated TreePath value. Fail-closed by design: a
     * declared taxonomy the validator has no mount for refuses — a gate that cannot check
     * the declaration must never pronounce the value clean. An empty path is left to the
     * type's own structural rules, and the mounted version rides the violation as evidence.
     */
    private void applyTaxonomy(
            FieldConstraints constraints, Object value, String path,
            List<ValidationResult.Violation> violations) {
        String name = constraints.taxonomy().orElse(null);
        if (name == null || !(value instanceof Message treePath)) {
            return;
        }
        FieldDescriptor segments = treePath.getDescriptorForType().findFieldByName("segments");
        @SuppressWarnings("unchecked")
        List<String> segmentValues = (List<String>) treePath.getField(segments);
        if (segmentValues.isEmpty()) {
            return;
        }
        String rendered = String.join("/", segmentValues);
        TaxonomyCatalog.Mounted mounted = taxonomies.taxonomy(name).orElse(null);
        if (mounted == null) {
            violations.add(violation(path, "taxonomy.unmounted",
                    "taxonomy \"" + name + "\" is not mounted; declared membership"
                            + " cannot be checked"));
            return;
        }
        if (!mounted.nodes().contains(rendered)) {
            violations.add(violation(path, "taxonomy.member",
                    "\"" + rendered + "\" is not a node of taxonomy \"" + name
                            + "\" at version " + mounted.version()));
        }
    }

    /**
     * The identity used for {@code repeated.unique} duplicate detection, matching CEL numeric
     * equality: {@code -0.0} equals {@code 0.0}, and {@code NaN} equals nothing — a NaN element
     * (returned as null) can never be a duplicate.
     */
    private static Object uniqueKey(Object element) {
        return switch (element) {
            // A pattern switch throws on a null selector; the chain this replaced returned it.
            case null -> null;
            case Double d -> Double.isNaN(d) ? null : (d == 0.0d ? Double.valueOf(0.0d) : d);
            case Float f -> Float.isNaN(f) ? null : (f == 0.0f ? Float.valueOf(0.0f) : f);
            default -> element;
        };
    }

    private void validateMap(
            Message message,
            FieldDescriptor field,
            List<FieldConstraints> constraints,
            String path,
            int depth,
            List<ValidationResult.Violation> violations) {
        int count = message.getRepeatedFieldCount(field);
        for (FieldConstraints c : constraints) {
            MapConstraints m = c.map().orElse(null);
            if (m == null) {
                continue;
            }
            if (m.minPairs().isPresent() && count < m.minPairs().getAsLong()) {
                violations.add(violation(path, "map.min_pairs",
                        "must have at least " + m.minPairs().getAsLong() + " entries"));
            }
            if (m.maxPairs().isPresent() && count > m.maxPairs().getAsLong()) {
                violations.add(violation(path, "map.max_pairs",
                        "must have at most " + m.maxPairs().getAsLong() + " entries"));
            }
        }
        Descriptor entryType = field.getMessageType();
        FieldDescriptor keyField = entryType.findFieldByNumber(1);
        FieldDescriptor valueField = entryType.findFieldByNumber(2);
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            Object key = entry.getField(keyField);
            Object value = entry.getField(valueField);
            String entryPath = path + subscript(key);
            String keyPath = entryPath + "#key";
            boolean skipEntryValue = false;
            for (FieldConstraints c : constraints) {
                MapConstraints m = c.map().orElse(null);
                if (m == null) {
                    continue;
                }
                FieldConstraints keyRules = m.keys().orElse(null);
                if (keyRules != null && !skipValue(keyRules, key, keyField)) {
                    applyFieldConstraints(keyField, keyRules, key, keyPath, violations);
                    runFieldCel(keyRules, celScalar(keyField, key), keyPath, violations);
                }
                FieldConstraints valueRules = m.values().orElse(null);
                if (valueRules == null) {
                    continue;
                }
                if (skipValue(valueRules, value, valueField)) {
                    // A value ignored by its own rule (IGNORE_ALWAYS) also skips embedded validation.
                    skipEntryValue = true;
                    continue;
                }
                applyFieldConstraints(valueField, valueRules, value, entryPath, violations);
                runFieldCel(valueRules, celScalar(valueField, value), entryPath, violations);
            }
            if (!skipEntryValue && value instanceof Message nested && !inspectOnly(constraints)) {
                validateChildren(nested, entryPath, depth, violations);
            }
        }
    }

    private static String subscript(Object key) {
        if (key instanceof String s) {
            // Escape backslash and quote so the quoted key round-trips unambiguously.
            return "[\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"]";
        }
        return "[" + key + "]";
    }

    /** Runs a field's CEL rules against an already CEL-converted {@code this} value. */
    private void runFieldCel(
            FieldConstraints constraints, Object celValue, String path,
            List<ValidationResult.Violation> violations) {
        if (constraints.cel().isEmpty()) {
            return;
        }
        // cel[N] and cel_expression[N] are indexed independently within their own repeated field;
        // predefined rules carry an explicit extension-shaped rule path instead.
        Map<String, Integer> next = new java.util.HashMap<>();
        for (CelConstraint rule : constraints.cel()) {
            String rulePath = rule.rulePath();
            if (rulePath.isEmpty()) {
                int i = next.merge(rule.celField(), 1, Integer::sum) - 1;
                rulePath = rule.celField() + "[" + i + "]";
            }
            evalCel(fieldCel.evaluator(), rule, celValue, path, rulePath, violations);
        }
    }

    private void validateMessageRules(
            Message message,
            Descriptor descriptor,
            CompiledRules rules,
            String path,
            List<ValidationResult.Violation> violations) {
        for (MessageConstraints constraints : rules.messages()) {
            // Message-level CEL rules report no FieldRules rule path (they are not on any field),
            // and top-level violations carry an empty field path: the rule targets the message
            // itself, not any named field.
            CelEvaluator evaluator = messageCelFor(descriptor).evaluator();
            for (CelConstraint rule : constraints.cel()) {
                evalCel(evaluator, rule, message, path, "", violations);
            }
            for (MessageConstraints.Oneof oneof : constraints.oneofs()) {
                validateMessageOneof(message, descriptor, oneof, path, violations);
            }
            for (String oneofName : constraints.requiredOneofs()) {
                validateRequiredOneof(message, descriptor, oneofName, path, violations);
            }
        }
        if ("google.type.PostalAddress".equals(descriptor.getFullName())) {
            applyPostalGrammar(message, descriptor, path, violations);
        }
    }

    /**
     * The operator-pack half of the PostalAddress contract: when the mounted pack
     * carries the address's region, a non-empty postal code must match one of the
     * region's masks ({@code N} a digit, {@code A} an uppercase letter, anything
     * else literal — one linear scan per mask, no operator-supplied patterns). An
     * unmounted region leaves the code unchecked, the data-free default.
     */
    private void applyPostalGrammar(
            Message message, Descriptor descriptor, String path,
            List<ValidationResult.Violation> violations) {
        FieldDescriptor regionField = descriptor.findFieldByName("region_code");
        FieldDescriptor codeField = descriptor.findFieldByName("postal_code");
        if (regionField == null || codeField == null
                || regionField.getJavaType() != FieldDescriptor.JavaType.STRING
                || codeField.getJavaType() != FieldDescriptor.JavaType.STRING) {
            return;
        }
        String code = (String) message.getField(codeField);
        if (code.isEmpty()) {
            return;
        }
        ai.protomolt.proto.validate.spi.PostalCodeCatalog.Mounted mounted = postalCodes
                .region((String) message.getField(regionField)).orElse(null);
        if (mounted == null) {
            return;
        }
        for (String mask : mounted.masks()) {
            if (ai.protomolt.proto.formats.PostalMasks.matches(code, mask)) {
                return;
            }
        }
        violations.add(violation(
                path.isEmpty() ? "postal_code" : path + ".postal_code",
                "postal.code_grammar",
                "postal code does not match the mounted grammar for region \""
                        + mounted.regionCode() + "\" at version " + mounted.version()));
    }

    /**
     * A real protobuf oneof marked {@code required}: exactly one member must be set. The violation
     * reports {@code required} on the oneof name itself (a bare {@code field_name} path element).
     * The oneof's existence was checked when the rule model was compiled.
     */
    private static void validateRequiredOneof(
            Message message,
            Descriptor descriptor,
            String oneofName,
            String path,
            List<ValidationResult.Violation> violations) {
        var oneof = descriptor.getRealOneofs().stream()
                .filter(o -> o.getName().equals(oneofName))
                .findFirst()
                .orElseThrow(() -> new RuleCompilationException(
                        "oneof " + oneofName + " not found in message " + descriptor.getFullName()));
        if (!message.hasOneof(oneof)) {
            String oneofPath = path.isEmpty() ? oneofName : path + "." + oneofName;
            violations.add(new ValidationResult.Violation(
                    oneofPath, "required", "exactly one field is required in oneof"));
        }
    }

    /**
     * A message-level {@code oneof} rule: at most one member may be populated, and when
     * {@code required} at least one must be. Both failures report {@code message.oneof} on the
     * message path with the member list spelled out, matching protovalidate's wording. Member
     * names were resolved against the descriptor when the rule model was compiled.
     */
    private static void validateMessageOneof(
            Message message,
            Descriptor descriptor,
            MessageConstraints.Oneof oneof,
            String path,
            List<ValidationResult.Violation> violations) {
        int populated = 0;
        for (String name : oneof.fields()) {
            FieldDescriptor fd = descriptor.findFieldByName(name);
            if (fd == null) {
                throw new RuleCompilationException(
                        "field " + name + " not found in message " + descriptor.getFullName());
            }
            if (isPresent(message, fd)) {
                populated++;
            }
        }
        String members = String.join(", ", oneof.fields());
        if (populated > 1) {
            violations.add(new ValidationResult.Violation(
                    path, "message.oneof", "only one of " + members + " can be set"));
        } else if (oneof.required() && populated == 0) {
            violations.add(new ValidationResult.Violation(
                    path, "message.oneof", "one of " + members + " must be set"));
        }
    }

    /** Whether an element (repeated item, map key/value) is skipped by its own ignore mode. */
    private static boolean skipValue(FieldConstraints constraints, Object value, FieldDescriptor field) {
        return switch (constraints.ignore()) {
            case ALWAYS -> true;
            case IF_ZERO_VALUE -> isZeroValue(value, field);
            case UNSPECIFIED -> false;
        };
    }

    private static boolean isZeroValue(Object value, FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case INT -> (Integer) value == 0;
            case LONG -> (Long) value == 0L;
            case FLOAT -> (Float) value == 0f;
            case DOUBLE -> (Double) value == 0d;
            case BOOLEAN -> !((Boolean) value);
            case STRING -> ((String) value).isEmpty();
            case BYTE_STRING -> ((ByteString) value).isEmpty();
            case ENUM -> ((EnumValueDescriptor) value).getNumber() == 0;
            case MESSAGE -> false;
        };
    }

    /** The strongest ignore mode declared across a field's rule sources. */
    private static IgnoreMode effectiveIgnore(List<FieldConstraints> constraints) {
        IgnoreMode mode = IgnoreMode.UNSPECIFIED;
        for (FieldConstraints c : constraints) {
            if (c.ignore().ordinal() > mode.ordinal()) {
                mode = c.ignore();
            }
        }
        return mode;
    }

    private static boolean isPresent(Message message, FieldDescriptor field) {
        if (field.isRepeated()) {
            return message.getRepeatedFieldCount(field) > 0;
        }
        if (field.hasPresence()) {
            return message.hasField(field);
        }
        Object value = message.getField(field);
        return switch (field.getJavaType()) {
            case STRING -> value instanceof String s && !s.isEmpty();
            case BYTE_STRING -> value instanceof ByteString b && !b.isEmpty();
            case MESSAGE -> message.hasField(field);
            case ENUM -> ((EnumValueDescriptor) value).getNumber() != 0;
            case BOOLEAN -> (Boolean) value;
            case INT, LONG -> ((Number) value).longValue() != 0L;
            // Bitwise comparison so -0.0 counts as set (its raw bits differ from +0.0).
            case FLOAT, DOUBLE ->
                    Double.doubleToRawLongBits(((Number) value).doubleValue()) != 0L;
            default -> true;
        };
    }
}

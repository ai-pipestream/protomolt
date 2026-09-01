package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.meta.DescriptorMetadata;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MessageMetric;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.pipestream.proto.metric.spi.MetricMapping.FieldKind;
import ai.pipestream.proto.metric.spi.MetricMapping.MetricMember;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.ast.CelConstant;
import dev.cel.common.ast.CelExpr;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.SimpleType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds a {@link MetricMapping} from a message type's metric.v1
 * declarations. Every schema error in the standard's list fails the build
 * here, naming the field path — a mapping that builds is a mapping every
 * query can trust, so executors and the query compiler never re-validate
 * declarations.
 *
 * <p>The walk descends into singular message fields, so declarations on
 * nested fields become members whose {@code fieldName} is the flattened
 * engine name ({@code parent_child}, matching the index mapping's own
 * naming from proto field names; an index-side name override puts a field
 * out of metric reach). Repeated paths cannot carry members, recursive
 * types stop the descent, and member names stay the bare field name unless
 * the declaration renames them, with collisions across depths refused.</p>
 *
 * <p>filter_cel is translated at build time into the engine-neutral
 * equality form (equality over string or bool fields, joined by
 * {@code &&}); a valid-but-untranslatable filter fails the build loudly
 * rather than surprising the first query. Calculated measures compile
 * against their sibling physical measure names and record what they read.</p>
 */
public final class MetricMappings {

    private static final String TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    private static final String TREE_PATH_TYPE = "ai.pipestream.proto.types.v1.TreePath";

    private MetricMappings() {
    }

    /**
     * Builds the queryable surface of {@code descriptor}.
     *
     * @param subject the mapping subject the host serves this type under;
     *        blank falls back to the MessageMetric declaration's subject
     * @param descriptor the message type carrying the declarations
     * @param source where declarations come from
     * @return the built mapping
     * @throws MetricSchemaException naming every violation at once
     */
    public static MetricMapping build(
            String subject, Descriptor descriptor, MetricHintSource source) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        List<String> violations = new ArrayList<>();

        Optional<MessageMetric> messageMetric = source.message(descriptor);
        String resolvedSubject = subject != null && !subject.isBlank()
                ? subject
                : messageMetric.map(MessageMetric::getSubject).orElse("");
        if (resolvedSubject.isBlank()) {
            violations.add("no subject: neither the host nor the message declaration names one");
        }
        messageMetric.map(MessageMetric::getIdentityField)
                .filter(name -> !name.isEmpty())
                .filter(name -> descriptor.findFieldByName(name) == null)
                .ifPresent(name -> violations.add(
                        "identity_field '" + name + "' names no field of "
                                + descriptor.getFullName()));

        Map<String, MetricMember> members = new LinkedHashMap<>();
        Map<String, FieldDescriptor> memberFields = new LinkedHashMap<>();
        Map<String, FieldMetric> declarations = new LinkedHashMap<>();
        Map<String, Prefixes> memberPrefixes = new LinkedHashMap<>();
        Set<String> visiting = new LinkedHashSet<>();
        visiting.add(descriptor.getFullName());
        collect(descriptor, new Prefixes("", ""), null, visiting, source,
                members, memberFields, declarations, memberPrefixes, violations);

        // Synthetic members: measures with no backing field, declared on
        // the message. A COUNT (optionally row-filtered) or a calculated
        // cel; anything needing storage stays a field.
        messageMetric.ifPresent(declaration -> {
            for (FieldMetric declared : declaration.getMembersList()) {
                buildSynthetic(descriptor, declared,
                        members, declarations, memberPrefixes, violations);
            }
        });

        checkCels(descriptor, members, memberFields, declarations,
                memberPrefixes, violations);

        if (!violations.isEmpty()) {
            throw new MetricSchemaException(descriptor.getFullName(), violations);
        }
        return new MetricMapping(resolvedSubject, descriptor.getFullName(), members);
    }

    // ------------------------------------------------------------- the walk

    /**
     * Walks the message tree for declarations. {@code prefix} accumulates
     * the flattened engine field name ({@code parent_child}, the index
     * mapping's own naming) so a nested member's {@code fieldName} lands on
     * the field the engine actually wrote. Undeclared singular message
     * fields are descended (Timestamp stays a DATE leaf, TreePath a
     * TREE_PATH leaf, map fields never carry members); a declaration below a repeated field is a violation,
     * because a repeated path has no single value to aggregate.
     */
    private static void collect(
            Descriptor type,
            Prefixes prefix,
            String repeatedAncestor,
            Set<String> visiting,
            MetricHintSource source,
            Map<String, MetricMember> members,
            Map<String, FieldDescriptor> memberFields,
            Map<String, FieldMetric> declarations,
            Map<String, Prefixes> memberPrefixes,
            List<String> violations) {
        for (FieldDescriptor field : type.getFields()) {
            Optional<FieldMetric> declared = source.field(field);
            if (declared.isPresent()) {
                if (repeatedAncestor != null) {
                    violations.add("'" + field.getFullName() + "' is reachable only"
                            + " through repeated field '" + repeatedAncestor
                            + "'; repeated paths cannot carry metric members");
                    continue;
                }
                buildMember(field, prefix, declared.get(), violations).ifPresent(member -> {
                    if (members.containsKey(member.name())) {
                        violations.add("member name '" + member.name() + "' on '"
                                + field.getFullName() + "' collides with another member");
                        return;
                    }
                    members.put(member.name(), member);
                    memberFields.put(member.name(), field);
                    declarations.put(member.name(), declared.get());
                    memberPrefixes.put(member.name(), prefix);
                });
                continue;
            }
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && !field.isMapField()
                    && !TIMESTAMP_TYPE.equals(field.getMessageType().getFullName())
                    && !TREE_PATH_TYPE.equals(field.getMessageType().getFullName())
                    && visiting.add(field.getMessageType().getFullName())) {
                collect(field.getMessageType(),
                        prefix.descend(field.getName()),
                        repeatedAncestor != null ? repeatedAncestor
                                : field.isRepeated() ? field.getFullName() : null,
                        visiting, source,
                        members, memberFields, declarations, memberPrefixes, violations);
                visiting.remove(field.getMessageType().getFullName());
            }
        }
    }

    // ------------------------------------------------------------- one member

    /** One declared field's member, or empty when its violations preclude it. */
    private static Optional<MetricMember> buildMember(
            FieldDescriptor field, Prefixes prefix, FieldMetric declared,
            List<String> violations) {
        String path = field.getFullName();
        int before = violations.size();

        if (declared.getRole() == MemberRole.MEMBER_ROLE_UNSPECIFIED) {
            violations.add("metric option on '" + path
                    + "' declares no role; the option is an explicit declaration");
            return Optional.empty();
        }
        if (field.isRepeated()) {
            violations.add("'" + path + "' is repeated; repeated fields cannot be metric members");
            return Optional.empty();
        }
        FieldKind kind = kindOf(field);
        if (kind == null) {
            violations.add("'" + path + "' has type " + field.getType()
                    + ", which no metric member can use");
            return Optional.empty();
        }
        if (declared.getDefaultGrain() != TimeGrain.TIME_GRAIN_UNSPECIFIED && kind != FieldKind.DATE) {
            violations.add("default_grain on '" + path + "' needs a DATE field, got " + kind);
        }

        if (declared.getRole() == MemberRole.MEMBER_ROLE_DIMENSION) {
            if (declared.getAggregate() != Aggregate.AGGREGATE_UNSPECIFIED) {
                violations.add("dimension '" + path + "' declares an aggregate");
            }
            if (!declared.getFilterCel().isEmpty()) {
                violations.add("dimension '" + path + "' declares a filter_cel");
            }
            if (!declared.getCel().isEmpty()) {
                violations.add("dimension '" + path + "' declares a calculated cel");
            }
            if (kind == FieldKind.NUMERIC) {
                violations.add("dimension '" + path
                        + "' is numeric; group-by needs a keyword, bool, or date field");
            }
        } else {
            boolean calculated = !declared.getCel().isEmpty();
            if (calculated && (declared.getAggregate() != Aggregate.AGGREGATE_UNSPECIFIED
                    || !declared.getFilterCel().isEmpty())) {
                violations.add("calculated measure '" + path
                        + "' also declares an aggregate or filter_cel");
            }
            if (!calculated && declared.getAggregate() == Aggregate.AGGREGATE_UNSPECIFIED) {
                violations.add("measure '" + path + "' declares neither an aggregate nor a cel");
            }
            if (!calculated) {
                switch (declared.getAggregate()) {
                    case AGGREGATE_SUM, AGGREGATE_AVG, AGGREGATE_MIN, AGGREGATE_MAX -> {
                        if (kind != FieldKind.NUMERIC) {
                            violations.add("aggregate " + declared.getAggregate() + " on '"
                                    + path + "' needs a numeric field, got " + kind);
                        }
                    }
                    case AGGREGATE_COUNT_DISTINCT -> {
                        if (kind != FieldKind.NUMERIC && kind != FieldKind.KEYWORD) {
                            violations.add("COUNT_DISTINCT on '" + path
                                    + "' needs a numeric or keyword field, got " + kind);
                        }
                    }
                    default -> {
                    }
                }
            }
        }
        if (violations.size() > before) {
            return Optional.empty();
        }

        String name = declared.getName().isEmpty() ? field.getName() : declared.getName();
        String description = DescriptorMetadata.field(field)
                .map(meta -> meta.getDescription()).orElse("");
        String sensitivity = DescriptorMetadata.field(field)
                .map(meta -> meta.getSensitivity()).orElse("");
        return Optional.of(new MetricMember(
                name,
                declared.getRole(),
                declared.getAggregate(),
                prefix.name() + field.getName(),
                prefix.path() + field.getName(),
                declared.getCel().isEmpty() ? kind : FieldKind.SYNTHETIC,
                List.of(),
                declared.getCel(),
                List.of(),
                declared.getDefaultGrain(),
                description,
                sensitivity));
    }

    /** One synthetic member, registered unless its violations preclude it. */
    private static void buildSynthetic(
            Descriptor root,
            FieldMetric declared,
            Map<String, MetricMember> members,
            Map<String, FieldMetric> declarations,
            Map<String, Prefixes> memberPrefixes,
            List<String> violations) {
        String name = declared.getName();
        String label = "synthetic member " + (name.isEmpty() ? "(unnamed)" : "'" + name + "'")
                + " on " + root.getFullName();
        int before = violations.size();
        if (name.isEmpty()) {
            violations.add(label + " declares no name; a synthetic member has no"
                    + " field name to fall back on");
        }
        if (declared.getRole() != MemberRole.MEMBER_ROLE_MEASURE) {
            violations.add(label + " must be a MEASURE; a dimension needs storage,"
                    + " so it stays a field");
        }
        if (declared.getDefaultGrain() != TimeGrain.TIME_GRAIN_UNSPECIFIED) {
            violations.add(label + " declares a default_grain; grains belong to"
                    + " DATE dimensions");
        }
        boolean calculated = !declared.getCel().isEmpty();
        if (calculated && (declared.getAggregate() != Aggregate.AGGREGATE_UNSPECIFIED
                || !declared.getFilterCel().isEmpty())) {
            violations.add(label + " is calculated and also declares an aggregate"
                    + " or filter_cel");
        }
        if (!calculated && declared.getAggregate() != Aggregate.AGGREGATE_COUNT) {
            violations.add(label + " has no backing field: only COUNT (optionally"
                    + " filtered) or a calculated cel works without one, got "
                    + declared.getAggregate());
        }
        if (violations.size() > before) {
            return;
        }
        if (members.containsKey(name)) {
            violations.add(label + " collides with another member");
            return;
        }
        members.put(name, new MetricMember(
                name,
                MemberRole.MEMBER_ROLE_MEASURE,
                declared.getAggregate(),
                "",
                "",
                FieldKind.SYNTHETIC,
                List.of(),
                declared.getCel(),
                List.of(),
                TimeGrain.TIME_GRAIN_UNSPECIFIED,
                "",
                ""));
        declarations.put(name, declared);
        memberPrefixes.put(name, new Prefixes("", ""));
    }

    /** The metric shape of one field; null when no member can use it. */
    private static FieldKind kindOf(FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case STRING, ENUM -> FieldKind.KEYWORD;
            case BOOLEAN -> FieldKind.BOOLEAN;
            case INT, LONG, FLOAT, DOUBLE -> FieldKind.NUMERIC;
            case MESSAGE -> switch (field.getMessageType().getFullName()) {
                case TIMESTAMP_TYPE -> FieldKind.DATE;
                case TREE_PATH_TYPE -> FieldKind.TREE_PATH;
                default -> null;
            };
            case BYTE_STRING -> null;
        };
    }

    // ------------------------------------------------------------- CEL checks

    /**
     * Second pass over the collected members: filter_cel compiles typed over
     * the declaring message and translates to the equality form; calculated
     * cel compiles over the sibling physical measure names and records what
     * it reads. Members that fail are replaced by violations.
     */
    private static void checkCels(
            Descriptor root,
            Map<String, MetricMember> members,
            Map<String, FieldDescriptor> memberFields,
            Map<String, FieldMetric> declarations,
            Map<String, Prefixes> memberPrefixes,
            List<String> violations) {
        CelEnvironmentFactory siblingEnv = CelEnvironmentFactory.builder();
        for (MetricMember member : members.values()) {
            if (member.role() == MemberRole.MEMBER_ROLE_MEASURE && !member.calculated()) {
                siblingEnv.addVar(member.name(), SimpleType.DOUBLE);
            }
        }

        for (Map.Entry<String, MetricMember> entry : List.copyOf(members.entrySet())) {
            MetricMember member = entry.getValue();
            FieldDescriptor field = memberFields.get(entry.getKey());
            String filterCel = declarations.get(entry.getKey()).getFilterCel();
            if (member.calculated()) {
                checkCalculated(entry.getKey(), member, siblingEnv, members, violations);
                continue;
            }
            if (filterCel.isEmpty()) {
                continue;
            }
            // A synthetic member has no backing field: its filter_cel
            // speaks about the root message itself.
            Descriptor declaring = field != null ? field.getContainingType() : root;
            String label = field != null
                    ? "'" + field.getFullName() + "'"
                    : "synthetic member '" + entry.getKey() + "'";
            checkFilter(entry.getKey(), member, declaring, label, filterCel,
                    memberPrefixes.get(entry.getKey()), members, violations);
        }
    }

    private static void checkCalculated(
            String name,
            MetricMember member,
            CelEnvironmentFactory siblingEnv,
            Map<String, MetricMember> members,
            List<String> violations) {
        CelAbstractSyntaxTree ast;
        try {
            ast = siblingEnv.build().compile(member.cel()).getAst();
        } catch (CelValidationException e) {
            violations.add("cel on member '" + name
                    + "' does not type-check against its sibling measures: " + e.getMessage());
            return;
        }
        CelKind result = ast.getResultType().kind();
        if (result != CelKind.DOUBLE && result != CelKind.INT && result != CelKind.UINT) {
            violations.add("cel on member '" + name + "' must be numeric, got "
                    + ast.getResultType().name());
            return;
        }
        Set<String> requires = new LinkedHashSet<>();
        collectIdents(ast.getExpr(), requires);
        members.put(name, new MetricMember(
                member.name(), member.role(), member.aggregate(), member.fieldName(),
                member.fieldPath(), member.kind(), member.rowFilters(), member.cel(),
                List.copyOf(requires), member.defaultGrain(),
                member.description(), member.sensitivity()));
    }

    private static void checkFilter(
            String name,
            MetricMember member,
            Descriptor declaring,
            String label,
            String filterCel,
            Prefixes prefix,
            Map<String, MetricMember> members,
            List<String> violations) {
        // filter_cel is written against the DECLARING message: `this` is the
        // field's containing type (the root message for a synthetic member),
        // so a nested declaration filters over its own siblings and the
        // translated field names take the same prefix as the member itself.
        CelAbstractSyntaxTree ast;
        try {
            ast = CelEnvironmentFactory.builder().addMessageVar("this", declaring)
                    .build().compile(filterCel).getAst();
        } catch (CelValidationException e) {
            violations.add("filter_cel on " + label
                    + " does not compile: " + e.getMessage());
            return;
        }
        if (ast.getResultType().kind() != CelKind.BOOL) {
            violations.add("filter_cel on " + label + " must be bool, got "
                    + ast.getResultType().name());
            return;
        }
        List<EqualsFilter> filters = new ArrayList<>();
        if (!translate(ast.getExpr(), name, declaring, prefix, filters)) {
            violations.add("filter_cel on " + label
                    + " is beyond the translatable subset: "
                    + "equality over string or bool fields, joined by &&");
            return;
        }
        members.put(name, new MetricMember(
                member.name(), member.role(), member.aggregate(), member.fieldName(),
                member.fieldPath(), member.kind(), List.copyOf(filters), member.cel(),
                member.celRequires(), member.defaultGrain(), member.description(),
                member.sensitivity()));
    }

    // -------------------------------------------------- filter_cel translation

    /**
     * Translates the bool AST into equality filters: {@code this.f == lit},
     * bare {@code this.f} and {@code !this.f} on bool fields, joined by
     * {@code &&}. Returns false when the expression falls outside that
     * subset.
     */
    private static boolean translate(
            CelExpr expr, String member, Descriptor descriptor, Prefixes prefix,
            List<EqualsFilter> filters) {
        return switch (expr.getKind()) {
            case CALL -> switch (expr.call().function()) {
                case "_&&_" -> expr.call().args().size() == 2
                        && translate(expr.call().args().getFirst(), member, descriptor, prefix,
                                filters)
                        && translate(expr.call().args().getLast(), member, descriptor, prefix,
                                filters);
                case "_==_" -> translateEquals(expr, member, descriptor, prefix, filters);
                case "!_" -> expr.call().args().size() == 1
                        && translateBare(expr.call().args().getFirst(), member, descriptor,
                                prefix, "false", filters);
                default -> false;
            };
            case SELECT -> translateBare(expr, member, descriptor, prefix, "true", filters);
            default -> false;
        };
    }

    private static boolean translateEquals(
            CelExpr call, String member, Descriptor descriptor, Prefixes prefix,
            List<EqualsFilter> filters) {
        if (call.call().args().size() != 2) {
            return false;
        }
        CelExpr left = call.call().args().getFirst();
        CelExpr right = call.call().args().getLast();
        CelExpr selected = left.getKind() == CelExpr.ExprKind.Kind.SELECT ? left : right;
        CelExpr constant = selected == left ? right : left;
        if (selected.getKind() != CelExpr.ExprKind.Kind.SELECT
                || constant.getKind() != CelExpr.ExprKind.Kind.CONSTANT) {
            return false;
        }
        FieldDescriptor field = selectedField(selected, descriptor);
        if (field == null) {
            return false;
        }
        FieldKind kind = kindOf(field);
        CelConstant value = constant.constant();
        if (kind == FieldKind.KEYWORD && value.getKind() == CelConstant.Kind.STRING_VALUE) {
            filters.add(new EqualsFilter(member, prefix.name() + field.getName(),
                    prefix.path() + field.getName(), DimensionKind.TERM,
                    List.of(value.stringValue())));
            return true;
        }
        if (kind == FieldKind.BOOLEAN && value.getKind() == CelConstant.Kind.BOOLEAN_VALUE) {
            filters.add(new EqualsFilter(member, prefix.name() + field.getName(),
                    prefix.path() + field.getName(), DimensionKind.BOOLEAN,
                    List.of(Boolean.toString(value.booleanValue()))));
            return true;
        }
        return false;
    }

    /** A bare {@code this.f} (or its negation) over a bool field. */
    private static boolean translateBare(
            CelExpr expr, String member, Descriptor descriptor, Prefixes prefix, String value,
            List<EqualsFilter> filters) {
        if (expr.getKind() != CelExpr.ExprKind.Kind.SELECT) {
            return false;
        }
        FieldDescriptor field = selectedField(expr, descriptor);
        if (field == null || kindOf(field) != FieldKind.BOOLEAN) {
            return false;
        }
        filters.add(new EqualsFilter(member, prefix.name() + field.getName(),
                prefix.path() + field.getName(), DimensionKind.BOOLEAN,
                List.of(value)));
        return true;
    }

    /** The field a {@code this.f} select names; null outside that shape. */
    private static FieldDescriptor selectedField(CelExpr select, Descriptor descriptor) {
        CelExpr operand = select.select().operand();
        if (operand.getKind() != CelExpr.ExprKind.Kind.IDENT
                || !"this".equals(operand.ident().name())) {
            return null;
        }
        return descriptor.findFieldByName(select.select().field());
    }

    private static void collectIdents(CelExpr expr, Set<String> idents) {
        switch (expr.getKind()) {
            case IDENT -> idents.add(expr.ident().name());
            case CALL -> {
                expr.call().target().ifPresent(target -> collectIdents(target, idents));
                expr.call().args().forEach(arg -> collectIdents(arg, idents));
            }
            case SELECT -> collectIdents(expr.select().operand(), idents);
            case LIST -> expr.list().elements().forEach(e -> collectIdents(e, idents));
            default -> {
            }
        }
    }

    /**
     * The two accumulated addresses of one walk position: the flattened
     * engine name (underscore-joined) and the proto field path
     * (dot-joined).
     */
    private record Prefixes(String name, String path) {

        Prefixes descend(String fieldName) {
            return new Prefixes(name + fieldName + "_", path + fieldName + ".");
        }
    }
}

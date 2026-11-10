package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.DateRangeFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.NumericRangeFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.RowPredicate;
import ai.protomolt.proto.metric.spi.MetricMapping.FieldKind;
import ai.protomolt.proto.metric.spi.MetricMapping.MetricMember;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TimestampProto;
import dev.cel.bundle.Cel;
import dev.cel.bundle.CelBuilder;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.CelValidationException;
import dev.cel.common.ast.CelConstant;
import dev.cel.common.ast.CelExpr;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.CelType;
import dev.cel.common.types.SimpleType;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema-aware CEL compiler and translator for metric row-security and query
 * filters.
 *
 * <p>Every CEL row filter is parsed and type-checked through
 * {@link CelEnvironmentFactory} against the target {@link MetricMapping}'s
 * declared members and backing protobuf descriptor before being lowered into an
 * engine-neutral {@link RowPredicate}. Any unknown field or member, type
 * mismatch, non-boolean result type, or untranslatable construct is refused
 * closed with {@link MetricRefusal}.
 */
public final class MetricRowPredicates {

    private static final String TIMESTAMP_FULL_NAME = "google.protobuf.Timestamp";
    private static final String TREE_PATH_FULL_NAME = "ai.protomolt.proto.types.v1.TreePath";

    private MetricRowPredicates() {
    }

    /**
     * Resolved target of a field or member reference inside a CEL row filter.
     *
     * @param member the public member name (or proto field path when referencing
     *        an undeclared backing field)
     * @param fieldName the engine's physical flattened field name
     * @param fieldPath the dot-joined protobuf field path
     * @param kind the metric field kind
     * @param floating true when the backing numeric field is IEEE-754 float/double
     */
    public record ResolvedTarget(
            String member,
            String fieldName,
            String fieldPath,
            FieldKind kind,
            boolean floating) {
    }

    /**
     * Compiles and type-checks {@code expression} against {@code mapping},
     * returning the engine-neutral {@link RowPredicate}.
     *
     * @throws MetricRefusal if the expression is blank, fails CEL syntax or
     *         type checking, does not evaluate to {@code bool}, references an
     *         unknown or unsupported member/field, or uses an unsupported
     *         construct
     */
    public static RowPredicate compile(MetricMapping mapping, String expression) {
        if (mapping == null) {
            throw new IllegalArgumentException("mapping must not be null");
        }
        if (expression == null || expression.isBlank()) {
            throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "CEL row filter on '" + mapping.subject() + "' must not be blank",
                    List.of());
        }
        SchemaContext context = SchemaContext.forMapping(mapping, expression);
        CelAbstractSyntaxTree ast;
        try {
            ast = context.cel().compile(expression).getAst();
        } catch (CelValidationException e) {
            throw classifyValidationFailure(mapping, expression, e);
        }
        if (ast.getResultType().kind() != CelKind.BOOL) {
            throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "CEL row filter '" + expression + "' on '" + mapping.subject()
                            + "' must return bool, got " + ast.getResultType().kind(),
                    List.of());
        }
        return translate(mapping, expression, context, ast.getExpr());
    }

    /**
     * Returns all public member names on {@code mapping} referenced by
     * {@code expression} (whether addressed by public member name or by
     * backing proto field path). Used by access-policy rewrites to ensure a
     * caller cannot probe a denied member through a CEL filter.
     */
    public static Set<String> referencedMembers(MetricMapping mapping, String expression) {
        if (expression == null || expression.isBlank()) {
            return Set.of();
        }
        SchemaContext context = SchemaContext.forMapping(mapping, expression);
        CelExpr root;
        try {
            root = context.cel().parse(expression, "row_filter").getAst().getExpr();
        } catch (CelValidationException e) {
            return Set.of();
        }
        Set<String> referenced = new LinkedHashSet<>();
        collectReferencedMembers(mapping, context, root, referenced);
        return Set.copyOf(referenced);
    }

    private static void collectReferencedMembers(
            MetricMapping mapping,
            SchemaContext context,
            CelExpr expr,
            Set<String> out) {
        switch (expr.getKind()) {
            case SELECT, IDENT -> {
                String path = extractReferencePath(expr);
                if (path != null) {
                    ResolvedTarget target = context.targets().get(path);
                    if (target != null) {
                        out.add(target.member());
                        for (MetricMember member : mapping.members().values()) {
                            if (!member.fieldPath().isEmpty()
                                    && (member.fieldPath().equals(target.fieldPath())
                                            || member.fieldName().equals(target.fieldName()))) {
                                out.add(member.name());
                            }
                        }
                    } else if (mapping.members().containsKey(path)) {
                        out.add(path);
                    }
                }
            }
            case CALL -> {
                CelExpr.CelCall call = expr.call();
                call.target().ifPresent(t -> collectReferencedMembers(mapping, context, t, out));
                for (CelExpr arg : call.args()) {
                    collectReferencedMembers(mapping, context, arg, out);
                }
            }
            case LIST -> {
                for (CelExpr elem : expr.list().elements()) {
                    collectReferencedMembers(mapping, context, elem, out);
                }
            }
            default -> {
            }
        }
    }

    private static MetricRefusal classifyValidationFailure(
            MetricMapping mapping, String expression, CelValidationException e) {
        String message = e.getMessage() == null ? "invalid CEL" : e.getMessage();
        if (message.contains("undeclared reference") || message.contains("undefined field")) {
            return new MetricRefusal(MetricRefusal.UNKNOWN_MEMBER,
                    "CEL row filter '" + expression + "' on '" + mapping.subject()
                            + "' references an unknown member or field: " + message,
                    mapping.memberNames());
        }
        return new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                "CEL row filter '" + expression + "' on '" + mapping.subject()
                        + "' failed type checking: " + message,
                List.of());
    }

    private static RowPredicate translate(
            MetricMapping mapping,
            String source,
            SchemaContext context,
            CelExpr expr) {
        return switch (expr.getKind()) {
            case CALL -> translateCall(mapping, source, context, expr.call());
            case SELECT, IDENT -> {
                ResolvedTarget target = resolveTarget(mapping, source, context, expr);
                if (target.kind() != FieldKind.BOOLEAN) {
                    throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                            "CEL row filter '" + source + "' uses '" + target.member()
                                    + "' as a standalone predicate, but its type is "
                                    + target.kind() + ", not BOOLEAN",
                            List.of());
                }
                yield new RowPredicate.Terms(new EqualsFilter(
                        target.member(), target.fieldName(), target.fieldPath(),
                        DimensionKind.BOOLEAN, List.of("true")));
            }
            default -> throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "CEL row filter '" + source + "' on '" + mapping.subject()
                            + "' must be a boolean predicate over mapping fields, got "
                            + expr.getKind(),
                    List.of());
        };
    }

    private static RowPredicate translateCall(
            MetricMapping mapping,
            String source,
            SchemaContext context,
            CelExpr.CelCall call) {
        String fn = call.function();
        return switch (fn) {
            case "_&&_" -> {
                List<RowPredicate> children = new ArrayList<>();
                for (CelExpr arg : call.args()) {
                    RowPredicate child = translate(mapping, source, context, arg);
                    if (child instanceof RowPredicate.And and) {
                        children.addAll(and.children());
                    } else {
                        children.add(child);
                    }
                }
                yield new RowPredicate.And(children);
            }
            case "_||_" -> {
                List<RowPredicate> children = new ArrayList<>();
                for (CelExpr arg : call.args()) {
                    RowPredicate child = translate(mapping, source, context, arg);
                    if (child instanceof RowPredicate.Or or) {
                        children.addAll(or.children());
                    } else {
                        children.add(child);
                    }
                }
                yield new RowPredicate.Or(children);
            }
            case "!_" -> {
                if (call.args().size() != 1) {
                    throw unsupported(mapping, source, "negation expects 1 argument");
                }
                RowPredicate inner = translate(mapping, source, context, call.args().get(0));
                Set<String> fieldNames = new LinkedHashSet<>();
                List<String> fieldPaths = new ArrayList<>();
                collectFields(inner, fieldNames, fieldPaths);
                yield new RowPredicate.Not(inner, fieldNames, fieldPaths);
            }
            case "_==_", "_!=_" -> translateEquality(mapping, source, context, call, "_!=_".equals(fn));
            case "@in", "_in_" -> translateIn(mapping, source, context, call);
            case "_<_", "_<=_", "_>_", "_>=_" -> translateComparison(mapping, source, context, call, fn);
            default -> throw unsupported(mapping, source,
                    "unsupported operator or function '" + fn + "'");
        };
    }

    private static RowPredicate translateEquality(
            MetricMapping mapping,
            String source,
            SchemaContext context,
            CelExpr.CelCall call,
            boolean negated) {
        if (call.args().size() != 2) {
            throw unsupported(mapping, source, "equality expects 2 arguments");
        }
        CelExpr left = call.args().get(0);
        CelExpr right = call.args().get(1);
        CelExpr refExpr;
        CelExpr litExpr;
        if (isReference(left) && !isReference(right)) {
            refExpr = left;
            litExpr = right;
        } else if (!isReference(left) && isReference(right)) {
            refExpr = right;
            litExpr = left;
        } else {
            throw unsupported(mapping, source,
                    "equality must compare a field or member to a literal value");
        }
        ResolvedTarget target = resolveTarget(mapping, source, context, refExpr);
        RowPredicate positive = switch (target.kind()) {
            case KEYWORD -> {
                String value = expectStringLiteral(mapping, source, litExpr, target.member());
                if (value.isEmpty()) {
                    throw unsupported(mapping, source,
                            "string literal on '" + target.member() + "' must not be empty");
                }
                yield new RowPredicate.Terms(new EqualsFilter(
                        target.member(), target.fieldName(), target.fieldPath(),
                        DimensionKind.TERM, List.of(value)));
            }
            case BOOLEAN -> {
                boolean value = expectBoolLiteral(mapping, source, litExpr, target.member());
                yield new RowPredicate.Terms(new EqualsFilter(
                        target.member(), target.fieldName(), target.fieldPath(),
                        DimensionKind.BOOLEAN, List.of(Boolean.toString(value))));
            }
            case NUMERIC -> {
                NumericLiteral num = expectNumericLiteral(mapping, source, litExpr, target.member());
                NumericRangeFilter range = (!target.floating() && num.integral())
                        ? NumericRangeFilter.ofLong(
                                target.member(), target.fieldName(), target.fieldPath(),
                                num.longValue(), num.longValue())
                        : NumericRangeFilter.ofDouble(
                                target.member(), target.fieldName(), target.fieldPath(),
                                num.doubleValue(), true, num.doubleValue(), true);
                yield new RowPredicate.NumericRange(range);
            }
            case DATE -> {
                long epochMillis = expectTimestampMillis(mapping, source, litExpr, target.member());
                yield new RowPredicate.DateRange(new DateRangeFilter(
                        target.member(), target.fieldName(), target.fieldPath(),
                        epochMillis, epochMillis));
            }
            case TREE_PATH -> throw unsupported(mapping, source,
                    "TREE_PATH member '" + target.member() + "' filters by prefix, not CEL equality");
            case SYNTHETIC -> throw unsupported(mapping, source,
                    "synthetic member '" + target.member() + "' cannot be filtered in CEL");
        };
        if (!negated) {
            return positive;
        }
        return new RowPredicate.Not(
                positive, Set.of(target.fieldName()), List.of(target.fieldPath()));
    }

    private static RowPredicate translateIn(
            MetricMapping mapping,
            String source,
            SchemaContext context,
            CelExpr.CelCall call) {
        if (call.args().size() != 2) {
            throw unsupported(mapping, source, "'in' expects a field and a literal list");
        }
        CelExpr left = call.args().get(0);
        CelExpr right = call.args().get(1);
        if (!isReference(left)
                || right.getKind() != CelExpr.ExprKind.Kind.LIST) {
            throw unsupported(mapping, source,
                    "'in' must test a field or member against a literal list");
        }
        List<CelExpr> elements = right.list().elements();
        if (elements.isEmpty()) {
            throw unsupported(mapping, source, "'in' list must not be empty");
        }
        ResolvedTarget target = resolveTarget(mapping, source, context, left);
        return switch (target.kind()) {
            case KEYWORD -> {
                List<String> values = new ArrayList<>(elements.size());
                for (CelExpr elem : elements) {
                    String val = expectStringLiteral(mapping, source, elem, target.member());
                    if (val.isEmpty()) {
                        throw unsupported(mapping, source,
                                "'in' list on '" + target.member() + "' contains an empty string");
                    }
                    values.add(val);
                }
                yield new RowPredicate.Terms(new EqualsFilter(
                        target.member(), target.fieldName(), target.fieldPath(),
                        DimensionKind.TERM, values));
            }
            case BOOLEAN -> {
                List<String> values = new ArrayList<>(elements.size());
                for (CelExpr elem : elements) {
                    boolean val = expectBoolLiteral(mapping, source, elem, target.member());
                    values.add(Boolean.toString(val));
                }
                yield new RowPredicate.Terms(new EqualsFilter(
                        target.member(), target.fieldName(), target.fieldPath(),
                        DimensionKind.BOOLEAN, values));
            }
            case NUMERIC -> {
                List<RowPredicate> branches = new ArrayList<>(elements.size());
                for (CelExpr elem : elements) {
                    NumericLiteral num = expectNumericLiteral(
                            mapping, source, elem, target.member());
                    NumericRangeFilter range = (!target.floating() && num.integral())
                            ? NumericRangeFilter.ofLong(
                                    target.member(), target.fieldName(), target.fieldPath(),
                                    num.longValue(), num.longValue())
                            : NumericRangeFilter.ofDouble(
                                    target.member(), target.fieldName(), target.fieldPath(),
                                    num.doubleValue(), true, num.doubleValue(), true);
                    branches.add(new RowPredicate.NumericRange(range));
                }
                yield branches.size() == 1 ? branches.get(0) : new RowPredicate.Or(branches);
            }
            default -> throw unsupported(mapping, source,
                    "'in' is not supported on " + target.kind() + " member '" + target.member() + "'");
        };
    }

    private static RowPredicate translateComparison(
            MetricMapping mapping,
            String source,
            SchemaContext context,
            CelExpr.CelCall call,
            String operator) {
        if (call.args().size() != 2) {
            throw unsupported(mapping, source, "comparison expects 2 arguments");
        }
        CelExpr left = call.args().get(0);
        CelExpr right = call.args().get(1);
        CelExpr refExpr;
        CelExpr litExpr;
        String op;
        if (isReference(left) && !isReference(right)) {
            refExpr = left;
            litExpr = right;
            op = operator;
        } else if (!isReference(left) && isReference(right)) {
            refExpr = right;
            litExpr = left;
            op = flipOperator(operator);
        } else {
            throw unsupported(mapping, source,
                    "comparison must compare a field or member to a literal value");
        }
        ResolvedTarget target = resolveTarget(mapping, source, context, refExpr);
        return switch (target.kind()) {
            case NUMERIC -> {
                NumericLiteral num = expectNumericLiteral(mapping, source, litExpr, target.member());
                if (!target.floating() && num.integral()) {
                    long bound = num.longValue();
                    try {
                        NumericRangeFilter filter = switch (op) {
                            case "_>=_" -> NumericRangeFilter.ofLong(
                                    target.member(), target.fieldName(), target.fieldPath(),
                                    bound, null);
                            case "_>_" -> NumericRangeFilter.ofLong(
                                    target.member(), target.fieldName(), target.fieldPath(),
                                    Math.addExact(bound, 1L), null);
                            case "_<=_" -> NumericRangeFilter.ofLong(
                                    target.member(), target.fieldName(), target.fieldPath(),
                                    null, bound);
                            case "_<_" -> NumericRangeFilter.ofLong(
                                    target.member(), target.fieldName(), target.fieldPath(),
                                    null, Math.subtractExact(bound, 1L));
                            default -> throw unsupported(mapping, source, "unexpected operator " + op);
                        };
                        yield new RowPredicate.NumericRange(filter);
                    } catch (ArithmeticException e) {
                        throw unsupported(mapping, source,
                                "integer comparison bound overflows 64-bit integer range");
                    }
                }
                double bound = num.doubleValue();
                NumericRangeFilter filter = switch (op) {
                    case "_>=_" -> NumericRangeFilter.ofDouble(
                            target.member(), target.fieldName(), target.fieldPath(),
                            bound, true, null, true);
                    case "_>_" -> NumericRangeFilter.ofDouble(
                            target.member(), target.fieldName(), target.fieldPath(),
                            bound, false, null, true);
                    case "_<=_" -> NumericRangeFilter.ofDouble(
                            target.member(), target.fieldName(), target.fieldPath(),
                            null, true, bound, true);
                    case "_<_" -> NumericRangeFilter.ofDouble(
                            target.member(), target.fieldName(), target.fieldPath(),
                            null, true, bound, false);
                    default -> throw unsupported(mapping, source, "unexpected operator " + op);
                };
                yield new RowPredicate.NumericRange(filter);
            }
            case DATE -> {
                long epochMillis = expectTimestampMillis(mapping, source, litExpr, target.member());
                DateRangeFilter filter;
                try {
                    filter = switch (op) {
                        case "_>=_" -> new DateRangeFilter(
                                target.member(), target.fieldName(), target.fieldPath(),
                                epochMillis, null);
                        case "_>_" -> new DateRangeFilter(
                                target.member(), target.fieldName(), target.fieldPath(),
                                Math.addExact(epochMillis, 1L), null);
                        case "_<=_" -> new DateRangeFilter(
                                target.member(), target.fieldName(), target.fieldPath(),
                                null, epochMillis);
                        case "_<_" -> new DateRangeFilter(
                                target.member(), target.fieldName(), target.fieldPath(),
                                null, Math.subtractExact(epochMillis, 1L));
                        default -> throw unsupported(mapping, source, "unexpected operator " + op);
                    };
                } catch (ArithmeticException e) {
                    throw unsupported(mapping, source,
                            "timestamp comparison bound overflows the epoch-millisecond range");
                }
                yield new RowPredicate.DateRange(filter);
            }
            default -> throw unsupported(mapping, source,
                    "ordered comparison is only supported on NUMERIC or DATE members, got "
                            + target.kind() + " on '" + target.member() + "'");
        };
    }

    private static String flipOperator(String op) {
        return switch (op) {
            case "_<_" -> "_>_";
            case "_<=_" -> "_>=_";
            case "_>_" -> "_<_";
            case "_>=_" -> "_<=_";
            default -> op;
        };
    }

    private static void collectFields(
            RowPredicate predicate, Set<String> fieldNames, List<String> fieldPaths) {
        switch (predicate) {
            case RowPredicate.And and -> {
                for (RowPredicate child : and.children()) {
                    collectFields(child, fieldNames, fieldPaths);
                }
            }
            case RowPredicate.Or or -> {
                for (RowPredicate child : or.children()) {
                    collectFields(child, fieldNames, fieldPaths);
                }
            }
            case RowPredicate.Not not -> {
                fieldNames.addAll(not.fieldNames());
                for (String path : not.fieldPaths()) {
                    if (!fieldPaths.contains(path)) {
                        fieldPaths.add(path);
                    }
                }
            }
            case RowPredicate.Terms terms -> {
                fieldNames.add(terms.filter().fieldName());
                if (!fieldPaths.contains(terms.filter().fieldPath())) {
                    fieldPaths.add(terms.filter().fieldPath());
                }
            }
            case RowPredicate.NumericRange num -> {
                fieldNames.add(num.filter().fieldName());
                if (!fieldPaths.contains(num.filter().fieldPath())) {
                    fieldPaths.add(num.filter().fieldPath());
                }
            }
            case RowPredicate.DateRange date -> {
                fieldNames.add(date.filter().fieldName());
                if (!fieldPaths.contains(date.filter().fieldPath())) {
                    fieldPaths.add(date.filter().fieldPath());
                }
            }
        }
    }

    private static boolean isReference(CelExpr expr) {
        if (expr.getKind() == CelExpr.ExprKind.Kind.SELECT) {
            return extractReferencePath(expr) != null;
        }
        if (expr.getKind() == CelExpr.ExprKind.Kind.IDENT) {
            String name = expr.ident().name();
            return !"true".equals(name) && !"false".equals(name) && !"null".equals(name);
        }
        return false;
    }

    private static String extractReferencePath(CelExpr expr) {
        if (expr.getKind() == CelExpr.ExprKind.Kind.IDENT) {
            String name = expr.ident().name();
            if ("this".equals(name) || "true".equals(name)
                    || "false".equals(name) || "null".equals(name)) {
                return null;
            }
            return name;
        }
        List<String> parts = new ArrayList<>();
        CelExpr current = expr;
        while (current.getKind() == CelExpr.ExprKind.Kind.SELECT) {
            CelExpr.CelSelect select = current.select();
            if (select.testOnly()) {
                return null;
            }
            parts.add(0, select.field());
            current = select.operand();
        }
        if (current.getKind() != CelExpr.ExprKind.Kind.IDENT) {
            return null;
        }
        String root = current.ident().name();
        if (!"this".equals(root)) {
            parts.add(0, root);
        }
        return parts.isEmpty() ? null : String.join(".", parts);
    }

    private static ResolvedTarget resolveTarget(
            MetricMapping mapping,
            String source,
            SchemaContext context,
            CelExpr expr) {
        String path = extractReferencePath(expr);
        if (path == null) {
            throw unsupported(mapping, source, "expected a member or field reference");
        }
        MetricMember directMember = mapping.members().get(path);
        if (directMember != null) {
            if (directMember.calculated() || directMember.fieldName().isEmpty()
                    || directMember.kind() == FieldKind.SYNTHETIC) {
                throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                        "CEL row filter '" + source + "' on '" + mapping.subject()
                                + "' cannot filter on synthetic or calculated member '"
                                + directMember.name() + "'",
                        List.of());
            }
            if (directMember.kind() == FieldKind.TREE_PATH) {
                throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                        "CEL row filter '" + source + "' on '" + mapping.subject()
                                + "' cannot filter on TREE_PATH member '" + directMember.name()
                                + "'; use a prefix filter",
                        List.of());
            }
        }
        ResolvedTarget target = context.targets().get(path);
        if (target == null) {
            throw new MetricRefusal(MetricRefusal.UNKNOWN_MEMBER,
                    "CEL row filter '" + source + "' on '" + mapping.subject()
                            + "' references unknown member or field '" + path + "'",
                    mapping.memberNames());
        }
        return target;
    }

    private static String expectStringLiteral(
            MetricMapping mapping, String source, CelExpr expr, String member) {
        if (expr.getKind() == CelExpr.ExprKind.Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.STRING_VALUE) {
            return expr.constant().stringValue();
        }
        throw unsupported(mapping, source,
                "expected a string literal for '" + member + "'");
    }

    private static boolean expectBoolLiteral(
            MetricMapping mapping, String source, CelExpr expr, String member) {
        if (expr.getKind() == CelExpr.ExprKind.Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.BOOLEAN_VALUE) {
            return expr.constant().booleanValue();
        }
        throw unsupported(mapping, source,
                "expected a boolean literal for '" + member + "'");
    }

    private record NumericLiteral(boolean integral, long longValue, double doubleValue) {
    }

    private static NumericLiteral expectNumericLiteral(
            MetricMapping mapping, String source, CelExpr expr, String member) {
        if (expr.getKind() == CelExpr.ExprKind.Kind.CONSTANT) {
            CelConstant constant = expr.constant();
            return switch (constant.getKind()) {
                case INT64_VALUE -> new NumericLiteral(
                        true, constant.int64Value(), (double) constant.int64Value());
                case UINT64_VALUE -> {
                    long raw = constant.uint64Value().longValue();
                    if (raw < 0) {
                        throw unsupported(mapping, source, "numeric literal for '" + member
                                + "' exceeds the signed 64-bit integer range");
                    }
                    yield new NumericLiteral(true, raw, (double) raw);
                }
                case DOUBLE_VALUE -> {
                    double d = constant.doubleValue();
                    if (!Double.isFinite(d)) {
                        throw unsupported(mapping, source,
                                "numeric literal for '" + member + "' must be finite");
                    }
                    yield new NumericLiteral(false, 0L, d);
                }
                default -> throw unsupported(mapping, source,
                        "expected a numeric literal for '" + member + "'");
            };
        }
        if (expr.getKind() == CelExpr.ExprKind.Kind.CALL
                && "-_".equals(expr.call().function())
                && expr.call().args().size() == 1) {
            NumericLiteral inner = expectNumericLiteral(
                    mapping, source, expr.call().args().get(0), member);
            if (inner.integral()) {
                long neg;
                try {
                    neg = Math.negateExact(inner.longValue());
                } catch (ArithmeticException e) {
                    throw unsupported(mapping, source, "negated numeric literal for '" + member
                            + "' overflows the signed 64-bit integer range");
                }
                return new NumericLiteral(true, neg, (double) neg);
            }
            return new NumericLiteral(false, 0L, -inner.doubleValue());
        }
        throw unsupported(mapping, source,
                "expected a numeric literal for '" + member + "'");
    }

    private static long expectTimestampMillis(
            MetricMapping mapping, String source, CelExpr expr, String member) {
        String text = null;
        if (expr.getKind() == CelExpr.ExprKind.Kind.CALL
                && "timestamp".equals(expr.call().function())
                && expr.call().args().size() == 1) {
            text = expectStringLiteral(mapping, source, expr.call().args().get(0), member);
        } else if (expr.getKind() == CelExpr.ExprKind.Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.STRING_VALUE) {
            text = expr.constant().stringValue();
        }
        if (text == null) {
            throw unsupported(mapping, source,
                    "expected timestamp(\"...\") or ISO-8601 date literal for '" + member + "'");
        }
        try {
            if (text.contains("T")) {
                return Instant.parse(text).toEpochMilli();
            }
            return LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            throw unsupported(mapping, source,
                    "invalid timestamp or ISO date '" + text + "' for '" + member + "'");
        }
    }

    private static MetricRefusal unsupported(
            MetricMapping mapping, String source, String detail) {
        return new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                "CEL row filter '" + source + "' on '" + mapping.subject()
                        + "' is not supported: " + detail,
                List.of());
    }

    private record SchemaContext(Cel cel, Map<String, ResolvedTarget> targets) {

        static SchemaContext forMapping(MetricMapping mapping, String expression) {
            Map<String, ResolvedTarget> targets = new LinkedHashMap<>();
            Map<String, CelType> topLevelTypes = new LinkedHashMap<>();
            boolean hasDoubleLiteral = containsDoubleLiteral(expression);

            if (mapping.descriptor() != null) {
                Set<String> visiting = new LinkedHashSet<>();
                visiting.add(mapping.descriptor().getFullName());
                collectDescriptorTargets(
                        mapping.descriptor(), "", "", visiting, targets, topLevelTypes);
            }

            for (MetricMember member : mapping.members().values()) {
                FieldKind effectiveKind = member.kind() != null
                        ? member.kind()
                        : FieldKind.NUMERIC;
                if (member.calculated() || member.fieldName().isEmpty()
                        || effectiveKind == FieldKind.TREE_PATH
                        || effectiveKind == FieldKind.SYNTHETIC) {
                    topLevelTypes.put(member.name(), celTypeFor(effectiveKind, true));
                    continue;
                }
                ResolvedTarget existingByPath = targets.get(member.fieldPath());
                boolean floating = existingByPath != null
                        ? existingByPath.floating()
                        : (effectiveKind == FieldKind.NUMERIC && hasDoubleLiteral);
                ResolvedTarget resolved = new ResolvedTarget(
                        member.name(),
                        member.fieldName(),
                        member.fieldPath().isEmpty() ? member.fieldName() : member.fieldPath(),
                        effectiveKind,
                        floating);
                targets.put(member.name(), resolved);
                if (!member.fieldPath().isEmpty()) {
                    targets.putIfAbsent(member.fieldPath(), resolved);
                }
                if (!member.fieldName().isEmpty()) {
                    targets.putIfAbsent(member.fieldName(), resolved);
                }
                topLevelTypes.put(member.name(), celTypeFor(effectiveKind, floating));
            }

            Descriptor celDescriptor = buildCelDescriptor(mapping, targets);
            CelEnvironmentFactory env = CelEnvironmentFactory.builder()
                    .addMessageType(TimestampProto.getDescriptor().getMessageTypes().get(0))
                    .addMessageVar("this", celDescriptor);
            for (Map.Entry<String, CelType> entry : topLevelTypes.entrySet()) {
                if (!"this".equals(entry.getKey()) && !entry.getKey().contains(".")) {
                    env.addVar(entry.getKey(), entry.getValue());
                }
            }
            CelBuilder compiler = env.advisoryBuilder();
            addNumericComparisonOverloads(compiler);
            return new SchemaContext(compiler.build(), targets);
        }

        private static void collectDescriptorTargets(
                Descriptor type,
                String flatPrefix,
                String dotPrefix,
                Set<String> visiting,
                Map<String, ResolvedTarget> targets,
                Map<String, CelType> topLevelTypes) {
            for (FieldDescriptor field : type.getFields()) {
                if (field.isMapField() || field.isRepeated()) {
                    continue;
                }
                String flatName = flatPrefix.isEmpty()
                        ? field.getName()
                        : flatPrefix + "_" + field.getName();
                String dotPath = dotPrefix.isEmpty()
                        ? field.getName()
                        : dotPrefix + "." + field.getName();
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    String msgName = field.getMessageType().getFullName();
                    if (TIMESTAMP_FULL_NAME.equals(msgName)) {
                        ResolvedTarget target = new ResolvedTarget(
                                dotPath, flatName, dotPath, FieldKind.DATE, false);
                        targets.put(dotPath, target);
                        if (dotPrefix.isEmpty()) {
                            topLevelTypes.put(field.getName(), SimpleType.TIMESTAMP);
                        }
                    } else if (!TREE_PATH_FULL_NAME.equals(msgName) && visiting.add(msgName)) {
                        collectDescriptorTargets(
                                field.getMessageType(), flatName, dotPath,
                                visiting, targets, topLevelTypes);
                        visiting.remove(msgName);
                    }
                    continue;
                }
                FieldKind kind = switch (field.getJavaType()) {
                    case STRING, ENUM -> FieldKind.KEYWORD;
                    case BOOLEAN -> FieldKind.BOOLEAN;
                    case INT, LONG, FLOAT, DOUBLE -> FieldKind.NUMERIC;
                    default -> null;
                };
                if (kind == null) {
                    continue;
                }
                boolean floating = field.getJavaType() == FieldDescriptor.JavaType.FLOAT
                        || field.getJavaType() == FieldDescriptor.JavaType.DOUBLE;
                ResolvedTarget target = new ResolvedTarget(
                        dotPath, flatName, dotPath, kind, floating);
                targets.put(dotPath, target);
                if (dotPrefix.isEmpty()) {
                    topLevelTypes.put(field.getName(), celTypeFor(kind, floating));
                }
            }
        }

        private static CelType celTypeFor(FieldKind kind, boolean floating) {
            return switch (kind) {
                case KEYWORD, TREE_PATH -> SimpleType.STRING;
                case BOOLEAN -> SimpleType.BOOL;
                case NUMERIC, SYNTHETIC -> floating ? SimpleType.DOUBLE : SimpleType.INT;
                case DATE -> SimpleType.TIMESTAMP;
            };
        }

        private static Descriptor buildCelDescriptor(
                MetricMapping mapping, Map<String, ResolvedTarget> targets) {
            Descriptor base = mapping.descriptor();
            DescriptorProto.Builder msg = DescriptorProto.newBuilder()
                    .setName("CelRowContext");
            Set<String> presentTopLevel = new LinkedHashSet<>();
            int nextFieldNumber = 1;
            List<FileDescriptor> deps = new ArrayList<>();
            deps.add(TimestampProto.getDescriptor());

            if (base != null) {
                deps.add(base.getFile());
                for (FieldDescriptor f : base.getFields()) {
                    msg.addField(f.toProto());
                    presentTopLevel.add(f.getName());
                    if (f.getNumber() >= nextFieldNumber) {
                        nextFieldNumber = f.getNumber() + 1;
                    }
                }
            }

            for (MetricMember member : mapping.members().values()) {
                String name = member.name();
                if (name.contains(".") || !presentTopLevel.add(name)) {
                    continue;
                }
                FieldKind kind = member.kind() != null ? member.kind() : FieldKind.NUMERIC;
                ResolvedTarget target = targets.get(name);
                boolean floating = target == null || target.floating();
                FieldDescriptorProto.Builder fb = FieldDescriptorProto.newBuilder()
                        .setName(name)
                        .setNumber(nextFieldNumber++)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
                switch (kind) {
                    case KEYWORD, TREE_PATH -> fb.setType(FieldDescriptorProto.Type.TYPE_STRING);
                    case BOOLEAN -> fb.setType(FieldDescriptorProto.Type.TYPE_BOOL);
                    case NUMERIC, SYNTHETIC -> fb.setType(floating
                            ? FieldDescriptorProto.Type.TYPE_DOUBLE
                            : FieldDescriptorProto.Type.TYPE_INT64);
                    case DATE -> fb.setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Timestamp");
                }
                msg.addField(fb.build());
            }

            for (Map.Entry<String, ResolvedTarget> entry : targets.entrySet()) {
                String name = entry.getKey();
                if (name.contains(".") || !presentTopLevel.add(name)) {
                    continue;
                }
                ResolvedTarget target = entry.getValue();
                FieldDescriptorProto.Builder fb = FieldDescriptorProto.newBuilder()
                        .setName(name)
                        .setNumber(nextFieldNumber++)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
                switch (target.kind()) {
                    case KEYWORD, TREE_PATH -> fb.setType(FieldDescriptorProto.Type.TYPE_STRING);
                    case BOOLEAN -> fb.setType(FieldDescriptorProto.Type.TYPE_BOOL);
                    case NUMERIC, SYNTHETIC -> fb.setType(target.floating()
                            ? FieldDescriptorProto.Type.TYPE_DOUBLE
                            : FieldDescriptorProto.Type.TYPE_INT64);
                    case DATE -> fb.setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Timestamp");
                }
                msg.addField(fb.build());
            }

            FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                    .setName("protomolt/metric/cel_row_context.proto")
                    .setPackage("ai.protomolt.metric.cel")
                    .setSyntax("proto3")
                    .addDependency(TimestampProto.getDescriptor().getName())
                    .addMessageType(msg.build());
            if (base != null) {
                file.addDependency(base.getFile().getName());
            }
            try {
                FileDescriptor built = FileDescriptor.buildFrom(
                        file.build(), deps.toArray(FileDescriptor[]::new));
                return built.findMessageTypeByName("CelRowContext");
            } catch (Exception e) {
                throw new IllegalStateException(
                        "failed to build CEL row context descriptor for " + mapping.subject(), e);
            }
        }

        private static void addNumericComparisonOverloads(CelBuilder compiler) {
            String[] ops = {"_<_", "_<=_", "_>_", "_>=_"};
            for (String op : ops) {
                String clean = op.replace("_", "")
                        .replace("<", "lt")
                        .replace(">", "gt")
                        .replace("=", "eq");
                compiler.addFunctionDeclarations(CelFunctionDecl.newFunctionDeclaration(
                        op,
                        CelOverloadDecl.newGlobalOverload(
                                clean + "_int_double",
                                SimpleType.BOOL,
                                List.of(SimpleType.INT, SimpleType.DOUBLE)),
                        CelOverloadDecl.newGlobalOverload(
                                clean + "_double_int",
                                SimpleType.BOOL,
                                List.of(SimpleType.DOUBLE, SimpleType.INT)),
                        CelOverloadDecl.newGlobalOverload(
                                clean + "_timestamp_string",
                                SimpleType.BOOL,
                                List.of(SimpleType.TIMESTAMP, SimpleType.STRING)),
                        CelOverloadDecl.newGlobalOverload(
                                clean + "_string_timestamp",
                                SimpleType.BOOL,
                                List.of(SimpleType.STRING, SimpleType.TIMESTAMP))));
            }
        }

        private static boolean containsDoubleLiteral(String expression) {
            try {
                CelExpr root = CelEnvironmentFactory.builder().build()
                        .parse(expression, "probe").getAst().getExpr();
                return hasDoubleConstant(root);
            } catch (CelValidationException e) {
                return false;
            }
        }

        private static boolean hasDoubleConstant(CelExpr expr) {
            return switch (expr.getKind()) {
                case CONSTANT -> expr.constant().getKind() == CelConstant.Kind.DOUBLE_VALUE;
                case CALL -> {
                    for (CelExpr arg : expr.call().args()) {
                        if (hasDoubleConstant(arg)) {
                            yield true;
                        }
                    }
                    yield false;
                }
                case LIST -> {
                    for (CelExpr elem : expr.list().elements()) {
                        if (hasDoubleConstant(elem)) {
                            yield true;
                        }
                    }
                    yield false;
                }
                default -> false;
            };
        }
    }
}

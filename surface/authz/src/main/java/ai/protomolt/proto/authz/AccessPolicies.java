package ai.protomolt.proto.authz;

import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.types.DateRange;
import ai.protomolt.proto.types.DoubleRange;
import ai.protomolt.proto.types.LongRange;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.cel.bundle.Cel;
import dev.cel.common.CelValidationException;
import dev.cel.common.ast.CelExpr;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Access-policy preconditions shared by everything that accepts one: the document's own
 * declared rules must hold, principal names and credential digests must be unique across
 * the document, and every scope must be in the closed vocabulary. A policy failing here is
 * the holder's error and refuses loudly naming the defect; a typo in a scope is a refusal,
 * never a silently dead grant.
 */
public final class AccessPolicies {

    private AccessPolicies() {
    }

    /** Refuses an invalid or ambiguous policy; returns it for chaining. */
    public static AccessPolicy requireWellFormed(AccessPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("access policy must not be null");
        }
        ValidationResult rules = ProtoValidator.create().validate(policy);
        if (!rules.valid()) {
            throw new IllegalArgumentException("access policy is invalid: "
                    + rules.violations().stream()
                            .map(violation -> violation.path() + ": " + violation.message())
                            .collect(Collectors.joining("; ")));
        }
        Set<String> names = new HashSet<>();
        Set<String> digests = new HashSet<>();
        for (Principal principal : policy.getPrincipalsList()) {
            if (!names.add(principal.getName())) {
                throw new IllegalArgumentException(
                        "access policy duplicates principal '" + principal.getName() + "'");
            }
            for (String digest : principal.getCredentialSha256List()) {
                if (!digests.add(digest)) {
                    throw new IllegalArgumentException("access policy duplicates credential "
                            + "digest '" + digest.substring(0, 12) + "…' under principal '"
                            + principal.getName() + "'");
                }
            }
            for (String scope : principal.getScopesList()) {
                if (!Scopes.VOCABULARY.contains(scope)) {
                    throw new IllegalArgumentException("principal '" + principal.getName()
                            + "' names unknown scope '" + scope + "'; the vocabulary is "
                            + String.join(", ", Scopes.VOCABULARY));
                }
            }
            Set<String> budgeted = new HashSet<>();
            for (ScopeBudget budget : principal.getBudgetsList()) {
                if (!principal.getScopesList().contains(budget.getScope())) {
                    throw new IllegalArgumentException("principal '" + principal.getName()
                            + "' budgets scope '" + budget.getScope()
                            + "', which it does not hold");
                }
                if (!budgeted.add(budget.getScope())) {
                    throw new IllegalArgumentException("principal '" + principal.getName()
                            + "' budgets scope '" + budget.getScope() + "' twice");
                }
                if (budget.getRequestsPerMinute() == 0
                        && budget.getMaxPayloadBytes() == 0) {
                    throw new IllegalArgumentException("principal '" + principal.getName()
                            + "' budget on '" + budget.getScope() + "' caps nothing: set"
                            + " requests_per_minute or max_payload_bytes");
                }
            }
            if (principal.hasMetricAccess()) {
                MetricAccess metricAccess = principal.getMetricAccess();
                if (metricAccess.getDenyCount() == 0
                        && metricAccess.getRowFiltersCount() == 0) {
                    throw new IllegalArgumentException("principal '" + principal.getName()
                            + "' metric access restricts nothing: add deny entries or"
                            + " row filters");
                }
                Set<String> deniedSubjects = new HashSet<>();
                for (MetricMemberDeny deny : metricAccess.getDenyList()) {
                    if (!deniedSubjects.add(deny.getMappingSubject())) {
                        throw new IllegalArgumentException("principal '"
                                + principal.getName() + "' metric access lists subject '"
                                + deny.getMappingSubject() + "' twice under deny");
                    }
                }
                Set<String> filtered = new HashSet<>();
                Set<String> filteredCel = new HashSet<>();
                for (MetricRowFilter filter : metricAccess.getRowFiltersList()) {
                    validateRowFilter(principal.getName(), filter);
                    if (filter.getCel().isEmpty()) {
                        if (!filtered.add(filter.getMappingSubject() + " "
                                + filter.getMember())) {
                            throw new IllegalArgumentException("principal '"
                                    + principal.getName() + "' metric access filters member '"
                                    + filter.getMember() + "' on '"
                                    + filter.getMappingSubject() + "' twice");
                        }
                    } else {
                        if (!filteredCel.add(filter.getMappingSubject() + " "
                                + filter.getCel().trim())) {
                            throw new IllegalArgumentException("principal '"
                                    + principal.getName() + "' metric access duplicates CEL filter"
                                    + " on '" + filter.getMappingSubject() + "'");
                        }
                    }
                }
            }
        }
        return policy;
    }

    private static final Cel ROW_FILTER_PARSER = CelEnvironmentFactory.builder().build();

    private static final Set<String> BOOLEAN_OPERATORS = Set.of(
            "_&&_", "_||_", "!_", "_==_", "_!=_", "_<_", "_<=_", "_>_", "_>=_", "@in", "_in_");

    private static void validateRowFilter(String principalName, MetricRowFilter filter) {
        if (filter.hasRange()) {
            DateRange range = filter.getRange();
            LocalDate lower = range.hasBegin()
                    ? LocalDate.parse(range.getBegin())
                            .plusDays(!range.hasIncludeHead() || range.getIncludeHead() ? 0 : 1)
                    : null;
            LocalDate upper = range.hasEnd()
                    ? LocalDate.parse(range.getEnd())
                            .minusDays(!range.hasIncludeTail() || range.getIncludeTail() ? 0 : 1)
                    : null;
            if (lower != null && upper != null && lower.isAfter(upper)) {
                throw new IllegalArgumentException("principal '" + principalName
                        + "' metric row filter on '" + filter.getMappingSubject() + "."
                        + filter.getMember() + "' has an empty date range after exclusive bounds");
            }
        }
        if (filter.hasLongRange()) {
            LongRange range = filter.getLongRange();
            try {
                long lower = range.hasBegin()
                        ? (!range.hasIncludeHead() || range.getIncludeHead()
                                ? range.getBegin() : Math.addExact(range.getBegin(), 1L))
                        : Long.MIN_VALUE;
                long upper = range.hasEnd()
                        ? (!range.hasIncludeTail() || range.getIncludeTail()
                                ? range.getEnd() : Math.subtractExact(range.getEnd(), 1L))
                        : Long.MAX_VALUE;
                if (lower > upper) {
                    throw new IllegalArgumentException("principal '" + principalName
                            + "' metric row filter on '" + filter.getMappingSubject() + "."
                            + filter.getMember() + "' has an empty long_range after exclusive bounds");
                }
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("principal '" + principalName
                        + "' metric row filter on '" + filter.getMappingSubject() + "."
                        + filter.getMember() + "' overflows 64-bit integer bounds", e);
            }
        }
        if (filter.hasDoubleRange()) {
            DoubleRange range = filter.getDoubleRange();
            double lower = range.hasBegin()
                    ? (!range.hasIncludeHead() || range.getIncludeHead()
                            ? range.getBegin() : Math.nextUp(range.getBegin()))
                    : -Double.MAX_VALUE;
            double upper = range.hasEnd()
                    ? (!range.hasIncludeTail() || range.getIncludeTail()
                            ? range.getEnd() : Math.nextDown(range.getEnd()))
                    : Double.MAX_VALUE;
            if (lower > upper) {
                throw new IllegalArgumentException("principal '" + principalName
                        + "' metric row filter on '" + filter.getMappingSubject() + "."
                        + filter.getMember() + "' has an empty double_range after exclusive bounds");
            }
        }
        if (!filter.getCel().isEmpty()) {
            String expression = filter.getCel().trim();
            if (expression.isEmpty()) {
                throw new IllegalArgumentException("principal '" + principalName
                        + "' metric row filter on '" + filter.getMappingSubject()
                        + "' has a blank cel expression");
            }
            CelExpr root;
            try {
                root = ROW_FILTER_PARSER.parse(expression, "row_filter").getAst().getExpr();
            } catch (CelValidationException e) {
                throw new IllegalArgumentException("principal '" + principalName
                        + "' metric row filter on '" + filter.getMappingSubject()
                        + "' has invalid cel '" + filter.getCel() + "': " + e.getMessage(), e);
            }
            if (!isBooleanPredicateShape(root) || !referencesField(root)) {
                throw new IllegalArgumentException("principal '" + principalName
                        + "' metric row filter on '" + filter.getMappingSubject()
                        + "' cel '" + filter.getCel()
                        + "' must be a boolean predicate over at least one member or field");
            }
        }
    }

    private static boolean isBooleanPredicateShape(CelExpr expr) {
        return switch (expr.getKind()) {
            case CALL -> {
                CelExpr.CelCall call = expr.call();
                String fn = call.function();
                if (!BOOLEAN_OPERATORS.contains(fn)) {
                    yield false;
                }
                if ("_&&_".equals(fn) || "_||_".equals(fn)) {
                    yield call.args().size() == 2
                            && isBooleanPredicateShape(call.args().get(0))
                            && isBooleanPredicateShape(call.args().get(1));
                }
                if ("!_".equals(fn)) {
                    yield call.args().size() == 1
                            && isBooleanPredicateShape(call.args().get(0));
                }
                yield true;
            }
            case SELECT, IDENT -> true;
            default -> false;
        };
    }

    private static boolean referencesField(CelExpr expr) {
        return switch (expr.getKind()) {
            case SELECT -> true;
            case IDENT -> {
                String name = expr.ident().name();
                yield !"this".equals(name) && !"true".equals(name)
                        && !"false".equals(name) && !"null".equals(name);
            }
            case CALL -> {
                CelExpr.CelCall call = expr.call();
                if (call.target().isPresent() && referencesField(call.target().get())) {
                    yield true;
                }
                for (CelExpr arg : call.args()) {
                    if (referencesField(arg)) {
                        yield true;
                    }
                }
                yield false;
            }
            case LIST -> {
                for (CelExpr elem : expr.list().elements()) {
                    if (referencesField(elem)) {
                        yield true;
                    }
                }
                yield false;
            }
            default -> false;
        };
    }

    /**
     * Loads and verifies a policy file. The extension decides the format — {@code .json}
     * parses as canonical proto3 JSON refusing unknown fields, {@code .binpb} or {@code .pb}
     * parses as the binary message — and anything else is refused naming the accepted forms.
     */
    public static AccessPolicy load(Path file) throws IOException {
        String name = file.getFileName().toString();
        if (name.endsWith(".json")) {
            AccessPolicy.Builder builder = AccessPolicy.newBuilder();
            try {
                JsonFormat.parser().merge(Files.readString(file), builder);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "access policy file '" + name + "' does not parse: " + e.getMessage(), e);
            }
            return requireWellFormed(builder.build());
        }
        if (name.endsWith(".binpb") || name.endsWith(".pb")) {
            try {
                return requireWellFormed(AccessPolicy.parseFrom(Files.readAllBytes(file)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "access policy file '" + name + "' does not parse: " + e.getMessage(), e);
            }
        }
        throw new IllegalArgumentException("access policy file '" + name
                + "' must end in .json, .binpb, or .pb");
    }
}

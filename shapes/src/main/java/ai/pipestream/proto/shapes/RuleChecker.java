package ai.pipestream.proto.shapes;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelMappingRule;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import dev.cel.bundle.Cel;
import dev.cel.common.CelValidationResult;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.CelType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Statically validates mapping rules and CEL expressions against descriptors — nothing is
 * executed, so a broken ruleset fails at configuration or registration time, not on the
 * millionth message. Two modes mirror the two rule dialects:
 *
 * <ul>
 *   <li><b>in-place</b> (the {@code map-message} dialect): unscoped paths over one message
 *       type, CEL seeing that message as a single variable;</li>
 *   <li><b>scoped</b> (the join dialect): source paths whose first segment names a scope
 *       entry, targets on a separate output type, CEL seeing every source plus
 *       {@code target}.</li>
 * </ul>
 *
 * <p>Checks: text-rule syntax; every path resolves on its descriptor (paths through
 * {@code Struct}, {@code Any}, or map fields become unverifiable and are accepted);
 * repeated-versus-singular shape (a repeated source needs a repeated target); message-typed
 * ends must agree on the message type; CEL mapping expressions compile and type-check
 * against the real descriptors; and <b>filters must type-check to bool</b> — the one thing
 * that separates a filter from a mapping selector.</p>
 */
public final class RuleChecker {

    /** One problem found: {@code kind} is {@code rule}, {@code celRule}, or {@code filter}. */
    public record Finding(String kind, int index, String rule, String error) {
    }

    /** In-place mode: rules over one type, CEL binding it as {@code varName}. */
    public List<Finding> checkInPlace(String varName, Descriptor type, List<String> rules,
                                      List<CelMappingRule> celRules, List<String> filters) {
        return check(Map.of(varName, type), type, true, rules, celRules, filters);
    }

    /** Scoped mode: named sources onto a target type. */
    public List<Finding> checkScoped(Map<String, Descriptor> sources, Descriptor target,
                                     List<String> rules, List<CelMappingRule> celRules,
                                     List<String> filters) {
        return check(sources, target, false, rules, celRules, filters);
    }

    private List<Finding> check(Map<String, Descriptor> sources, Descriptor target,
                                boolean inPlace, List<String> rules,
                                List<CelMappingRule> celRules, List<String> filters) {
        List<Finding> findings = new ArrayList<>();
        for (int i = 0; i < rules.size(); i++) {
            String error = checkTextRule(rules.get(i), sources, target, inPlace);
            if (error != null) {
                findings.add(new Finding("rule", i, rules.get(i), error));
            }
        }
        Cel cel = environment(sources, target, inPlace);
        for (int i = 0; i < celRules.size(); i++) {
            CelMappingRule rule = celRules.get(i);
            String label = rule.targetPath();
            String error = checkCelRule(rule, cel, sources, target, inPlace);
            if (error != null) {
                findings.add(new Finding("celRule", i, label, error));
            }
        }
        for (int i = 0; i < filters.size(); i++) {
            String error = mustBool(cel, filters.get(i));
            if (error != null) {
                findings.add(new Finding("filter", i, filters.get(i), error));
            }
        }
        return findings;
    }

    private static Cel environment(Map<String, Descriptor> sources, Descriptor target,
                                   boolean inPlace) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        sources.forEach(factory::addMessageVar);
        if (!inPlace) {
            factory.addMessageVar("target", target);
        }
        return factory.build();
    }

    private static String checkCelRule(CelMappingRule rule, Cel cel,
                                       Map<String, Descriptor> sources, Descriptor target,
                                       boolean inPlace) {
        if (rule.filterExpression() != null && !rule.filterExpression().isBlank()) {
            String error = mustBool(cel, rule.filterExpression());
            if (error != null) {
                return "filter: " + error;
            }
        }
        if (rule.selectorExpression() != null && !rule.selectorExpression().isBlank()) {
            String error = compileError(cel, rule.selectorExpression());
            if (error != null) {
                return "selector: " + error;
            }
        }
        Resolution targetPath = resolve(target, rule.targetPath());
        if (targetPath.error() != null) {
            return "target: " + targetPath.error();
        }
        for (String fallback : rule.textRuleFallback()) {
            String error = checkTextRule(fallback, sources, target, inPlace);
            if (error != null) {
                return "fallback '" + fallback + "': " + error;
            }
        }
        return null;
    }

    private static String checkTextRule(String rule, Map<String, Descriptor> sources,
                                        Descriptor target, boolean inPlace) {
        String trimmed = rule.trim();
        if (trimmed.isEmpty()) {
            return "empty rule";
        }
        if (trimmed.startsWith("-")) {
            Resolution cleared = resolve(target, trimmed.substring(1).trim());
            return cleared.error();
        }
        boolean append = trimmed.contains("+=");
        String[] sides = trimmed.split(append ? "\\+=" : "=", 2);
        if (sides.length != 2 || sides[0].isBlank() || sides[1].isBlank()) {
            return "not 'target = source.path', 'target += source.path', or '-target'";
        }
        Resolution to = resolve(target, sides[0].trim());
        if (to.error() != null) {
            return "target: " + to.error();
        }
        String source = sides[1].trim();
        if (Literals.isLiteral(source)) {
            return null;
        }
        Resolution from = inPlace
                ? resolve(sources.values().iterator().next(), source)
                : resolveScoped(sources, source);
        if (from.error() != null) {
            return "source: " + from.error();
        }
        return shapeError(from, to, append);
    }

    /** The shape rules the runtime enforces (or trips over), caught statically. */
    private static String shapeError(Resolution from, Resolution to, boolean append) {
        if (from.field() == null || to.field() == null) {
            return null; // a wildcard end (Struct/Any/map) is unverifiable
        }
        FieldDescriptor source = from.field();
        FieldDescriptor target = to.field();
        if (source.isRepeated() && !target.isRepeated()) {
            return "repeated source '" + source.getName() + "' cannot land on singular '"
                    + target.getName() + "'";
        }
        boolean sourceMessage = source.getJavaType() == FieldDescriptor.JavaType.MESSAGE;
        boolean targetMessage = target.getJavaType() == FieldDescriptor.JavaType.MESSAGE;
        if (sourceMessage != targetMessage) {
            return "cannot convert between message and scalar ('" + source.getName()
                    + "' -> '" + target.getName() + "')";
        }
        if (sourceMessage && !source.getMessageType().getFullName()
                .equals(target.getMessageType().getFullName())) {
            // Struct and Any targets absorb anything at runtime.
            String targetType = target.getMessageType().getFullName();
            if (!targetType.equals("google.protobuf.Struct")
                    && !targetType.equals("google.protobuf.Any")) {
                return "message type mismatch: " + source.getMessageType().getFullName()
                        + " -> " + targetType;
            }
        }
        if (append && !target.isRepeated()) {
            return "'" + target.getName() + "' is singular; += needs a repeated target";
        }
        return null;
    }

    private static Resolution resolveScoped(Map<String, Descriptor> sources, String path) {
        int dot = path.indexOf('.');
        String name = dot < 0 ? path : path.substring(0, dot);
        Descriptor source = sources.get(name);
        if (source == null) {
            return Resolution.error("unknown source '" + name + "'; the scope has "
                    + sources.keySet());
        }
        if (dot < 0) {
            return Resolution.unverifiable(); // the whole source message
        }
        return resolve(source, path.substring(dot + 1));
    }

    /** A resolved path end: a concrete field, a wildcard (unverifiable), or an error. */
    private record Resolution(FieldDescriptor field, boolean wildcard, String error) {
        static Resolution of(FieldDescriptor field) {
            return new Resolution(field, false, null);
        }

        static Resolution unverifiable() {
            return new Resolution(null, true, null);
        }

        static Resolution error(String error) {
            return new Resolution(null, false, error);
        }
    }

    private static Resolution resolve(Descriptor root, String dotted) {
        if (dotted.isEmpty()) {
            return Resolution.error("empty path");
        }
        Descriptor current = root;
        String[] segments = dotted.split("\\.");
        if (segments.length == 0) {
            // A path of nothing but separators, such as "." — split drops the empty segments.
            return Resolution.error("path '" + dotted + "' names no fields");
        }
        for (int i = 0; i < segments.length; i++) {
            String wellKnown = current.getFullName();
            if (wellKnown.equals("google.protobuf.Struct")
                    || wellKnown.equals("google.protobuf.Any")
                    || wellKnown.equals("google.protobuf.Value")) {
                return Resolution.unverifiable(); // dynamic from here down
            }
            FieldDescriptor field = current.findFieldByName(segments[i]);
            if (field == null) {
                return Resolution.error("no field '" + segments[i] + "' on "
                        + current.getFullName());
            }
            if (i == segments.length - 1) {
                return Resolution.of(field);
            }
            if (field.isMapField()) {
                return Resolution.unverifiable(); // keys are dynamic
            }
            if (field.isRepeated()) {
                return Resolution.error("cannot traverse through repeated field '"
                        + segments[i] + "'");
            }
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                return Resolution.error("cannot traverse through non-message field '"
                        + segments[i] + "'");
            }
            current = field.getMessageType();
        }
        throw new IllegalStateException("unreachable");
    }

    private static String compileError(Cel cel, String expression) {
        try {
            CelValidationResult result = cel.compile(expression);
            if (result.hasError()) {
                return result.getErrorString();
            }
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private static String mustBool(Cel cel, String expression) {
        try {
            CelValidationResult result = cel.compile(expression);
            if (result.hasError()) {
                return result.getErrorString();
            }
            CelType type = result.getAst().getResultType();
            if (type.kind() != CelKind.BOOL && type.kind() != CelKind.DYN) {
                return "a filter must be a boolean expression; this is " + type;
            }
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}

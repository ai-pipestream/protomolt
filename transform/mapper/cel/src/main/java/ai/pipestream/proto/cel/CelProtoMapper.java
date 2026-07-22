package ai.pipestream.proto.cel;

import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import com.google.protobuf.Message;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Applies CEL-aware mapping rules to a protobuf builder.
 *
 * <p>Each rule sees the progressive state of the builder (later filters/selectors
 * observe earlier writes). Candidate lists try rules in order until one applies.
 */
public final class CelProtoMapper {
    private final ProtoFieldMapper fieldMapper;
    private final CelEvaluator evaluator;
    private final String rootVariable;
    private final Map<String, Object> extraBindings;

    public CelProtoMapper(ProtoFieldMapper fieldMapper, CelEvaluator evaluator) {
        this(fieldMapper, evaluator, "input", Map.of());
    }

    public CelProtoMapper(ProtoFieldMapper fieldMapper, CelEvaluator evaluator, String rootVariable) {
        this(fieldMapper, evaluator, rootVariable, Map.of());
    }

    /**
     * Creates a mapper with bindings available to every CEL filter and selector.
     * The current target message always replaces a same-named root binding.
     */
    public CelProtoMapper(
            ProtoFieldMapper fieldMapper,
            CelEvaluator evaluator,
            String rootVariable,
            Map<String, Object> extraBindings) {
        this.fieldMapper = Objects.requireNonNull(fieldMapper, "fieldMapper");
        this.evaluator = Objects.requireNonNull(evaluator, "evaluator");
        this.rootVariable = Objects.requireNonNull(rootVariable, "rootVariable");
        this.extraBindings = Map.copyOf(extraBindings == null ? Map.of() : extraBindings);
    }

    public void map(Message.Builder target, List<CelMappingRule> rules) throws MappingException {
        if (rules == null) {
            return;
        }
        for (CelMappingRule rule : rules) {
            apply(target, rule, false);
        }
    }

    /**
     * Tries a single rule. Returns {@code false} when the filter rejects it or
     * the selector fails (candidate-fallback friendly). Throws only for path I/O
     * failures after a successful selector evaluation.
     */
    public boolean tryMap(Message.Builder target, CelMappingRule rule) throws MappingException {
        return apply(target, rule, true);
    }

    /**
     * Applies the first candidate that succeeds (filter match + selector/text rules).
     *
     * @return {@code true} if a candidate applied
     */
    public boolean mapFirstCandidate(Message.Builder target, List<CelMappingRule> candidates)
            throws MappingException {
        if (candidates == null) {
            return false;
        }
        for (CelMappingRule candidate : candidates) {
            if (tryMap(target, candidate)) {
                return true;
            }
        }
        return false;
    }

    private boolean apply(Message.Builder target, CelMappingRule rule, boolean softSelector)
            throws MappingException {
        Objects.requireNonNull(rule, "rule");
        Map<String, Object> bindings = new LinkedHashMap<>(extraBindings);
        // buildPartial: the progressive builder state may legitimately have unset proto2
        // required fields mid-mapping; build() would throw UninitializedMessageException.
        bindings.put(rootVariable, target.buildPartial());
        if (rule.filterExpression() != null && !rule.filterExpression().isBlank()) {
            // Soft mode skips the rule on filter compile/evaluation errors; strict mode propagates them.
            boolean filterMatches = softSelector
                    ? evaluator.evaluateBoolean(rule.filterExpression(), bindings)
                    : evaluator.evaluateBooleanOrFail(rule.filterExpression(), bindings);
            if (!filterMatches) {
                return false;
            }
        }
        if (rule.selectorExpression() != null && !rule.selectorExpression().isBlank()) {
            Object value;
            try {
                value = evaluator.evaluateValue(rule.selectorExpression(), bindings);
            } catch (CelEvaluationException e) {
                if (softSelector) {
                    return false;
                }
                throw e;
            }
            fieldMapper.setValue(target, rule.targetPath(), value);
            return true;
        }
        if (!rule.textRuleFallback().isEmpty()) {
            fieldMapper.mapInPlace(target, rule.textRuleFallback());
            return true;
        }
        return false;
    }
}

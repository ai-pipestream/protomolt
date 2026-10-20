package ai.protomolt.proto.workflow;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.mapper.cel.CelMappingRule;
import ai.protomolt.proto.mapper.cel.CelProtoMapper;
import ai.protomolt.proto.shapes.MessageScope;
import ai.protomolt.proto.shapes.ScopedProtoMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The scoped mapping a step request, edge value, or workflow output is built with: text
 * rules over the named scope, then CEL rules that also see {@code target}, the progressive
 * message. {@link WorkflowRunner} and {@link WorkflowReplay} both map through here, so a
 * replay re-derives a request with exactly the rules the live run applied.
 */
final class ScopedMapping {

    private ScopedMapping() {
    }

    /** The named scope the text rules read: every value, in insertion order. */
    static MessageScope scope(Map<String, Message> values) {
        MessageScope.Builder scope = MessageScope.builder();
        values.forEach(scope::add);
        return scope.build();
    }

    /**
     * Applies {@code rules}, then {@code celRules} when there are any, to {@code builder}.
     * {@code evaluator} is consulted only when CEL rules are present.
     */
    static void apply(ScopedProtoMapper mapper, MessageScope scope,
                      Map<String, Message> values, DynamicMessage.Builder builder,
                      List<String> rules, List<CelMappingRule> celRules,
                      Supplier<CelEvaluator> evaluator) throws Exception {
        mapper.map(scope, builder, rules);
        if (!celRules.isEmpty()) {
            new CelProtoMapper(mapper.fieldMapper(), evaluator.get(), "target",
                    Map.copyOf(values)).map(builder, celRules);
        }
    }

    /** A CEL environment over exactly the values in scope, plus {@code target}. */
    static CelEvaluator evaluator(Map<String, Message> values, Descriptor targetType) {
        CelEnvironmentFactory factory = CelEnvironmentFactory.builder();
        values.forEach((name, message) ->
                factory.addMessageVar(name, message.getDescriptorForType()));
        factory.addMessageVar("target", targetType);
        return new CelEvaluator(factory.build());
    }

    /** The mapper's rule form of a contract CEL rule; blank expressions are absent. */
    static CelMappingRule celRule(ai.protomolt.proto.grpc.workflow.v1.CelMappingRule rule) {
        return new CelMappingRule(
                rule.getFilter().isBlank() ? null : rule.getFilter(),
                rule.getSelector().isBlank() ? null : rule.getSelector(),
                rule.getTarget(), rule.getFallbackList());
    }
}

package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.mapper.cel.CelMappingRule;

import java.util.List;

/**
 * Converts the wire form of CEL mapping rules into the mapper's rule records, the one
 * conversion the checker type-checks and the executor applies. A blank filter or selector
 * on the wire means the rule declares none.
 */
final class CelMappingRules {

    private CelMappingRules() {
    }

    /**
     * Converts {@code rules} in order.
     *
     * @return the mapper rule records, one per wire rule
     */
    static List<CelMappingRule> toMapperRules(
            List<ai.protomolt.proto.grpc.workflow.v1.CelMappingRule> rules) {
        return rules.stream()
                .map(rule -> new CelMappingRule(
                        rule.getFilter().isBlank() ? null : rule.getFilter(),
                        rule.getSelector().isBlank() ? null : rule.getSelector(),
                        rule.getTarget(), rule.getFallbackList()))
                .toList();
    }
}

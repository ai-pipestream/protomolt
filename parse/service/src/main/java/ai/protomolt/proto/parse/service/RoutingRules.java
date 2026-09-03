package ai.protomolt.proto.parse.service;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.parse.v1.PlannedParse;
import ai.protomolt.proto.parse.v1.RoutingRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import dev.cel.common.types.SimpleType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * The loaded routing-rule set: service configuration, not mutable RPC state.
 *
 * <p>Rules are sorted once at construction (descending priority, ties
 * lexicographic on {@code rule_id}) and every CEL guard is compiled up front —
 * a rule that does not compile fails the boot, not the millionth document.
 * {@link #plan} evaluates the guards over a {@link RoutingContext} with
 * exactly the bindings the routing.proto header declares; EVERY matching rule
 * contributes one {@link PlannedParse} (routing is a set, not first-match).
 */
public final class RoutingRules {

    /**
     * The routing context one document presents to the rule guards — one
     * field per CEL binding the routing.proto header declares.
     *
     * @param mimeType the SNIFFED content type (the routing source of truth)
     * @param declaredMimeType the type the intake caller declared (a hint)
     * @param filename the original filename, when known
     * @param extension lowercase filename extension, {@code ""} when none
     * @param sizeBytes the blob size in bytes
     * @param accountId the owning account (tenant root)
     */
    public record RoutingContext(
            String mimeType,
            String declaredMimeType,
            String filename,
            String extension,
            long sizeBytes,
            String accountId) {
    }

    private final List<RoutingRule> rules;
    private final CelEvaluator evaluator;

    private RoutingRules(List<RoutingRule> rules) {
        List<RoutingRule> sorted = new ArrayList<>(rules);
        sorted.sort(Comparator.comparingInt(RoutingRule::getPriority).reversed()
                .thenComparing(RoutingRule::getRuleId));
        this.rules = List.copyOf(sorted);
        this.evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addVar("mime_type", SimpleType.STRING)
                .addVar("declared_mime_type", SimpleType.STRING)
                .addVar("filename", SimpleType.STRING)
                .addVar("extension", SimpleType.STRING)
                .addVar("size_bytes", SimpleType.INT)
                .addVar("account_id", SimpleType.STRING)
                .build());
        for (RoutingRule rule : this.rules) {
            if (rule.getWhen().isBlank()) {
                continue; // an empty guard matches nothing, by contract
            }
            try {
                evaluator.precompile(rule.getWhen());
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "routing rule '" + rule.getRuleId() + "' has an invalid CEL guard: "
                                + e.getMessage(), e);
            }
        }
    }

    /**
     * Builds the rule set from already-decoded rules.
     *
     * @param rules the rules, in any order
     * @return the compiled, sorted rule set
     * @throws IllegalArgumentException when a rule's CEL guard does not compile
     */
    public static RoutingRules of(List<RoutingRule> rules) {
        if (rules == null) {
            throw new IllegalArgumentException("rules must not be null");
        }
        return new RoutingRules(rules);
    }

    /**
     * Builds the rule set from a JSON array of proto3-JSON
     * {@link RoutingRule} objects — the service-config wire format.
     *
     * @param json the JSON array
     * @return the compiled, sorted rule set
     * @throws IllegalArgumentException when the JSON does not decode or a
     *         rule's CEL guard does not compile
     */
    public static RoutingRules fromJson(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("rules JSON must not be blank");
        }
        List<RoutingRule> rules = new ArrayList<>();
        try {
            ListValue.Builder array = ListValue.newBuilder();
            JsonFormat.parser().merge(json, array);
            for (Value element : array.getValuesList()) {
                RoutingRule.Builder rule = RoutingRule.newBuilder();
                JsonFormat.parser().merge(JsonFormat.printer().print(element), rule);
                rules.add(rule.build());
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("rules JSON does not decode as a RoutingRule array: "
                    + e.getMessage(), e);
        }
        return new RoutingRules(rules);
    }

    /**
     * Evaluates the rule set against one document's routing context.
     *
     * @param ctx the routing context
     * @return one {@link PlannedParse} per matching rule, ordered by
     *         descending rule priority (ties lexicographic on rule_id)
     */
    public List<PlannedParse> plan(RoutingContext ctx) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        Map<String, Object> bindings = Map.of(
                "mime_type", ctx.mimeType() == null ? "" : ctx.mimeType(),
                "declared_mime_type", ctx.declaredMimeType() == null ? "" : ctx.declaredMimeType(),
                "filename", ctx.filename() == null ? "" : ctx.filename(),
                "extension", ctx.extension() == null ? "" : ctx.extension(),
                "size_bytes", ctx.sizeBytes(),
                "account_id", ctx.accountId() == null ? "" : ctx.accountId());
        List<PlannedParse> planned = new ArrayList<>();
        for (RoutingRule rule : rules) {
            if (rule.getWhen().isBlank()) {
                continue;
            }
            if (evaluator.evaluateBooleanOrFail(rule.getWhen(), bindings)) {
                planned.add(PlannedParse.newBuilder()
                        .setParserName(rule.getParserName())
                        .setParserConfig(rule.getParserConfig())
                        .setMatchedRuleId(rule.getRuleId())
                        .build());
            }
        }
        return List.copyOf(planned);
    }

    /** The rules in evaluation order (descending priority, ties on rule_id). */
    public List<RoutingRule> rules() {
        return rules;
    }
}

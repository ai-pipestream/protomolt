package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.cel.CelMappingRule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the {@code ChainDefinition} JSON envelope (the proto3 JSON of the typed RPC
 * message) into the resolved model. Anything unresolvable — schema, types, methods —
 * surfaces as {@link ChainParseException} with the step context, so the verbs can answer
 * with findings instead of stack traces.
 */
final class ChainJson {

    /** A definition that cannot be resolved; {@code step} is empty for chain-level issues. */
    static final class ChainParseException extends Exception {
        final String step;

        ChainParseException(String step, String message) {
            super(message);
            this.step = step;
        }
    }

    private ChainJson() {
    }

    static ChainDefinition parse(ObjectNode chain, ActionContext context)
            throws ChainParseException {
        SchemaResolver.ResolvedSchema schema;
        Descriptor inputType;
        try {
            schema = SchemaResolver.resolve(chain, "schema", context);
            inputType = schema.message(text(chain, "inputType"), "/chain/inputType");
        } catch (ActionException e) {
            throw new ChainParseException("", e.getMessage());
        }
        JsonNode stepsNode = chain.get("steps");
        if (stepsNode == null || !stepsNode.isArray() || stepsNode.isEmpty()) {
            throw new ChainParseException("", "'steps' must be a non-empty array");
        }
        List<ChainDefinition.Step> steps = new ArrayList<>(stepsNode.size());
        for (JsonNode node : stepsNode) {
            if (!(node instanceof ObjectNode step)) {
                throw new ChainParseException("", "each step must be an object");
            }
            String name = text(step, "name");
            if (name == null) {
                throw new ChainParseException("", "each step needs a 'name'");
            }
            String target = text(step, "target");
            String method = text(step, "method");
            if (target == null || method == null) {
                throw new ChainParseException(name, "a step needs 'target' and 'method'");
            }
            MethodDescriptor resolved;
            try {
                resolved = ChainDefinition.resolveMethod(schema.files(), method);
            } catch (IllegalArgumentException e) {
                throw new ChainParseException(name, e.getMessage());
            }
            steps.add(new ChainDefinition.Step(name, target,
                    step.path("tls").asBoolean(false), resolved, text(step, "when"),
                    strings(step.get("rules")), celRules(step.get("celRules"), name),
                    step.path("validate").asBoolean(false),
                    step.path("deadlineMs").asLong(0)));
        }
        ChainDefinition.Output output = null;
        JsonNode outputNode = chain.get("output");
        if (outputNode instanceof ObjectNode outputObject) {
            String type = text(outputObject, "type");
            if (type == null) {
                throw new ChainParseException("", "'output' needs a 'type'");
            }
            Descriptor outputType;
            try {
                outputType = schema.message(type, "/chain/output/type");
            } catch (ActionException e) {
                throw new ChainParseException("", e.getMessage());
            }
            output = new ChainDefinition.Output(outputType,
                    strings(outputObject.get("rules")),
                    celRules(outputObject.get("celRules"), "output"));
        }
        try {
            return new ChainDefinition(text(chain, "name"), schema.files(), inputType,
                    chain.path("deadlineMs").asLong(0), steps, output);
        } catch (IllegalArgumentException e) {
            throw new ChainParseException("", e.getMessage());
        }
    }

    private static String text(ObjectNode node, String field) {
        JsonNode value = node.get(field);
        return value != null && value.isTextual() && !value.asText().isBlank()
                ? value.asText()
                : null;
    }

    private static List<String> strings(JsonNode array) {
        if (array == null || !array.isArray()) {
            return List.of();
        }
        List<String> values = new ArrayList<>(array.size());
        array.forEach(node -> values.add(node.asText()));
        return values;
    }

    private static List<CelMappingRule> celRules(JsonNode array, String step)
            throws ChainParseException {
        if (array == null || !array.isArray()) {
            return List.of();
        }
        List<CelMappingRule> rules = new ArrayList<>(array.size());
        for (JsonNode node : array) {
            if (!(node instanceof ObjectNode rule)) {
                throw new ChainParseException(step, "each CEL rule must be an object");
            }
            String target = text(rule, "target");
            if (target == null) {
                throw new ChainParseException(step, "a CEL rule needs a 'target' path");
            }
            rules.add(new CelMappingRule(text(rule, "filter"), text(rule, "selector"),
                    target, strings(rule.get("fallback"))));
        }
        return rules;
    }
}

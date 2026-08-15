package ai.pipestream.proto.workflow;

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
 * Parses the {@code CompiledWorkflow} JSON envelope (the proto3 JSON of the typed RPC
 * message) into the resolved model. Anything unresolvable — schema, types, methods —
 * surfaces as {@link WorkflowParseException} with the step context, so the verbs can answer
 * with findings instead of stack traces.
 *
 * <p>Public so the jobs worker parses stored workflow definitions with exactly the verbs'
 * semantics.</p>
 */
public final class WorkflowJson {

    /** A definition that cannot be resolved; {@code step} is empty for workflow-level issues. */
    public static final class WorkflowParseException extends Exception {
        /** The step the parse failed on; empty for workflow-level issues. */
        public final String step;

        WorkflowParseException(String step, String message) {
            super(message);
            this.step = step;
        }
    }

    private WorkflowJson() {
    }

    /** Parses one workflow-definition envelope into the resolved model. */
    public static CompiledWorkflow parse(ObjectNode workflow, ActionContext context)
            throws WorkflowParseException {
        SchemaResolver.ResolvedSchema schema;
        Descriptor inputType;
        try {
            schema = SchemaResolver.resolve(workflow, "schema", context);
            inputType = schema.message(text(workflow, "inputType"), "/workflow/inputType");
        } catch (ActionException e) {
            throw new WorkflowParseException("", e.getMessage());
        }
        JsonNode stepsNode = workflow.get("steps");
        if (stepsNode == null || !stepsNode.isArray() || stepsNode.isEmpty()) {
            throw new WorkflowParseException("", "'steps' must be a non-empty array");
        }
        List<CompiledWorkflow.Step> steps = new ArrayList<>(stepsNode.size());
        for (JsonNode node : stepsNode) {
            if (!(node instanceof ObjectNode step)) {
                throw new WorkflowParseException("", "each step must be an object");
            }
            String name = text(step, "name");
            if (name == null) {
                throw new WorkflowParseException("", "each step needs a 'name'");
            }
            JsonNode structuredNode = step.get("structured");
            if (structuredNode != null) {
                if (!(structuredNode instanceof ObjectNode structured)) {
                    throw new WorkflowParseException(name,
                            "'structured' must be an object");
                }
                if (step.has("target") || step.has("method") || step.has("tls")) {
                    throw new WorkflowParseException(name,
                            "a structured step must not declare target, method, or tls");
                }
                String targetTypeName = text(structured, "targetType");
                String model = text(structured, "model");
                if (targetTypeName == null || model == null) {
                    throw new WorkflowParseException(name,
                            "a structured step needs 'targetType' and 'model'");
                }
                JsonNode attemptsNode = structured.get("maxAttempts");
                if (attemptsNode != null && (!attemptsNode.isIntegralNumber()
                        || !attemptsNode.canConvertToInt())) {
                    throw new WorkflowParseException(name,
                            "structured.maxAttempts must be a 32-bit integer");
                }
                Descriptor targetType;
                try {
                    targetType = schema.message(targetTypeName,
                            "/workflow/steps/" + name + "/structured/targetType");
                } catch (ActionException e) {
                    throw new WorkflowParseException(name, e.getMessage());
                }
                steps.add(step(name, () -> new CompiledWorkflow.Step(name,
                        CompiledWorkflow.Step.STRUCTURED_DEPENDENCY, false, null,
                        text(step, "when"), strings(step.get("rules")),
                        celRules(step.get("celRules"), name),
                        step.path("validate").asBoolean(false),
                        step.path("deadlineMs").asLong(0), text(step, "completion"),
                        new CompiledWorkflow.StructuredSpec(targetType, model,
                                attemptsNode == null ? 0 : attemptsNode.intValue()),
                        edge(step.get("edge"), name, schema),
                        fanOut(step.get("fanOut"), name, schema))));
                continue;
            }
            String target = text(step, "target");
            String method = text(step, "method");
            if (target == null || method == null) {
                throw new WorkflowParseException(name, "a step needs 'target' and 'method'");
            }
            MethodDescriptor resolved;
            try {
                resolved = CompiledWorkflow.resolveMethod(schema.files(), method);
            } catch (IllegalArgumentException e) {
                throw new WorkflowParseException(name, e.getMessage());
            }
            steps.add(step(name, () -> new CompiledWorkflow.Step(name, target,
                    step.path("tls").asBoolean(false), resolved, text(step, "when"),
                    strings(step.get("rules")), celRules(step.get("celRules"), name),
                    step.path("validate").asBoolean(false),
                    step.path("deadlineMs").asLong(0), text(step, "completion"), null,
                    edge(step.get("edge"), name, schema),
                    fanOut(step.get("fanOut"), name, schema))));
        }
        CompiledWorkflow.Output output = null;
        JsonNode outputNode = workflow.get("output");
        if (outputNode instanceof ObjectNode outputObject) {
            String type = text(outputObject, "type");
            if (type == null) {
                throw new WorkflowParseException("", "'output' needs a 'type'");
            }
            Descriptor outputType;
            try {
                outputType = schema.message(type, "/workflow/output/type");
            } catch (ActionException e) {
                throw new WorkflowParseException("", e.getMessage());
            }
            output = new CompiledWorkflow.Output(outputType,
                    strings(outputObject.get("rules")),
                    celRules(outputObject.get("celRules"), "output"));
        }
        try {
            return new CompiledWorkflow(text(workflow, "name"), schema.files(), inputType,
                    workflow.path("deadlineMs").asLong(0), steps, output);
        } catch (IllegalArgumentException e) {
            throw new WorkflowParseException("", e.getMessage());
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
            throws WorkflowParseException {
        if (array == null || !array.isArray()) {
            return List.of();
        }
        List<CelMappingRule> rules = new ArrayList<>(array.size());
        for (JsonNode node : array) {
            if (!(node instanceof ObjectNode rule)) {
                throw new WorkflowParseException(step, "each CEL rule must be an object");
            }
            String target = text(rule, "target");
            if (target == null) {
                throw new WorkflowParseException(step, "a CEL rule needs a 'target' path");
            }
            rules.add(new CelMappingRule(text(rule, "filter"), text(rule, "selector"),
                    target, strings(rule.get("fallback"))));
        }
        return rules;
    }

    /** Builds one step, attributing any shape rejection to the step being parsed. */
    private static CompiledWorkflow.Step step(String name, StepBuilder builder)
            throws WorkflowParseException {
        try {
            return builder.build();
        } catch (IllegalArgumentException e) {
            throw new WorkflowParseException(name, e.getMessage());
        }
    }

    @FunctionalInterface
    private interface StepBuilder {
        CompiledWorkflow.Step build() throws WorkflowParseException;
    }

    /** Parses a step's typed edge, resolving its types against the workflow schema. */    private static CompiledWorkflow.EdgeSpec edge(JsonNode node, String step,
                                                 SchemaResolver.ResolvedSchema schema)
            throws WorkflowParseException {
        if (node == null) {
            return null;
        }
        if (!(node instanceof ObjectNode edge)) {
            throw new WorkflowParseException(step, "'edge' must be an object");
        }
        String produceTypeName = text(edge, "produceType");
        List<String> sources = strings(edge.get("sources"));
        if (produceTypeName == null || sources.isEmpty()) {
            throw new WorkflowParseException(step,
                    "an edge needs 'sources' and 'produceType'");
        }
        try {
            Descriptor produceType = schema.message(produceTypeName,
                    "/workflow/steps/" + step + "/edge/produceType");
            String projectToName = text(edge, "projectTo");
            Descriptor projectTo = projectToName == null ? null : schema.message(
                    projectToName, "/workflow/steps/" + step + "/edge/projectTo");
            return new CompiledWorkflow.EdgeSpec(sources, produceType,
                    strings(edge.get("rules")), celRules(edge.get("celRules"), step),
                    projectTo, edge.path("validate").asBoolean(false));
        } catch (ActionException | IllegalArgumentException e) {
            throw new WorkflowParseException(step, e.getMessage());
        }
    }

    /** Parses a step's bounded fan-out, resolving the collect type against the schema. */
    private static CompiledWorkflow.FanOutSpec fanOut(JsonNode node, String step,
                                                     SchemaResolver.ResolvedSchema schema)
            throws WorkflowParseException {
        if (node == null) {
            return null;
        }
        if (!(node instanceof ObjectNode fanOut)) {
            throw new WorkflowParseException(step, "'fanOut' must be an object");
        }
        String items = text(fanOut, "items");
        String collectTypeName = text(fanOut, "collectType");
        String collectInto = text(fanOut, "collectInto");
        String policy = text(fanOut, "failurePolicy");
        if (items == null || collectTypeName == null || collectInto == null
                || policy == null) {
            throw new WorkflowParseException(step, "a fanOut needs 'items', 'collectType', "
                    + "'collectInto', and 'failurePolicy'");
        }
        CompiledWorkflow.BranchFailurePolicy failurePolicy;
        try {
            failurePolicy = CompiledWorkflow.BranchFailurePolicy.valueOf(policy);
        } catch (IllegalArgumentException e) {
            throw new WorkflowParseException(step, "fanOut.failurePolicy must be FAIL_FAST "
                    + "or CONTINUE; got '" + policy + "'");
        }
        JsonNode maxItems = fanOut.get("maxItems");
        JsonNode maxConcurrency = fanOut.get("maxConcurrency");
        if (maxItems != null && (!maxItems.isIntegralNumber()
                || !maxItems.canConvertToInt())) {
            throw new WorkflowParseException(step, "fanOut.maxItems must be a 32-bit integer");
        }
        if (maxConcurrency != null && (!maxConcurrency.isIntegralNumber()
                || !maxConcurrency.canConvertToInt())) {
            throw new WorkflowParseException(step,
                    "fanOut.maxConcurrency must be a 32-bit integer");
        }
        try {
            return new CompiledWorkflow.FanOutSpec(items,
                    maxItems == null ? 0 : maxItems.intValue(),
                    maxConcurrency == null ? 0 : maxConcurrency.intValue(),
                    failurePolicy,
                    schema.message(collectTypeName,
                            "/workflow/steps/" + step + "/fanOut/collectType"),
                    collectInto);
        } catch (ActionException | IllegalArgumentException e) {
            throw new WorkflowParseException(step, e.getMessage());
        }
    }
}

package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.cel.CelMappingRule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
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
    /**
     * A stored definition, which reaches the runtime as a document rather than as a field of
     * a request: out of a job row, or out of the workflow registry. It is read against the
     * same message an inline definition arrives as, so both are held to one description.
     */
    public static CompiledWorkflow parse(ObjectNode workflow, ActionContext context)
            throws WorkflowParseException {
        try {
            return parse(CatalogContract.read(workflow,
                    CatalogContract.request("CompiledWorkflow"), "workflow"), context);
        } catch (ActionException e) {
            throw new WorkflowParseException("", e.getMessage());
        }
    }

    public static CompiledWorkflow parse(Message workflow, ActionContext context)
            throws WorkflowParseException {
        SchemaResolver.ResolvedSchema schema;
        Descriptor inputType;
        try {
            schema = SchemaResolver.resolve(workflow, "schema", context);
            inputType = schema.message(text(workflow, "inputType"), "/workflow/inputType");
        } catch (ActionException e) {
            throw new WorkflowParseException("", e.getMessage());
        }
        List<Message> stepMessages = Fields.list(workflow, "steps");
        if (stepMessages.isEmpty()) {
            throw new WorkflowParseException("", "'steps' must be a non-empty array");
        }
        List<CompiledWorkflow.Step> steps = new ArrayList<>(stepMessages.size());
        for (Message step : stepMessages) {
            String name = text(step, "name");
            if (name == null) {
                throw new WorkflowParseException("", "each step needs a 'name'");
            }
            if (Fields.has(step, "structured")) {
                Message structured = Fields.message(step, "structured");
                // proto3 cannot tell an unset bool from a false one, so declaring tls is
                // declaring it true.
                if (text(step, "target") != null || text(step, "method") != null
                        || Fields.flag(step, "tls")) {
                    throw new WorkflowParseException(name,
                            "a structured step must not declare target, method, or tls");
                }
                String targetTypeName = text(structured, "targetType");
                String model = text(structured, "model");
                if (targetTypeName == null || model == null) {
                    throw new WorkflowParseException(name,
                            "a structured step needs 'targetType' and 'model'");
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
                        text(step, "when"), Fields.strings(step, "rules"),
                        celRules(step, name),
                        Fields.flag(step, "validate"),
                        Fields.integer(step, "deadlineMs"), text(step, "completion"),
                        new CompiledWorkflow.StructuredSpec(targetType, model,
                                Fields.integer(structured, "maxAttempts")),
                        edge(step, name, schema),
                        fanOut(step, name, schema))));
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
                    Fields.flag(step, "tls"), resolved, text(step, "when"),
                    Fields.strings(step, "rules"), celRules(step, name),
                    Fields.flag(step, "validate"),
                    Fields.integer(step, "deadlineMs"), text(step, "completion"), null,
                    edge(step, name, schema),
                    fanOut(step, name, schema))));
        }
        CompiledWorkflow.Output output = null;
        if (Fields.has(workflow, "output")) {
            Message outputMessage = Fields.message(workflow, "output");
            String type = text(outputMessage, "type");
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
                    Fields.strings(outputMessage, "rules"),
                    celRules(outputMessage, "output"));
        }
        try {
            return new CompiledWorkflow(text(workflow, "name"), schema.files(), inputType,
                    Fields.integer(workflow, "deadlineMs"), steps, output);
        } catch (IllegalArgumentException e) {
            throw new WorkflowParseException("", e.getMessage());
        }
    }

    /** A string field, or null when it is blank: absent and empty mean the same here. */
    private static String text(Message message, String field) {
        String value = Fields.string(message, field);
        return value.isBlank() ? null : value;
    }

    private static List<CelMappingRule> celRules(Message owner, String step)
            throws WorkflowParseException {
        List<Message> declared = Fields.list(owner, "celRules");
        List<CelMappingRule> rules = new ArrayList<>(declared.size());
        for (Message rule : declared) {
            String target = text(rule, "target");
            if (target == null) {
                throw new WorkflowParseException(step, "a CEL rule needs a 'target' path");
            }
            rules.add(new CelMappingRule(text(rule, "filter"), text(rule, "selector"),
                    target, Fields.strings(rule, "fallback")));
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

    /** Parses a step's typed edge, resolving its types against the workflow schema. */    private static CompiledWorkflow.EdgeSpec edge(Message owner, String step,
                                                 SchemaResolver.ResolvedSchema schema)
            throws WorkflowParseException {
        if (!Fields.has(owner, "edge")) {
            return null;
        }
        Message edge = Fields.message(owner, "edge");
        String produceTypeName = text(edge, "produceType");
        List<String> sources = Fields.strings(edge, "sources");
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
                    Fields.strings(edge, "rules"), celRules(edge, step),
                    projectTo, Fields.flag(edge, "validate"));
        } catch (ActionException | IllegalArgumentException e) {
            throw new WorkflowParseException(step, e.getMessage());
        }
    }

    /** Parses a step's bounded fan-out, resolving the collect type against the schema. */
    private static CompiledWorkflow.FanOutSpec fanOut(Message owner, String step,
                                                     SchemaResolver.ResolvedSchema schema)
            throws WorkflowParseException {
        if (!Fields.has(owner, "fanOut")) {
            return null;
        }
        Message fanOut = Fields.message(owner, "fanOut");
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
        try {
            return new CompiledWorkflow.FanOutSpec(items,
                    Fields.integer(fanOut, "maxItems"),
                    Fields.integer(fanOut, "maxConcurrency"),
                    failurePolicy,
                    schema.message(collectTypeName,
                            "/workflow/steps/" + step + "/fanOut/collectType"),
                    collectInto);
        } catch (ActionException | IllegalArgumentException e) {
            throw new WorkflowParseException(step, e.getMessage());
        }
    }
}

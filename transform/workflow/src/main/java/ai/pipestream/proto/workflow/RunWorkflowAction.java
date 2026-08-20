package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;

import java.util.List;

/**
 * The {@code run-workflow} verb: execute an inline workflow — serial typed gRPC calls, each
 * request mapped from the workflow input and every prior step's response. The workflow is
 * statically verified first; execution failures return {@code ok=false} with the failing
 * step, never a stack trace.
 */
public final class RunWorkflowAction implements ProtoAction {

    private final WorkflowRunner runner;
    private final WorkflowRepository repository;

    public RunWorkflowAction() {
        this(new WorkflowRunner(), null);
    }

    /** Injectable runner — the channel-factory seam for tests and TLS policy. */
    public RunWorkflowAction(WorkflowRunner runner) {
        this(runner, null);
    }

    /** With a repository, {@code workflowName} resolves stored workflows. */
    public RunWorkflowAction(WorkflowRunner runner, WorkflowRepository repository) {
        this.runner = runner;
        this.repository = repository;
    }

    @Override
    public String name() {
        return "run-workflow";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Executes a workflow: serial unary gRPC calls where each step's request is "
                + "mapped (rules + CEL) from the workflow 'input' and every prior step's "
                + "response, gates ('when') skip steps, 'validate' checks responses against "
                + "their declared rules, and deadlines nest. The workflow is verified before "
                + "anything runs. Returns the composed output (the last response, or the "
                + "'output' mapping's message).";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = baseSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("workflow", workflowSchema());
        properties.putObject("workflowName")
                .put("type", "string")
                .put("description", "A stored workflow to run instead of an inline 'workflow' — "
                        + "registered via the registry's workflows endpoint.");
        properties.putObject("input")
                .put("type", "object")
                .put("description", "The workflow input, as proto3 JSON of the workflow's "
                        + "inputType.");
        schema.putArray("required").add("input");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        JsonNode workflowNode = input.get("workflow");
        JsonNode nameNode = input.get("workflowName");
        if (workflowNode == null && nameNode != null && nameNode.isTextual()) {
            if (repository == null) {
                result.put("ok", false);
                result.put("error", "No workflow repository is mounted; run with an inline "
                        + "'workflow' or start a server with a registry");
                return result;
            }
            workflowNode = repository.workflow(nameNode.asText()).orElse(null);
            if (workflowNode == null) {
                result.put("ok", false);
                result.put("error", "No stored workflow named '" + nameNode.asText() + "'");
                return result;
            }
        }
        JsonNode inputNode = input.get("input");
        if (!(workflowNode instanceof ObjectNode workflow) || !(inputNode instanceof ObjectNode)) {
            result.put("ok", false);
            result.put("error", "'workflow' (or 'workflowName') and 'input' objects are required");
            return result;
        }
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflow, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            result.put("ok", false);
            result.put("failedStep", e.step);
            result.put("error", e.getMessage());
            return result;
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        if (!findings.isEmpty()) {
            WorkflowVerifier.Finding first = findings.get(0);
            result.put("ok", false);
            result.put("failedStep", first.step());
            result.put("error", "workflow does not verify (" + findings.size() + " finding"
                    + (findings.size() == 1 ? "" : "s") + "); first: [" + first.kind() + "] "
                    + first.error());
            return result;
        }
        DynamicMessage message;
        try {
            message = context.transcoder()
                    .fromJsonDynamic(inputNode.toString(), definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            result.put("ok", false);
            result.put("error", "'input' is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
            return result;
        }
        WorkflowRunner.Result outcome;
        try {
            outcome = runner.run(definition, message);
        } catch (WorkflowRunner.WorkflowExecutionException e) {
            result.put("ok", false);
            result.put("failedStep", e.step());
            result.put("error", e.getMessage());
            return result;
        }
        result.put("ok", true);
        result.put("outputType", outcome.output().getDescriptorForType().getFullName());
        try {
            result.set("output", context.objectMapper()
                    .readTree(context.transcoder().toJson(outcome.output())));
        } catch (JsonProcessingException e) {
            result.put("ok", false);
            result.put("error", "failed to render the workflow output: " + e.getMessage());
            return result;
        }
        ArrayNode steps = result.putArray("steps");
        for (WorkflowRunner.StepOutcome step : outcome.steps()) {
            ObjectNode node = steps.addObject();
            node.put("name", step.name());
            node.put("skipped", step.skipped());
        }
        return result;
    }

    static ObjectNode baseSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }

    /** The workflow-definition schema shared by run-workflow and check-workflow. */
    static ObjectNode workflowSchema() {
        ObjectNode workflow = JsonNodeFactory.instance.objectNode();
        workflow.put("type", "object");
        workflow.put("description", "The workflow definition: a schema declaring every step's "
                + "service, an inputType, and serial steps whose requests are mapped from "
                + "'input' plus prior steps' responses (by step name). gRPC steps: {name, "
                + "target, method, tls?, when? (bool CEL gate), rules?, celRules?, "
                + "validate?, deadlineMs?}. Structured steps: {name, structured: "
                + "{targetType, model, maxAttempts?}}. A step may carry a typed edge "
                + "instead of top-level rules: {edge: {sources, produceType, rules?, "
                + "celRules?, projectTo?, validate?}} maps the named scope entries into "
                + "produceType, optionally projects it to a projection-annotated "
                + "consumer form and validates it before the step runs; on a structured "
                + "step the value becomes the generation's grounding. An edge step may "
                + "fan out: {fanOut: {items, maxItems, maxConcurrency, failurePolicy "
                + "(FAIL_FAST|CONTINUE), collectType, collectInto}} runs one branch per "
                + "item of the produced message's repeated field and collects the branch "
                + "outputs, in index order, into collectType.collectInto. Optional "
                + "output: {type, rules, celRules}; "
                + "without it the last response returns.");
        return workflow;
    }
}

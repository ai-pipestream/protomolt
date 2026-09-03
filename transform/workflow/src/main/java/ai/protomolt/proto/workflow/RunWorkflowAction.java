package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
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
    public Descriptor requestType() {
        return CatalogContract.request("RunWorkflowRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("RunWorkflowResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context)
            throws ActionException {
        // The message states that a run names either an inline workflow or a stored one, so
        // the choice between them is all that is left to make here.
        ObjectNode stored = null;
        if (!Fields.has(input, "workflow")) {
            String named = Fields.string(input, "workflowName");
            if (repository == null) {
                return refusal("No workflow repository is mounted; run with an inline "
                        + "'workflow' or start a server with a registry");
            }
            stored = repository.workflow(named).orElse(null);
            if (stored == null) {
                return refusal("No stored workflow named '" + named + "'");
            }
        }
        CompiledWorkflow definition;
        try {
            definition = stored == null
                    ? WorkflowJson.parse(Fields.message(input, "workflow"), context)
                    : WorkflowJson.parse(stored, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            return refusal(e.step, e.getMessage());
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        if (!findings.isEmpty()) {
            WorkflowVerifier.Finding first = findings.getFirst();
            return refusal(first.step(), "workflow does not verify (" + findings.size()
                    + " finding" + (findings.size() == 1 ? "" : "s") + "); first: ["
                    + first.kind() + "] " + first.error());
        }
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(
                    Fields.json(input, "input").toString(), definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            return refusal("'input' is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
        }
        WorkflowRunner.Result outcome;
        try {
            outcome = runner.run(definition, message);
        } catch (WorkflowRunner.WorkflowExecutionException e) {
            return refusal(e.step(), e.getMessage());
        }
        Reply result = Reply.of(responseType())
                .set("ok", true)
                .set("outputType", outcome.output().getDescriptorForType().getFullName());
        try {
            result.set("output", context.transcoder().toJson(outcome.output()));
        } catch (RuntimeException e) {
            return refusal("failed to render the workflow output: " + e.getMessage());
        }
        for (WorkflowRunner.StepOutcome step : outcome.steps()) {
            result.append("steps")
                    .set("name", step.name())
                    .set("skipped", step.skipped())
                    .build();
        }
        return result.build();
    }

    /** A run that did not happen, reported as a result rather than as an error. */
    private Message refusal(String error) {
        return refusal("", error);
    }

    private Message refusal(String failedStep, String error) {
        return Reply.of(responseType())
                .set("ok", false)
                .set("failedStep", failedStep)
                .set("error", error)
                .build();
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
